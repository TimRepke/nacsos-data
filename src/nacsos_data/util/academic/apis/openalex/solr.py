import uuid
import logging
from pathlib import Path
from time import time
from datetime import timedelta
from typing import Any, Generator, Annotated

import typer
from httpx import Response
from pydantic import BaseModel

from nacsos_data.models.items import AcademicItemModel
from nacsos_data.models.openalex import WorksSchema, DefType, SearchField, OpType
from nacsos_data.util import get
from nacsos_data.util.conf import OpenAlexConfig, load_settings
from .shared import translate_work_to_item, FIELDS_SOLR, NESTED_FIELDS
from ..util import RequestClient, AbstractAPI


class SearchResult(BaseModel):
    query_time: int
    num_found: int
    docs: list[AcademicItemModel]
    histogram: dict[str, int] | None = None


class OpenAlexSolrAPI(AbstractAPI):
    PAGE_MAX = 5000  # ignored, use `batch_size` instead

    def __init__(
        self,
        openalex_conf: OpenAlexConfig,
        batch_size: int = 5000,
        export_fields: list[str] | None = None,
        include_histogram: bool = False,
        histogram_from: int = 1990,
        histogram_to: int = 2026,
        def_type: DefType = 'lucene',
        field: SearchField = 'title_abstract',
        op: OpType = 'AND',
        proxy: str | None = None,
        max_req_per_sec: int = 5,
        max_retries: int = 5,
        backoff_rate: float = 5.0,
        timeout: int = 60,
        logger: logging.Logger | None = None,
    ):
        super().__init__(api_key='', proxy=proxy, max_retries=max_retries, max_req_per_sec=max_req_per_sec, backoff_rate=backoff_rate, logger=logger)
        self.openalex_conf = openalex_conf

        self.def_type = def_type
        self.field = field
        self.op = op

        self.batch_size = batch_size

        self.histogram: dict[str, int] | None = None
        self.include_histogram = include_histogram
        self.histogram_from = histogram_from
        self.histogram_to = histogram_to
        self.num_found: int | None = None
        self.query_time: int | None = None
        self.timeout: int = timeout

        if export_fields is not None:
            self.export_fields = export_fields
        else:
            self.export_fields = list(FIELDS_SOLR)

        self.export_fields = [f'{field}:[json]' if field in NESTED_FIELDS else field for field in self.export_fields]

    def fetch_raw(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        """
        OpenAlex via solr wrapper for downloading all records for a given query.

        Documentation:
        https://solr.apache.org/guide/solr/latest/query-guide/standard-query-parser.html
        https://solr.apache.org/guide/solr/latest/query-guide/common-query-parameters.html
        https://solr.apache.org/guide/solr/latest/query-guide/other-parsers.html#surround-query-parser

        :return:
        """
        self.logger.info(
            f'Using proxy: {self.proxy} | auth info is provided: {self.openalex_conf.auth is None} | ssl verification: {self.openalex_conf.SSL_VERIFY}'
        )
        with RequestClient(
            backoff_rate=self.backoff_rate,
            max_req_per_sec=self.max_req_per_sec,
            max_retries=self.max_retries,
            proxy=self.proxy,
            auth=self.openalex_conf.auth,
            verify=self.openalex_conf.SSL_VERIFY,
        ) as request_client:
            params_ = {'q': query, 'q.op': self.op, 'sort': 'id desc', 'fl': ','.join(self.export_fields), 'rows': self.batch_size, 'cursorMark': '*'}

            if self.def_type == 'lucene':
                params_ |= {'df': self.field, 'defType': 'lucene'}
            else:
                params_ |= {'qf': self.field, 'defType': self.def_type}

            if self.include_histogram:
                params_ |= {
                    'facet': 'true',
                    'facet.range': 'publication_year',
                    'facet.sort': 'index',
                    'facet.range.gap': '1',
                    'facet.range.start': self.histogram_from,
                    'facet.range.end': self.histogram_to,
                }

            # overrides
            if params:
                params_ |= params

            t0 = time()
            self.logger.info(f'Querying endpoint with batch_size={self.batch_size:,}: {self.openalex_conf.solr_url}')
            self.logger.info(f'Request parameters: {params_}')

            if params_['cursorMark'] is None:
                del params_['cursorMark']

            batch_i = 0
            num_docs_cum = 0
            while True:
                t1 = time()
                batch_i += 1
                self.logger.info(f'Running query for batch {batch_i} with cursor "{params_.get("cursorMark")}"')
                t2 = time()
                res = request_client.post(f'{self.openalex_conf.solr_url}/select', data=params_, timeout=self.timeout).json()

                next_curser = res.get('nextCursorMark')
                params_['cursorMark'] = next_curser

                batch_docs = res['response']['docs']
                n_docs_batch = len(batch_docs)
                num_docs_cum += n_docs_batch
                self.num_found = res['response']['numFound']
                self.n_results = self.num_found  # not pretty, but for compatibility copy this variable
                self.query_time = res['responseHeader']['QTime']

                self.logger.debug(f'Query took {timedelta(seconds=time() - t2)}h and yielded {n_docs_batch:,} docs')
                if self.num_found > 0:
                    self.logger.debug(f'Current progress: {num_docs_cum:,}/{self.num_found:,}={num_docs_cum / self.num_found:.2%} docs')

                if len(batch_docs) == 0:
                    self.logger.info('No documents in this batch, assuming to be done!')
                    break

                if self.include_histogram and batch_i < 2:
                    self.histogram = self._prepare_histogram(res)

                self.logger.debug('Yielding documents...')
                yield from batch_docs

                self.logger.debug(f'Done with batch {batch_i} in {timedelta(seconds=time() - t1)}h; {timedelta(seconds=time() - t0)}h passed overall')

                if next_curser is None:
                    self.logger.info('Did not receive a `nextCursorMark`, assuming to be done!')
                    break

            self.logger.info(f'Reached end of result set after {timedelta(seconds=time() - t1)}h')

    @classmethod
    def _prepare_histogram(cls, response: Response) -> dict[str, int] | None:
        hist_facets: list[int | str] | None = get(response, 'facet_counts', 'facet_ranges', 'publication_year', 'counts', default=None)
        if hist_facets is None:
            return None

        return {hist_facets[i]: hist_facets[i + 1] for i in range(0, len(hist_facets), 2)}  # type: ignore[misc]

    @classmethod
    def translate_record(cls, record: dict[str, Any], project_id: str | uuid.UUID | None = None) -> AcademicItemModel:
        work = WorksSchema.model_validate(record)
        return translate_work_to_item(work, project_id=project_id)

    def query(
        self,
        query: str,
        limit: int = 20,
        offset: int = 0,
        params: dict[str, Any] | None = None,
    ) -> SearchResult:
        docs = list(
            self.fetch_translated(
                query=query,
                params={
                    'rows': limit,
                    'start': offset,
                    'cursorMark': None,
                }
                | (params or {}),
            ),
        )

        return SearchResult(
            num_found=self.num_found or 0,
            query_time=self.query_time or 0,
            docs=docs,
            histogram=self.histogram,
        )

    def get_count(self, query: str, params: dict[str, Any] | None = None) -> SearchResult:
        return self.query(query, params=params, limit=0)


def wildcards(
    openalex_config: Annotated[Path, typer.Option(help='Path to env settings')],
    query_file: Annotated[Path, typer.Option(help='Path to text file containing the query')],
    target: Annotated[Path, typer.Option(help='Path to target csv file')],
    limit: Annotated[int, typer.Option(help='Number of terms to return per wildcard')] = 100,
) -> None:
    import re
    import pandas as pd
    from tqdm import tqdm
    from httpx import Client

    wc = re.compile(r'["*\-\s](\w+?)\*')

    conf = load_settings(openalex_config)

    with open(query_file) as f:
        query = f.read()

    stats = []
    with Client(timeout=120) as client:
        for term in tqdm(set(wc.findall(query))):
            url = (
                f'{conf.OPENALEX.solr_url}/terms'
                f'?facet=true'
                f'&indent=true'
                f'&q.op=OR'
                f'&q=*%3A*'
                f'&terms.fl=title_abstract'
                f'&terms.limit={limit}'
                f'&terms.prefix={term}'
                f'&terms.stats=true'
                f'&terms.ttf=true'
                f'&terms=true'
                f'&useParams='
            )
            print(url)

            response = client.get(url)
            terms = response.json()['terms']['title_abstract']
            stats.extend(
                [
                    {
                        'prefix': term,
                        'term': terms[i],
                        'df': terms[i + 1]['df'],
                        'ttf': terms[i + 1]['ttf'],
                    }
                    for i in range(0, len(terms), 2)
                ],
            )

    (pd.DataFrame(stats).sort_values(['prefix', 'df'], ascending=False).to_csv(target, index=False))

    df = pd.read_csv(target)
    for g, vs in df[df['df'] > 10].groupby('prefix'):
        print(f"'{g}': {vs['term'].tolist()}")
