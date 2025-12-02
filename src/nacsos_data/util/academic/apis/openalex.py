import json
import uuid
import logging
from pathlib import Path
from time import time
from datetime import timedelta
from typing import Any, Generator, Annotated, Literal, get_args

import httpx
from httpx import Response, HTTPStatusError
from pydantic import BaseModel

from nacsos_data.models.items import AcademicItemModel
from nacsos_data.models.items.academic import AcademicAuthorModel, AffiliationModel
from nacsos_data.models.openalex import WorksSchema, AuthorshipsSchema, DefType, SearchField, OpType, AbstractSource
from nacsos_data.util import clear_empty, as_uuid, get
from nacsos_data.util.academic.apis.util import RequestClient, AbstractAPI
from nacsos_data.util.conf import OpenAlexConfig, load_settings

FIELDS_API = {
    'id',
    'doi',
    'title',
    'display_name',
    'publication_year',
    'publication_date',
    'ids',
    'language',
    'primary_location',
    # 'sources',
    'type',
    'type_crossref',
    'indexed_in',
    'open_access',
    'authorships',
    # 'institution_assertions',
    # 'institutions',
    # 'countries_distinct_count',
    # 'institutions_distinct_count',
    # 'corresponding_author_ids',
    # 'corresponding_institution_ids',
    # 'apc_list',
    # 'apc_paid',
    'fwci',
    # 'is_authors_truncated',
    'has_fulltext',
    # 'fulltext_origin',
    # 'fulltext',
    'cited_by_count',
    # 'citation_normalized_percentile',
    # 'cited_by_percentile_year',
    # 'biblio',
    'is_retracted',
    'is_paratext',
    'is_xpac',
    # 'primary_topic',
    'topics',
    'keywords',
    'concepts',
    # 'mesh',
    # 'locations_count',
    'locations',
    # 'best_oa_location',
    'sustainable_development_goals',
    'grants',
    # 'awards',
    # 'funders',
    # 'datasets',
    # 'versions',
    'has_content',
    # 'referenced_works_count',
    'referenced_works',
    # 'related_works',
    'abstract_inverted_index',
    # 'cited_by_api_url',
    # 'counts_by_year',
    'updated_date',
    'created_date',
}

FIELDS_SOLR: set[str] = FIELDS_API - {
    'abstract_inverted_index',
    'ids',
    'display_name',
    'primary_location',
} | {
    'abstract',
    'title_abstract',
    'abstract_source',
    'is_published',
    'is_accepted',
    'is_open_access',
    'publisher',
    'publisher_id',
    'source',
    'source_id',
    'id_mag',
    'id_pmid',
    'id_pmcid',
}
FIELDS_META = set(FIELDS_SOLR) - {'abstract', 'abstract_inverted_index'}

NESTED_FIELDS = {field for field, dtype in WorksSchema.model_fields.items() if get_args(dtype.annotation)[0] not in {str, int, float, bool, AbstractSource}}


def translate_work_to_solr(work: WorksSchema, source: str = 'OpenAlex') -> dict[str, str | bool | int | float]:
    doc = work.model_dump(include=FIELDS_SOLR, exclude_none=True, exclude_unset=True)
    return (
        doc
        | {
            'title_abstract': work.tiab,
            'abstract_source': source if work.abstract else None,
        }
        | {field: json.dumps(doc[field]) for field in NESTED_FIELDS if field in doc}
    )


def translate_authorship(author: AuthorshipsSchema) -> AcademicAuthorModel:
    ret = AcademicAuthorModel(name='[missing]')
    if author.author is not None:
        if author.author.display_name is not None:
            ret.name = author.author.display_name
        ret.openalex_id = author.author.id
        ret.orcid = author.author.orcid
    if author.institutions is not None:
        ret.affiliations = [
            AffiliationModel(name=inst.display_name if inst.display_name is not None else '[missing]', openalex_id=inst.id, country=inst.country_code)
            for inst in author.institutions
        ]
    elif author.raw_affiliation_strings is not None:
        ret.affiliations = [AffiliationModel(name=affil) for affil in author.raw_affiliation_strings]

    return ret


def translate_work_to_item(work: WorksSchema, project_id: str | uuid.UUID | None = None) -> AcademicItemModel:
    source = None
    if work.locations is not None and len(work.locations) > 0:
        # find the first location with a source name
        for loc in work.locations:
            if loc.source is not None and loc.source.display_name is not None:
                source = loc.source.display_name
                break

    return AcademicItemModel(
        item_id=uuid.uuid4(),
        doi=work.doi,
        project_id=as_uuid(project_id),
        openalex_id=work.id,
        pubmed_id=work.id_pmid,
        title=work.title,
        text=work.abstract,
        publication_year=work.publication_year,
        source=source,
        authors=[translate_authorship(a) for a in work.authorships] if work.authorships is not None else None,
        meta=clear_empty(
            {
                'openalex': work.model_dump(include=FIELDS_META, exclude_none=True, exclude_unset=True),
            },
        ),
    )


class OpenAlexAPI(AbstractAPI):
    def fetch_raw(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        """
        OpenAlex via API wrapper for downloading all records for a given query.

        Documentation:
        https://docs.openalex.org/

        https://docs.openalex.org/api-entities/works/filter-works#from_created_date
        --> https://api.openalex.org/works?filter=from_created_date:2023-01-12&api_key=myapikey
        """
        cursor = '*'
        n_pages = 0
        n_works = 0
        headers = {'api_key': self.api_key} if self.api_key else None
        with RequestClient(
            backoff_rate=self.backoff_rate,
            max_req_per_sec=self.max_req_per_sec,
            max_retries=self.max_retries,
            proxy=self.proxy,
        ) as request_client:
            while cursor is not None:
                n_pages += 1

                req = request_client.get(
                    'https://api.openalex.org/works',
                    params={  # type: ignore[arg-type]
                        'filter': query,
                        'select': ','.join(FIELDS_API),
                        'cursor': cursor,
                        'per-page': 50,
                    }
                    | (params or {}),
                    headers=headers,
                )
                try:
                    req.raise_for_status()
                except HTTPStatusError as e:
                    self.logger.error(e.response.text)
                    raise e
                page = req.json()
                cursor = page['meta']['next_cursor']
                self.logger.info(f'Retrieved {n_works:,} / {page["meta"]["count"]:,} | currently on page {n_pages:,}')

                yield from page['results']
                n_works += len(page['results'])

    @classmethod
    def translate_record(cls, record: dict[str, Any], project_id: str | uuid.UUID | None = None) -> AcademicItemModel:
        work = WorksSchema.model_validate(record)
        return translate_work_to_item(work=work, project_id=project_id)

    def get_count(self, query: str, params: dict[str, Any] | None = None) -> int | None:
        headers = {'api_key': self.api_key} if self.api_key else None

        page = httpx.get(
            'https://api.openalex.org/works',
            params={  # type: ignore[arg-type]
                'filter': query,
                'select': 'id',
                'per-page': 1,
            }
            | (params or {}),
            headers=headers,
        )
        try:
            page.raise_for_status()
        except HTTPStatusError as e:
            self.logger.error(e.response.text)
            raise e

        return page.json().get('meta', {}).get('count', None)


class SearchResult(BaseModel):
    query_time: int
    num_found: int
    docs: list[AcademicItemModel]
    histogram: dict[str, int] | None = None


class OpenAlexSolrAPI(AbstractAPI):
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
        with RequestClient(
            backoff_rate=self.backoff_rate,
            max_req_per_sec=self.max_req_per_sec,
            max_retries=self.max_retries,
            proxy=self.proxy,
            auth=self.openalex_conf.auth,
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
                res = request_client.post(f'{self.openalex_conf.solr_url}/select', data=params_, timeout=60).json()

                next_curser = res.get('nextCursorMark')
                params_['cursorMark'] = next_curser

                batch_docs = res['response']['docs']
                n_docs_batch = len(batch_docs)
                num_docs_cum += n_docs_batch
                self.num_found = res['response']['numFound']
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
        hist_facets = get(response, 'facet_counts', 'facet_ranges', 'publication_year', 'counts', default=None)
        if hist_facets is None:
            return None

        return {hist_facets[i]: hist_facets[i + 1] for i in range(0, len(hist_facets), 2)}

    @classmethod
    def translate_record(cls, record: dict[str, Any], project_id: str | uuid.UUID | None = None) -> AcademicItemModel:
        work = WorksSchema.model_validate(record)
        return translate_work_to_item(work, project_id=project_id)

    def query(
        self,
        query: str,
        limit: int = 20,
        offset: int = 0,
    ) -> SearchResult:
        docs = list(
            self.fetch_translated(
                query=query,
                params={
                    'rows': limit,
                    'start': offset,
                    'cursorMark': None,
                },
            ),
        )

        return SearchResult(
            num_found=self.num_found or 0,
            query_time=self.query_time or 0,
            docs=docs,
            histogram=self.histogram,
        )

    def get_count(self, query: str) -> SearchResult:
        return self.query(query, limit=0)


if __name__ == '__main__':
    import typer

    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(name)s (%(process)d): %(message)s', level='DEBUG')

    fin = Path('../../../../../scratch/snippet.jsonl').resolve()
    print(fin)
    with open(fin) as f:
        for li, line in enumerate(f):
            print(li)
            work = WorksSchema.model_validate_json(line)
            # print(work.model_dump(exclude_unset=True, exclude_none=True))
            print(translate_work_to_solr(work))

    app = typer.Typer()

    @app.command()
    def download(
        target: Annotated[Path, typer.Option(help='File to write results to')],
        api_key: Annotated[str | None, typer.Option(help='Valid API key')] = None,
        kind: Annotated[Literal['SOLR', 'API'], typer.Option(help='database to use')] = 'SOLR',
        openalex_conf: Annotated[str | None, typer.Option(help='NACSOS config with solr settings')] = None,
        batch_size: Annotated[int, typer.Option(help='File to write results to')] = 5,
        query_file: Annotated[Path | None, typer.Option(help='File containing search query')] = None,
        query: Annotated[str | None, typer.Option(help='Search query')] = None,
    ) -> None:
        if query_file:
            with open(query_file, 'r') as qf:
                query_str = qf.read()
        elif query:
            query_str = query
        else:
            raise AttributeError('Must provide either `query_file` or `query`')

        conf = load_settings(conf_file=openalex_conf)

        api: OpenAlexSolrAPI | OpenAlexAPI
        if kind == 'SOLR':
            api = OpenAlexSolrAPI(openalex_conf=conf.OPENALEX, batch_size=batch_size)
        elif kind == 'API':
            api = OpenAlexAPI(api_key=api_key or conf.OPENALEX.API_KEY)
        else:
            raise AttributeError(f'Unknown API type: {kind}')
        api.download_raw(query=query_str, target=target)

    @app.command()
    def convert(
        source: Annotated[Path, typer.Option(help='File to read results from')],
        target: Annotated[Path, typer.Option(help='File to write results to')],
        kind: Annotated[Literal['SOLR', 'API'], typer.Option(help='database to use')] = 'SOLR',
    ) -> None:
        cls = OpenAlexSolrAPI if kind == 'SOLR' else OpenAlexAPI
        cls.convert_file(source=source, target=target)

    @app.command()
    def translate(kind: Annotated[Literal['SOLR', 'API'], typer.Option(help='database to use')] = 'SOLR') -> None:
        cls = OpenAlexSolrAPI if kind == 'SOLR' else OpenAlexAPI
        for fp in []:  # type: ignore[var-annotated]
            for item in cls.read_translated(Path(fp)):
                print(item)
