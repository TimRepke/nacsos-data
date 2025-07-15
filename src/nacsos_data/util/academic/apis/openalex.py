import json
import uuid
import logging
from pathlib import Path
from time import time
from datetime import timedelta
from typing import Any, Generator, Annotated, Literal

from nacsos_data.models.items import AcademicItemModel
from nacsos_data.models.items.academic import AcademicAuthorModel, AffiliationModel
from nacsos_data.models.openalex import WorkSolr, Authorship, Biblio, Work, Location
from nacsos_data.util import clear_empty, as_uuid
from nacsos_data.util.academic.apis.util import RequestClient, AbstractAPI

FIELDS_API = [
    'id',
    'doi',
    'title',
    'display_name',
    'publication_year',
    'publication_date',
    'ids',
    'language',
    'primary_location',
    'type',
    'type_crossref',
    'indexed_in',
    'open_access',
    'authorships',
    # 'institution_assertions',
    # 'countries_distinct_count',
    # 'institutions_distinct_count',
    # 'corresponding_author_ids',
    # 'corresponding_institution_ids',
    'apc_list',
    'apc_paid',
    'fwci',
    'has_fulltext',
    'fulltext_origin',
    # 'cited_by_count',
    # 'citation_normalized_percentile',
    # 'cited_by_percentile_year',
    # 'biblio',
    'is_retracted',
    'is_paratext',
    # 'primary_topic',
    'topics',
    'keywords',
    # 'concepts',
    # 'mesh',
    # 'locations_count',
    'locations',
    'best_oa_location',
    # 'sustainable_development_goals',
    'grants',
    'datasets',
    # 'versions',
    # 'referenced_works_count',
    'referenced_works',
    # 'related_works',
    'abstract_inverted_index',
    # 'cited_by_api_url',
    # 'counts_by_year',
    'updated_date',
    'created_date'
]
FIELDS_SOLR = [
    'id', 'title', 'abstract', 'display_name',  # 'title_abstract',
    'publication_year', 'publication_date',
    'created_date', 'updated_date',
    'cited_by_count', 'type', 'doi', 'mag', 'pmid', 'pmcid',
    'is_oa', 'is_paratext', 'is_retracted',
    'locations', 'authorships', 'biblio', 'language'
]


def translate_doc(doc: WorkSolr) -> Work:
    locations = None
    if doc.locations is not None:
        raw = json.loads(doc.locations)
        locations = [
            Location.model_validate(loc)
            for loc in raw
        ]
    authorships = None
    if doc.authorships is not None:
        raw = json.loads(doc.authorships)
        authorships = [
            Authorship.model_validate(loc)
            for loc in raw
        ]

    return Work.model_validate({
        **doc.model_dump(),
        'locations': locations,
        'authorships': authorships,
        'biblio': Biblio.model_validate(json.loads(doc.biblio)) if doc.biblio is not None else None
    })


def translate_authorship(author: Authorship) -> AcademicAuthorModel:
    ret = AcademicAuthorModel(name='[missing]')
    if author.author is not None:
        if author.author.display_name is not None:
            ret.name = author.author.display_name
        ret.openalex_id = author.author.id
        ret.orcid = author.author.orcid
    if author.institutions is not None:
        ret.affiliations = [
            AffiliationModel(name=inst.display_name if inst.display_name is not None else '[missing]',
                             openalex_id=inst.id,
                             country=inst.country_code)
            for inst in author.institutions
        ]
    elif author.raw_affiliation_string is not None:
        ret.affiliations = [AffiliationModel(name=author.raw_affiliation_string)]

    return ret


def translate_record(work: Work, project_id: str | uuid.UUID | None = None) -> AcademicItemModel:
    source = None
    if work.locations is not None and len(work.locations) > 0:
        # find the first location with a source name
        for loc in work.locations:
            if loc.source is not None and loc.source.display_name is not None:
                source = loc.source.display_name
                break

    doi: str | None = None
    if work.doi is not None:
        doi = work.doi.replace('https://doi.org/', '')

    return AcademicItemModel(
        item_id=uuid.uuid4(),
        doi=doi,
        project_id=as_uuid(project_id),
        openalex_id=work.id,
        pubmed_id=work.pmid,
        title=work.title,
        text=work.abstract,
        publication_year=work.publication_year,
        source=source,
        authors=[translate_authorship(a) for a in work.authorships] if work.authorships is not None else None,

        meta=clear_empty({
            'openalex': {
                'locations': work.locations,
                'type': work.type,
                'updated_date': work.updated_date,
                'mag': work.mag,
                'pmid': work.pmid,
                'pmcid': work.pmcid,
                'display_name': work.display_name,
                'is_oa': work.is_oa,
                'is_paratext': work.is_paratext,
                'is_retracted': work.is_retracted,
                'language': work.language
            }
        })
    )


class OpenAlexAPI(AbstractAPI):

    def fetch_raw(self, query: str) -> Generator[dict[str, Any], None, None]:
        """
           OpenAlex via API wrapper for downloading all records for a given query.

           Documentation:
           https://docs.openalex.org/

           https://docs.openalex.org/api-entities/works/filter-works#from_created_date
           --> https://api.openalex.org/works?filter=from_created_date:2023-01-12&api_key=myapikey

           :param query:
           :return:
           """
        cursor = '*'
        n_pages = 0
        n_works = 0

        with RequestClient(backoff_rate=self.backoff_rate,
                           max_req_per_sec=self.max_req_per_sec,
                           max_retries=self.max_retries,
                           proxy=self.proxy) as request_client:
            while cursor is not None:
                n_pages += 1

                page = request_client.get(
                    'https://api.openalex.org/works',
                    params={
                        'filter': query,
                        'select': ','.join(FIELDS_API),
                        'cursor': cursor,
                        'per-page': 50
                    },
                    headers={'api_key': self.api_key},
                ).json()
                cursor = page['meta']['next_cursor']
                self.logger.info(f'Retrieved {n_works:,} / {page['meta']['count']:,} | currently on page {n_pages:,}')

                yield from page['results']
                n_works += len(page['results'])

    @classmethod
    def translate_record(cls, record: dict[str, Any], project_id: str | uuid.UUID | None = None) -> AcademicItemModel:
        work = Work.model_validate(record)
        return translate_record(work=work, project_id=project_id)


class OpenAlexSolrAPI(AbstractAPI):
    def __init__(self,
                 api_key: str,
                 openalex_endpoint: str,
                 batch_size: int = 5000,
                 proxy: str | None = None,
                 max_req_per_sec: int = 5,
                 max_retries: int = 5,
                 backoff_rate: float = 5.,
                 logger: logging.Logger | None = None):
        super().__init__(api_key=api_key, proxy=proxy, max_retries=max_retries,
                         max_req_per_sec=max_req_per_sec, backoff_rate=backoff_rate, logger=logger)
        self.openalex_endpoint = openalex_endpoint
        self.batch_size = batch_size

    def fetch_raw(self, query: str) -> Generator[dict[str, Any], None, None]:
        """
           OpenAlex via solr wrapper for downloading all records for a given query.

           Documentation:
           https://solr.apache.org/guide/solr/latest/query-guide/standard-query-parser.html
           https://solr.apache.org/guide/solr/latest/query-guide/common-query-parameters.html
           https://solr.apache.org/guide/solr/latest/query-guide/other-parsers.html#surround-query-parser

           :param query:
           :return:
           """
        with RequestClient(backoff_rate=self.backoff_rate,
                           max_req_per_sec=self.max_req_per_sec,
                           max_retries=self.max_retries,
                           proxy=self.proxy) as request_client:

            params = {
                'q': query,
                'q.op': 'AND',
                'sort': 'id desc',
                'fl': ','.join(FIELDS_SOLR),
                'rows': self.batch_size,
                'df': 'title_abstract',
                'defType': 'lucene',
                'cursorMark': '*'
            }

            t0 = time()
            self.logger.info(f'Querying endpoint with batch_size={self.batch_size:,}: {self.openalex_endpoint}')
            self.logger.info(f'Request parameters: {params}')

            batch_i = 0
            num_docs_cum = 0
            while True:
                t1 = time()
                batch_i += 1
                self.logger.info(f'Running query for batch {batch_i} with cursor "{params["cursorMark"]}"')
                t2 = time()
                res = request_client.post(f'{self.openalex_endpoint}/select', data=params, timeout=60).json()

                next_curser = res.get('nextCursorMark')
                params['cursorMark'] = next_curser
                n_docs_total = res['response']['numFound']
                batch_docs = res['response']['docs']
                n_docs_batch = len(batch_docs)
                num_docs_cum += n_docs_batch

                self.logger.debug(f'Query took {timedelta(seconds=time() - t2)}h and yielded {n_docs_batch:,} docs')
                if n_docs_total > 0:
                    self.logger.debug(f'Current progress: {num_docs_cum:,}/{n_docs_total:,}={num_docs_cum / n_docs_total:.2%} docs')

                if len(batch_docs) == 0:
                    self.logger.info('No documents in this batch, assuming to be done!')
                    break

                self.logger.debug('Yielding documents...')
                yield from batch_docs

                self.logger.debug(f'Done with batch {batch_i} in {timedelta(seconds=time() - t1)}h; '
                                  f'{timedelta(seconds=time() - t0)}h passed overall')

                if next_curser is None:
                    self.logger.info('Did not receive a `nextCursorMark`, assuming to be done!')
                    break

    @classmethod
    def translate_record(cls, record: dict[str, Any], project_id: str | uuid.UUID | None = None) -> AcademicItemModel:
        doc = WorkSolr.model_validate(record)
        work = translate_doc(doc)
        return translate_record(work, project_id=project_id)


if __name__ == '__main__':
    import typer

    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(name)s (%(process)d): %(message)s', level='DEBUG')

    app = typer.Typer()

    @app.command()
    def download(
            target: Annotated[Path, typer.Option(help='File to write results to')],
            api_key: Annotated[str | None, typer.Option(help='Valid API key')] = None,
            kind: Annotated[Literal['SOLR', 'API'], typer.Option(help='database to use')] = 'SOLR',
            openalex_endpoint: Annotated[str | None, typer.Option(help='solr endpoint')] = None,
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
        api: OpenAlexSolrAPI | OpenAlexAPI
        if kind == 'SOLR' and openalex_endpoint is not None:
            api = OpenAlexSolrAPI(api_key='', openalex_endpoint=openalex_endpoint, batch_size=batch_size)
        elif kind == 'API' and api_key is not None:
            api = OpenAlexAPI(api_key=api_key)
        else:
            raise AttributeError('Must provide either `openalex_endpoint` or `api_key`')
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
