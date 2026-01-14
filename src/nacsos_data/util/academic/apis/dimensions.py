import logging
import uuid
from typing import Any, Generator
import orjson as json
from httpx import codes, HTTPError, Response
from nacsos_data.models.items.academic import AcademicAuthorModel, AcademicItemModel, AffiliationModel
from nacsos_data.util import get, as_uuid, clear_empty
from nacsos_data.util.academic.apis.util import RequestClient, AbstractAPI

FIELDS = [  # FIXME: would be nice to activate ALL fields but that leads to a "too many" error (2026-01-14)...
    'abstract',
    'acknowledgements',
    'altmetric',
    'altmetric_id',
    'arxiv_id',
    'authors',
    'authors_count',
    'book_doi',
    'book_series_title',
    'book_title',
    'category_bra',
    'category_for',
    'category_for_2020',
    'category_hra',
    'category_hrcs_hc',
    'category_hrcs_rac',
    'category_icrp_cso',
    'category_icrp_ct',
    'category_rcdc',
    'category_sdg',
    'category_uoa',
    'clinical_trial_ids',
    'concepts',
    'concepts_scores',
    'date',
    'date_inserted',
    'date_online',
    'date_print',
    'dimensions_url',
    'document_type',
    'doi',
    'editors',
    'field_citation_ratio',
    'funder_countries',
    'funders',
    'funding_section',
    'id',
    'isbn',
    'issn',
    'issue',
    'journal',
    'journal_lists',
    'journal_title_raw',
    # 'linkout',
    # 'mesh_terms',
    # 'open_access',
    # 'pages',
    'pmcid',
    'pmid',
    'proceedings_title',
    'publisher',
    # 'recent_citations',
    'reference_ids',
    # 'referenced_pubs',
    # 'relative_citation_ratio',
    # 'research_org_cities',
    # 'research_org_countries',
    # 'research_org_country_names',
    # 'research_org_names',
    # 'research_org_state_codes',
    # 'research_org_state_names',
    # 'research_org_types',
    # 'research_orgs',
    'researchers',
    # 'resulting_publication_doi',
    # 'score',
    'source_title',
    'subtitles',
    'supporting_grant_ids',
    # 'times_cited',
    'title',
    'type',
    'volume',
    'year',
]


def get_str(obj: dict[str, Any], k: str) -> str | None:
    v = obj.get(k)
    return v if type(v) is str else None


def get_int(obj: dict[str, Any], k: str) -> int | None:
    v = obj.get(k)
    return v if type(v) is int else None


def get_source(obj: dict[str, Any]) -> str | None:
    v = obj.get('journal', {}).get('title')
    if v is not None and type(v) is str:
        return v  # type: ignore[no-any-return]
    v = obj.get('source_title', {}).get('title')
    return v if type(v) is str else None


def translate_author(record: dict[str, Any]) -> AcademicAuthorModel | None:
    orcid = record.get('orcid')  # "orcid": "['s']",

    if orcid is not None:
        try:
            if type(orcid) is str:
                orcid = json.loads(orcid)
            if type(orcid) is list and len(orcid) > 0:
                orcid = orcid[0]
        except Exception:
            pass

        if type(orcid) is not str:
            orcid = None

    return AcademicAuthorModel(
        name=f'{record.get("first_name")} {record.get("last_name")}',
        orcid=orcid,
        dimensions_id=record.get('researcher_id'),
        affiliations=[
            AffiliationModel(
                name=affiliation.get('name'),
                country=affiliation.get('country_code'),
            )
            for affiliation in record.get('affiliations', [])
        ],
    )


class DimensionsAPI(AbstractAPI):
    def __init__(
        self,
        api_key: str,
        page_size: int = 5,
        proxy: str | None = None,
        max_req_per_sec: int = 5,
        max_retries: int = 5,
        backoff_rate: float = 5.0,
        fields: list[str] | None = None,
        logger: logging.Logger | None = None,
    ):
        super().__init__(
            api_key=api_key,
            proxy=proxy,
            max_retries=max_retries,
            max_req_per_sec=max_req_per_sec,
            backoff_rate=backoff_rate,
            logger=logger,
        )
        self.page_size = page_size
        self.fields = fields if fields is not None and len(fields) > 0 else FIELDS

    def fetch_raw(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        """
        dimensions.ai API wrapper for downloading all records for a given query.

        Documentation:
        https://docs.dimensions.ai/dsl/language.html
        https://github.com/digital-science/dimcli/blob/master/dimcli/core/api.py
        fields: https://docs.dimensions.ai/dsl/datasource-publications.html

        Query should look something like this:
        search publications
        where ...
        return publications[basics+categories+extras+book]

        :param query:
        :param params:
        :return:
        """

        if self.api_key is None:
            raise AssertionError('Missing API key!')

        with RequestClient(
            backoff_rate=self.backoff_rate,
            max_req_per_sec=self.max_req_per_sec,
            max_retries=self.max_retries,
            proxy=self.proxy,
        ) as request_client:
            headers = {
                'Accept': 'application/json',
                'Authorization': 'JWT empty',
            }
            logger = self.logger
            api_key = self.api_key

            def update_jwt(response: Response) -> dict[str, dict[str, str]]:
                logger.debug('Fetching JWT token')
                res = request_client.post('https://app.dimensions.ai/api/auth.json', json={'key': api_key})
                res.raise_for_status()
                headers['Authorization'] = f'JWT {res.json()["token"]}'
                return {'headers': headers}

            request_client.on(codes.UNAUTHORIZED, update_jwt)

            where = ''
            if params and 'where' in params:
                where = f' where {params.pop("where")}'

            n_pages = 0
            n_records = 0
            while True:
                logger.info(f'Fetching page {n_pages}...')
                try:
                    content = (
                        f'search publications '
                        f'in title_abstract_only for "{query.replace("\n", " ").replace('"', '\\"')}" {where} '
                        f'return publications[{"+".join(self.fields)}] '
                        f'sort by id '
                        f'limit {self.page_size} skip {n_pages * self.page_size} '
                    )
                    logger.debug(f'Query: {content}')
                    page = request_client.post(
                        url='https://app.dimensions.ai/api/dsl/v2',
                        content=content,
                        headers=headers,
                    )

                    n_pages += 1
                    data = page.json()

                    n_results = get(data, '_stats', 'total_count', default=0)
                    entries = get(data, 'publications', default=[])

                    if len(entries) == 0 or n_results == 0 or n_records >= n_results:
                        break

                    for entry in entries:
                        n_records += 1
                        yield entry
                    logger.debug(f'Found {n_records:,} records after processing page {n_pages} (total {n_results:,} records)')
                except HTTPError as e:
                    logging.warning(f'Failed: {e}')
                    logging.warning(e.response.text)  # type: ignore[attr-defined]
                    logging.warning(e.request.content)
                    logging.exception(e)
                    raise e

    @classmethod
    def translate_record(cls, record: dict[str, Any], project_id: str | uuid.UUID | None = None) -> AcademicItemModel:
        return AcademicItemModel(
            item_id=uuid.uuid4(),
            project_id=as_uuid(project_id),
            doi=get_str(record, 'doi'),
            title=get_str(record, 'title'),
            dimensions_id=get_str(record, 'id'),
            text=get_str(record, 'abstract'),
            publication_year=get_int(record, 'year'),
            source=get_source(record),
            authors=[auth for auth in [translate_author(author) for author in record.get('authors', [])] if auth is not None],
            meta={'dimensions': clear_empty(record)},
        )


if __name__ == '__main__':
    app = DimensionsAPI.test_app(
        static_files=[
            # 'scratch/academic_apis/response_scopus1.json',
            # 'scratch/academic_apis/response_scopus2.jsonl',
        ],
    )
    app()
