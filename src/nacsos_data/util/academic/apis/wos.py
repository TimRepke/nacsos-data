import uuid
import logging
from dataclasses import dataclass
from typing import Any, Generator, Literal, TypeVar
from httpx import codes as http_status
from nacsos_data.models.items import AcademicItemModel
from nacsos_data.models.items.academic import AcademicAuthorModel
from nacsos_data.models.web_of_science import WosRecord
from nacsos_data.util import as_uuid, get_value, clear_empty, get
from nacsos_data.util.academic.apis.util import RequestClient, AbstractAPI

# WOS - Web of Science Core collection
# BIOABS - Biological Abstracts
# BCI - BIOSIS Citation Index
# BIOSIS - BIOSIS Previews
# CCC - Current Contents Connect
# DIIDW - Derwent Innovations Index
# DRCI - Data Citation Index
# MEDLINE - MEDLINE The U.S. National Library of Medicine® (NLM®) premier life sciences database.
# ZOOREC - Zoological Records
# PPRN - Preprint Citation Index
# WOK - All databases
Database = Literal['WOS', 'BCI', 'BIOABS', 'BIOSIS', 'CCC', 'DIIDW', 'DRCI', 'MEDLINE', 'PPRN', 'WOK', 'ZOOREC']

# FR - Retrieves all metadata of the document (also known as FullRecord)
# SR - Retrieves a short version of the document that would not count against the quota. The response fields are similar to the Web of Science Starter API fields.
# FS - Custom Field Selection - must be combined with query parameter viewField. It is automatically selected if viewField is used.
ViewOption = Literal['FR', 'SR', 'FS']

T = TypeVar('T')


def dump_wos_record(record: WosRecord) -> dict[str, Any]:
    return clear_empty(record.model_dump(exclude_none=True, exclude_defaults=True, exclude_unset=True))  # type: ignore[return-value]


def get_title(wr: WosRecord) -> str | None:
    if not wr.static_data.summary or not wr.static_data.summary.titles or not wr.static_data.summary.titles.title:
        return None
    for title in wr.static_data.summary.titles.title:
        if title.type == 'item':
            return title.content
    return None


def get_abstract(wr: WosRecord) -> str | None:
    if (
        wr.static_data.fullrecord_metadata is None
        or wr.static_data.fullrecord_metadata.abstracts is None
        or wr.static_data.fullrecord_metadata.abstracts.abstract is None
    ):
        return None
    abstracts = get_value(lambda: wr.static_data.fullrecord_metadata.abstracts.abstract)  # type: ignore[union-attr]
    if abstracts is None:
        return None
    for abstract in abstracts:
        abstract_ = get_value(lambda: abstract.abstract_text.p)  # type: ignore[union-attr]  # noqa: B023
        if abstract_ is None:
            return None
        for abstract__ in abstract_:
            if abstract__ is not None and len(abstract__.strip()) > 5:
                return abstract__.strip()
    return None


def get_doi(wr: WosRecord) -> str | None:
    if (
        wr.dynamic_data.cluster_related is None
        or wr.dynamic_data.cluster_related.identifiers is None
        or wr.dynamic_data.cluster_related.identifiers.IdentifierItem is None
    ):
        return None
    identifiers = get_value(lambda: wr.dynamic_data.cluster_related.identifiers.IdentifierItem)  # type: ignore[union-attr]
    if identifiers is None:
        return None
    for identifier in identifiers:
        if identifier.type == 'doi':
            return identifier.value
    return None


def get_source(wr: WosRecord) -> str | None:
    if wr.static_data.summary is None or wr.static_data.summary.titles is None or wr.static_data.summary.titles.title is None:
        return None
    for title in wr.static_data.summary.titles.title:
        if title.type == 'source':
            return title.content
    return None


def get_keywords(wr: WosRecord) -> list[str] | None:
    if (
        wr.static_data.fullrecord_metadata is None
        or wr.static_data.fullrecord_metadata.keywords is None
        or wr.static_data.fullrecord_metadata.keywords.keyword is None
    ):
        return None

    if wr.static_data.item is None or wr.static_data.item.keywords_plus is None or wr.static_data.item.keywords_plus.keyword is None:
        return None
    kw1 = get_value(lambda: wr.static_data.fullrecord_metadata.keywords.keyword)  # type: ignore[union-attr]
    kw2 = get_value(lambda: wr.static_data.item.keywords_plus.keyword)  # type: ignore[union-attr]
    if kw1 and kw2:
        return kw1 + kw2
    if kw1:
        return kw1
    if kw2:
        return kw2
    return None


def translate_authors(record: WosRecord) -> list[AcademicAuthorModel] | None:
    # TODO: Switch to using AddressName via record.static_data.fullrecord_metadata.addresses.address_name
    if record.static_data.summary is None or record.static_data.summary.names is None or record.static_data.summary.names.name is None:
        return None
    authors = get_value(lambda: record.static_data.summary.names.name)  # type: ignore[union-attr]
    if not authors:
        return None
    return [AcademicAuthorModel(name=author.full_name) for author in authors if author.full_name is not None]


@dataclass
class State:
    n_pages: int = 0
    n_records: int = 0

    query_id: int = 0
    records_searched: int = 0
    records_found: int = 0


class WoSAPI(AbstractAPI):
    def __init__(
        self,
        api_key: str,
        # Number of records to return, must be 0-100.
        page_size: int = 5,
        # Database to search. WOK represents all databases.
        # Available values : WOS, BCI, BIOABS, BIOSIS, CABI, CCC, CSCD, DCI, DIIDW, FSTA, GRANTS, INSPEC, MEDLINE, PPRN, PQDT, SCIELO, WOK, ZOOREC
        database: str = 'WOK',
        proxy: str | None = None,
        max_req_per_sec: int = 5,
        max_retries: int = 5,
        backoff_rate: float = 5.0,
        logger: logging.Logger | None = None,
    ):
        super().__init__(api_key=api_key, proxy=proxy, max_retries=max_retries, max_req_per_sec=max_req_per_sec, backoff_rate=backoff_rate, logger=logger)
        self.database = database
        self.page_size = page_size

    def fetch_raw(self, query: str, params: dict[str, Any] | None = None) -> Generator[dict[str, Any], None, None]:
        """
        Web of Science ExpandedAPI wrapper for downloading all records for a given query.

        Documentation:
        https://developer.clarivate.com/apis/wos
        https://api.clarivate.com/swagger-ui/?apikey=none&url=https%3A%2F%2Fdeveloper.clarivate.com%2Fapis%2Fwos%2Fswagger

        Search syntax
        https://webofscience.help.clarivate.com/en-us/Content/advanced-search.html
        https://webofscience.help.clarivate.com/en-us/Content/wos-core-collection/woscc-search-field-tags.htm

        :return:
        """
        if self.api_key is None:
            raise AssertionError('Missing API key!')

        skip_step = int(self.page_size / self.max_retries) + 2

        with RequestClient(
            backoff_rate=self.backoff_rate,
            max_req_per_sec=self.max_req_per_sec,
            max_retries=self.max_retries,
            proxy=self.proxy,
            params={
                'usrQuery': query,
                'count': self.page_size,
                'databaseId': self.database,
                'optionView': 'FR',
                'firstRecord': 1,
            }
            | (params or {}),
            headers={
                'X-ApiKey': self.api_key,
            },
        ) as request_client:
            state = State()

            def skip_on_error(_response: Any) -> dict[str, Any]:
                state.n_records += skip_step
                request_client.kwargs['params']['firstRecord'] += skip_step
                request_client.time_per_request = 1 / request_client.max_req_per_sec
                return {}

            request_client.on(http_status.INTERNAL_SERVER_ERROR, skip_on_error)
            page = request_client.get('https://api.clarivate.com/api/wos')

            # FIXME: deal with HTTPStatusError: Client error '429 Too Many Requests' for url

            while True:
                state.n_pages += 1
                self.logger.info(f'Fetching page {state.n_pages}...')
                try:
                    data = page.json()

                    # Gather info from meta-data (only on first page)
                    if 'QueryResult' in data:
                        query_id = data['QueryResult']['QueryID']
                        records_searched = data['QueryResult']['RecordsSearched']
                        records_found = data['QueryResult']['RecordsFound']
                        self.n_results = records_found  # looks odd, but better ensures compatibility

                    # Gather info from header
                    # next_page = page.headers.get('x-paginate-by-query-id')
                    self.api_feedback = {
                        'remaining_year': page.headers.get('x-rec-amtperyear-remaining'),
                        'remaining_sec': page.headers.get('x-req-reqpersec-remaining'),
                    }

                    # Records are nested in Data on first page
                    records: list[dict[str, Any]] | None = get(data['Data'] if 'Data' in data else data, 'Records', 'records', 'REC', default=[])

                    if records is None or len(records) == 0:
                        self.logger.info('No more records received.')
                        break

                    yield from records

                    state.n_records += len(records)
                    self.logger.info(
                        f'Found {state.n_records:,}/{records_found:,} records in {records_searched:,} records '
                        f'after processing page {state.n_pages} for query {query_id} '
                        f'{self.api_feedback}',
                    )

                    if state.n_records >= records_found:
                        self.logger.info('Reached num_results')
                        break

                    request_client.kwargs['params'] = {
                        'count': self.page_size,
                        'sortField': 'LD+D',
                        'optionView': 'FR',
                        'firstRecord': state.n_records + 1,
                    }
                    page = request_client.get(f'https://api.clarivate.com/api/wos/query/{query_id}')

                except Exception as e:
                    logging.warning(f'Failed: {e}')
                    if hasattr(e, 'response') and hasattr(e.response, 'text'):
                        logging.warning(e.response.text)
                    logging.exception(e)
                    raise e

    @classmethod
    def translate_record(cls, record: dict[str, Any], project_id: str | uuid.UUID | None = None) -> AcademicItemModel:
        wos_record = WosRecord.model_validate(record)
        return AcademicItemModel(
            item_id=uuid.uuid4(),
            project_id=as_uuid(project_id),
            doi=get_doi(wos_record),
            title=get_title(wos_record),
            # title_slug  # not required
            wos_id=wos_record.UID,
            text=get_abstract(wos_record),
            publication_year=get_value(lambda: wos_record.static_data.summary.pub_info.pubyear),  # type: ignore[union-attr]
            source=get_source(wos_record),
            keywords=get_keywords(wos_record),
            authors=translate_authors(wos_record),
            meta={'wos-api': dump_wos_record(wos_record)},
        )


if __name__ == '__main__':
    import json

    app = WoSAPI.test_app(
        static_files=[
            # 'scratch/academic_apis/response_scopus1.json',
            # 'scratch/academic_apis/response_scopus2.jsonl',
        ],
    )

    @app.command()  # type: ignore[untyped-decorator]
    def offline() -> None:
        for fp in [
            'scratch/academic_apis/wos_response.json',
            'scratch/academic_apis/response2.json',
        ]:
            with open(fp, 'r') as f:
                data = json.load(f)
                records: list[dict[str, Any]] | None = get(data, 'Data', 'Records', 'records', 'REC', default=[])
                if not records:
                    return

                for record in records:
                    item = WoSAPI.translate_record(record)
                    print(item)

    app()
