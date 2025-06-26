import uuid
import logging
from typing import Any, Generator, Literal, TypeVar
from httpx import HTTPError

from nacsos_data.models.items import AcademicItemModel
from nacsos_data.models.items.academic import AcademicAuthorModel
from nacsos_data.models.web_of_science import WosRecord
from nacsos_data.util import as_uuid, get_value, clear_empty
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

T = TypeVar("T")


def dump_wos_record(record: WosRecord) -> dict[str, Any]:
    return clear_empty(record.model_dump(exclude_none=True, exclude_defaults=True, exclude_unset=True))


def get_title(wr: WosRecord) -> str | None:
    for title in wr.static_data.summary.titles.title:
        if title.type == 'item':
            return title.content
    return None


def get_abstract(wr: WosRecord) -> str | None:
    abstracts = get_value(lambda: wr.static_data.fullrecord_metadata.abstracts.abstract)
    if abstracts is None:
        return None
    for abstract in abstracts:
        abstract_ = get_value(lambda: abstract.abstract_text.p)
        if abstract_ is None:
            return None
        for abstract__ in abstract_:
            if abstract__ is not None and len(abstract__.strip()) > 5:
                return abstract__.strip()
    return None


def get_doi(wr: WosRecord) -> str | None:
    identifiers = get_value(lambda: wr.dynamic_data.cluster_related.identifiers.IdentifierItem)
    if identifiers is None:
        return None
    for identifier in identifiers:
        if identifier.type == 'doi':
            return identifier.value
    return None


def get_source(wr: WosRecord) -> str | None:
    for title in wr.static_data.summary.titles.title:
        if title.type == 'source':
            return title.content
    return None


def get_keywords(wr: WosRecord) -> list[str] | None:
    kw1 = get_value(lambda: wr.static_data.fullrecord_metadata.keywords.keyword)
    kw2 = get_value(lambda: wr.static_data.item.keywords_plus.keyword)
    if kw1 and kw2:
        return kw1 + kw2
    if kw1:
        return kw1
    if kw2:
        return kw2
    return None


def translate_authors(record: WosRecord):
    # TODO: Switch to using AddressName via record.static_data.fullrecord_metadata.addresses.address_name
    authors = get_value(lambda: record.static_data.summary.names.name)
    if not authors:
        return None
    return [AcademicAuthorModel(name=author.full_name) for author in authors]


class WoSAPI(AbstractAPI):
    def __init__(self,
                 api_key: str,
                 page_size: int = 5,
                 database: str = 'WOK',
                 proxy: str | None = None,
                 max_req_per_sec: int = 5,
                 max_retries: int = 5,
                 timeout_rate: float = 5.,
                 logger: logging.Logger | None = None):
        super().__init__(api_key=api_key, proxy=proxy, max_retries=max_retries,
                         max_req_per_sec=max_req_per_sec, timeout_rate=timeout_rate, logger=logger)
        self.database = database
        self.page_size = page_size

    def fetch_raw(self, query: str) -> Generator[dict[str, Any], None, None]:
        """
           Web of Science ExpandedAPI wrapper for downloading all records for a given query.

           Documentation:
           https://developer.clarivate.com/apis/wos
           https://api.clarivate.com/swagger-ui/?apikey=none&url=https%3A%2F%2Fdeveloper.clarivate.com%2Fapis%2Fwos%2Fswagger

           Search syntax
           https://webofscience.help.clarivate.com/en-us/Content/advanced-search.html
           https://webofscience.help.clarivate.com/en-us/Content/wos-core-collection/woscc-search-field-tags.htm

           :param query:
           :return:
           """
        with RequestClient(timeout_rate=self.timeout_rate,
                           max_req_per_sec=self.max_req_per_sec,
                           max_retries=self.max_retries,
                           proxy=self.proxy) as request_client:
            page = request_client.get(
                'https://api.clarivate.com/api/wos',
                params={
                    'usrQuery': query,
                    'count': self.page_size,
                    'databaseId': self.database,
                    'optionView': 'FR',
                    'firstRecord': 1,
                },
                headers={
                    'X-ApiKey': self.api_key,
                },
            )

            n_pages = 0
            n_records = 0
            while True:
                self.logger.info(f'Fetching page {n_pages}...')
                try:
                    data = page.json()

                    # Gather info from meta-data
                    query_id = data['QueryResult']['QueryID']
                    records_searched = data['QueryResult']['RecordsSearched']
                    records_found = data['QueryResult']['RecordsFound']

                    # Gather info from header
                    next_page = page.headers.get('x-paginate-by-query-id')
                    remaining_year = page.headers.get('x-rec-amtperyear-remaining')
                    remaining_sec = page.headers.get('x-req-reqpersec-remaining')

                    records = get(data, 'Data', 'Records', 'records', 'REC', default=[])

                    if len(records) == 0:
                        break

                    yield from records

                    n_records += len(records)
                    self.logger.info(f'Found {n_records:,}/{records_found:,} records in {records_searched:,} records '
                                     f'after processing page {n_pages} for query {query_id} '
                                     f'(remaining this year = {remaining_year} | remaining / second = {remaining_sec})')

                    if next_page is None:
                        self.logger.info('No more pages available.')
                        break

                    page = request_client.get(next_page, headers={'X-ApiKey': self.api_key})

                except HTTPError as e:
                    logging.warning(f'Failed: {e}')
                    logging.warning(e.response.text)  # type: ignore[attr-defined]
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
            publication_year=get_value(lambda: wos_record.static_data.summary.pub_info.pubyear),
            source=get_source(wos_record),
            keywords=get_keywords(wos_record),
            authors=translate_authors(wos_record),
            meta={'wos-api': dump_wos_record(wos_record)}
        )


if __name__ == '__main__':

    import json
    from nacsos_data.util import get

    app = WoSAPI.test_app(
        static_files=[
            # 'scratch/academic_apis/response_scopus1.json',
            # 'scratch/academic_apis/response_scopus2.jsonl',
        ])


    @app.command()
    def offline():
        for fp in [
            'scratch/academic_apis/wos_response.json',
            'scratch/academic_apis/response2.json',
        ]:
            with open(fp, 'r') as f:
                data = json.load(f)
                records = get(data, 'Data', 'Records', 'records', 'REC', default=[])
                for record in records:
                    item = WoSAPI.translate_record(record)
                    print(item)


    app()
