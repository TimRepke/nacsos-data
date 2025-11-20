import uuid
from typing import Any, Generator

from httpx import codes

from nacsos_data.models.items import AcademicItemModel
from nacsos_data.models.items.academic import AcademicAuthorModel, AffiliationModel
from nacsos_data.util import get, clear_empty, as_uuid
from nacsos_data.util.academic.apis.util import RequestClient, AbstractAPI, response_logger


def get_title(obj: dict[str, Any]) -> str | None:
    return obj.get('dc:title')


def get_abstract(obj: dict[str, Any]) -> str | None:
    return obj.get('dc:description')


def get_doi(obj: dict[str, Any]) -> str | None:
    return obj.get('prism:doi')


def get_id(obj: dict[str, Any]) -> str | None:
    return obj.get('eid', obj.get('dc:identifier'))


def get_py(obj: dict[str, Any]) -> int | None:
    py = obj.get('prism:coverDate')
    if py and len(py) >= 4:
        return int(py[:4])
    return None


def get_keywords(obj: dict[str, Any]) -> list[str] | None:
    kw = obj.get('authkeywords', '').split(' | ')
    return clear_empty(kw)


def translate_authors(record: dict[str, Any]) -> list[AcademicAuthorModel] | None:
    affiliations = {
        aff['afid']: AffiliationModel(s2_id=aff['afid'], name=aff.get('affilname'), country=aff.get('affiliation-country'))
        for aff in record.get('affiliation', [])
    }

    authors = record.get('author')
    if not authors:
        return None
    return [
        AcademicAuthorModel(
            s2_id=author.get('authid'),
            name=author.get('authname'),
            affiliations=clear_empty([affiliations.get(aff['$']) for aff in author.get('afid', [])]),
        )
        for author in authors
    ]


class ScopusAPI(AbstractAPI):

    def fetch_raw(
            self,
            query: str,
            params: dict[str, Any] | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        """
        Scopus API wrapper for downloading all records for a given query.

        API overview
        https://dev.elsevier.com/

        API Documentation:
        https://dev.elsevier.com/documentation/ScopusSearchAPI.wadl

        :param query:
        :return:
        """
        with RequestClient(backoff_rate=self.backoff_rate,
                           max_req_per_sec=self.max_req_per_sec,
                           max_retries=self.max_retries,
                           proxy=self.proxy) as request_client:

            request_client.on(status=codes.UNAUTHORIZED, func=response_logger(self.logger))

            next_cursor = '*'
            n_pages = 0
            n_records = 0
            while True:
                self.logger.info(f'Fetching page {n_pages}...')

                page = request_client.get(
                    'https://api.elsevier.com/content/search/scopus',
                    params={
                        'query': query,
                        'cursor': next_cursor,
                        # https://dev.elsevier.com/sc_search_views.html
                        'view': 'COMPLETE',
                    },
                    headers={
                        'Accept': 'application/json',
                        "X-ELS-APIKey": self.api_key,
                    },
                )

                scopus_requests_limit = page.headers.get('x-ratelimit-limit')
                scopus_requests_remaining = page.headers.get('x-ratelimit-remaining')
                scopus_requests_reset = page.headers.get('x-ratelimit-reset')

                n_pages += 1
                data = page.json()

                next_cursor = get(data, 'search-results', 'cursor', '@next', default=None)
                entries = get(data, 'search-results', 'entry', default=[])
                n_results = get(data, 'search-results', 'opensearch:totalResults', default=0)

                if len(entries) == 0 or n_results == 0:
                    break
                if len(entries) == 1 and entries[0].get('error') is not None:
                    break

                yield from entries

                n_records += len(entries)
                self.logger.debug(f'Found {n_records}/{n_results} records after processing page {n_pages} '
                                  f'(rate limit = {scopus_requests_limit} '
                                  f'| remaining = {scopus_requests_remaining} '
                                  f'| reset = {scopus_requests_reset})')

    @classmethod
    def translate_record(cls, record: dict[str, Any], project_id: str | uuid.UUID | None = None) -> AcademicItemModel:
        return AcademicItemModel(
            item_id=uuid.uuid4(),
            project_id=as_uuid(project_id),
            doi=get_doi(record),
            title=get_title(record),
            scopus_id=get_id(record),
            text=get_abstract(record),
            # title_slug  # not required
            publication_year=get_py(record),
            source=record.get('prism:publicationName'),
            keywords=get_keywords(record),
            authors=translate_authors(record),
            meta={'scopus-api': clear_empty(record)}
        )


if __name__ == '__main__':
    app = ScopusAPI.test_app(
        static_files=[
            # 'scratch/academic_apis/response_scopus1.json',
            'scratch/academic_apis/response_scopus2.jsonl',
        ],
        proxy='socks5://127.0.0.1:1080')
    app()
