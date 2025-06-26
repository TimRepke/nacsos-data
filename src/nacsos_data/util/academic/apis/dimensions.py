import logging
import uuid
from typing import Any, Generator

from httpx import codes, HTTPError, Response
from nacsos_data.models.items import AcademicItemModel
from nacsos_data.util import get, as_uuid
from nacsos_data.util.academic.apis.util import RequestClient, AbstractAPI


def get_title(obj: dict[str, Any]) -> str | None:
    return obj.get('title')


def get_abstract(obj: dict[str, Any]) -> str | None:
    return obj.get('abstract')


def get_doi(obj: dict[str, Any]) -> str | None:
    return obj.get('doi')


def get_id(obj: dict[str, Any]) -> str | None:
    return obj.get('id')


class DimensionsAPI(AbstractAPI):

    def __init__(self,
                 api_key: str,
                 page_size: int = 5,
                 proxy: str | None = None,
                 max_req_per_sec: int = 5,
                 max_retries: int = 5,
                 backoff_rate: float = 5.,
                 logger: logging.Logger | None = None):
        super().__init__(api_key=api_key, proxy=proxy, max_retries=max_retries,
                         max_req_per_sec=max_req_per_sec, backoff_rate=backoff_rate, logger=logger)
        self.page_size = page_size

    def fetch_raw(self, query: str) -> Generator[dict[str, Any], None, None]:
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
        :return:
        """
        with RequestClient(backoff_rate=self.backoff_rate,
                           max_req_per_sec=self.max_req_per_sec,
                           max_retries=self.max_retries,
                           proxy=self.proxy) as request_client:
            jwt = 'empty'
            logger = self.logger
            api_key = self.api_key

            def update_jwt(response: Response) -> dict[str, dict[str, str]]:
                logger.debug('Fetching JWT token')
                res = request_client.post('https://app.dimensions.ai/api/auth.json', json={'key': api_key})
                res.raise_for_status()
                jwt = res.json()['token']
                return {'headers': {'Authorization': f'JWT {jwt}'}}

            request_client.on(codes.UNAUTHORIZED, update_jwt)

            n_pages = 0
            n_records = 0
            while True:
                logger.info(f'Fetching page {n_pages}...')
                try:
                    page = request_client.post(
                        url='https://app.dimensions.ai/api/dsl/v2',
                        content=f'{query} limit {self.page_size} skip {n_pages * self.page_size}',
                        headers={
                            'Accept': 'application/json',
                            'Authorization': f'JWT {jwt}',
                        },
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
                    logging.exception(e)
                    raise e

    @classmethod
    def translate_record(cls, record: dict[str, Any], project_id: str | uuid.UUID | None = None) -> AcademicItemModel:
        return AcademicItemModel(
            item_id=uuid.uuid4(),
            project_id=as_uuid(project_id),
            doi=get_doi(record),
            title=get_title(record),
            dimensions_id=get_id(record),
            text=get_abstract(record),
            # TODO
        )


if __name__ == '__main__':
    app = DimensionsAPI.test_app(
        static_files=[
            # 'scratch/academic_apis/response_scopus1.json',
            # 'scratch/academic_apis/response_scopus2.jsonl',
        ])
    app()
