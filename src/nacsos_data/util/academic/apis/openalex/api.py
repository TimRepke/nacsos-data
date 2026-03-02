import uuid
import logging
from typing import Any, Generator, Type

import httpx
from httpx import HTTPStatusError

from nacsos_data.models.items import AcademicItemModel
from nacsos_data.models.openalex import WorksSchema
from nacsos_data.util.academic.apis.util import RequestClient, AbstractAPI
from .shared import FIELDS_API, translate_work_to_item


# TODO: https://developers.openalex.org/api-reference/errors#retry-logic
# import time
# import requests
#
# def fetch_with_retry(url, max_retries=5):
#     for attempt in range(max_retries):
#         try:
#             response = requests.get(url, timeout=30)
#
#             if response.status_code == 200:
#                 return response.json()
#
#             if response.status_code == 429:
#                 # Rate limited - wait longer
#                 wait_time = 2 ** attempt
#                 time.sleep(wait_time)
#                 continue
#
#             if response.status_code >= 500:
#                 # Server error - retry
#                 wait_time = 2 ** attempt
#                 time.sleep(wait_time)
#                 continue
#
#             # Client error - don't retry
#             response.raise_for_status()
#
#         except requests.exceptions.Timeout:
#             if attempt < max_retries - 1:
#                 time.sleep(2 ** attempt)
#             else:
#                 raise
#
#     raise Exception(f"Failed after {max_retries} retries")
class OpenAlexAPI(AbstractAPI):
    PAGE_MAX = 50

    def __init__(
        self,
        split_larger: int | None = None,
        api_key: str | None = None,
        proxy: str | None = None,
        max_req_per_sec: int = 5,
        max_retries: int = 5,
        backoff_rate: float = 5.0,
        ignored_exceptions: list[Type[Exception]] | None = None,
        logger: logging.Logger | None = None,
    ):
        super().__init__(
            proxy=proxy,
            max_retries=max_retries,
            max_req_per_sec=max_req_per_sec,
            backoff_rate=backoff_rate,
            logger=logger,
            ignored_exceptions=ignored_exceptions,
            api_key=api_key,
        )
        self.split_larger = split_larger

    def fetch_raw(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        # If there's no "large result" split threshold, proceed as normal
        if self.split_larger is None:
            yield from self._fetch_raw(query, params)

        # We'd like to split response sets that are too large
        else:
            # Get the size of the expected result set
            count = self.get_count(query, params) or 0

            # Seems too big for one chunk, proceed to sub-chunking
            if count > self.split_larger:
                # keep track of original logger
                logger = self.logger
                for name, subfilter in [
                    ('(1/4) oa=false|typ=art', 'type:!article,open_access.is_oa:false'),
                    ('(2/4) oa=true|typ=art', 'type:!article,open_access.is_oa:true'),
                    ('(3/4) oa=false|typ=!art', 'type:article,open_access.is_oa:false'),
                    ('(4/4) oa=true|typ=!art', 'type:article,open_access.is_oa:true'),
                ]:
                    # get child logger for neat log separation
                    self.logger = logger.getChild(name)

                    # run the query with the sub-filter
                    yield from self._fetch_raw(
                        query,
                        (params or {}) | {'filter': f'{params["filter"]},{subfilter}' if params and params.get('filter') else subfilter},
                    )

                # reset to original logger
                self.logger = logger

            # Size is reasonable, do it in one go
            else:
                yield from self._fetch_raw(query, params)

    def _fetch_raw(
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

                    page = req.json()
                    cursor = page['meta']['next_cursor']
                    self.logger.info(f'Retrieved {n_works:,} / {page["meta"]["count"]:,} | currently on page {n_pages:,}')

                    yield from page['results']
                    n_works += len(page['results'])

                except HTTPStatusError as e:
                    self.logger.error(e.response.text)
                    raise e
                except Exception as e:
                    for exc_type in self.ignored_exceptions:
                        if type(e) is exc_type:
                            self.logger.error(e)
                            self.logger.warning(f'Ignoring error {e}')
                            break
                    else:
                        raise e

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

        return page.json().get('meta', {}).get('count', None)  # type: ignore [no-any-return]
