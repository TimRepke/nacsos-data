import time
import signal
import base64
import logging
from types import FrameType
from urllib import parse
from json import JSONDecodeError
from typing import Literal, Generator, Any, TextIO, Callable

import httpx
from httpx import RemoteProtocolError
from pydantic import BaseModel

from nacsos_data.models.items.lexis_nexis import NewsSearchResult

logger = logging.getLogger('nacsos_data.util.LexisNexis')

ContentType = Literal[
    'News', 'LegalNews', 'CompanyAndFinancial', 'SecondaryMaterials',
    'Cases', 'CodeRegulations', 'Directories', 'Dockets', 'Forms', 'StatuteLegislations'
]

Select = Literal[
    'Jurisdiction', 'Location', 'ContentType', 'Byline', 'WordLength', 'WebNewsUrl', 'Geography', 'NegativeNews',
    'Language', 'Industry', 'People', 'Subject', 'Section', 'Company', 'PublicationType', 'Publisher', 'Document',
    'GroupDuplicates', 'SimilarDocuments', 'InternationalLocation', 'LEI', 'CompanyName', 'LNGI', 'SearchWithinResults',
    'Exclusions', 'ResultId', 'SearchType', 'Source', 'Topic', 'PracticeArea', 'Date', 'Keyword', 'PostFilters',
    'AppliedPostFilter', 'Title', 'DocumentContent', 'Overview', 'Extracts', 'IsCitationMatch', 'SourcePath'
]
Expand = Literal[
    'Document', 'SimilarDocuments', 'Source', 'PostFilters', 'AppliedPostFilter'
]

DEFAULT_SELECT: list[Select] = [
    'Jurisdiction', 'Location', 'ContentType', 'Byline', 'WordLength', 'WebNewsUrl', 'Geography', 'NegativeNews',
    'Language', 'Industry', 'People', 'Subject', 'Section', 'Company', 'PublicationType', 'Publisher', 'Document',
    'GroupDuplicates', 'InternationalLocation', 'LEI', 'CompanyName', 'LNGI', 'Exclusions', 'ResultId', 'SearchType',
    'Source', 'Topic', 'PracticeArea', 'Date', 'Keyword', 'AppliedPostFilter', 'Title', 'DocumentContent', 'Overview',
    'Extracts', 'IsCitationMatch', 'SourcePath'
]
SLEEP_TIMES = [
    # 5,  # 5s
    # 15,  # 15s
    20,  # 20s
    # 30,  # 30s
    60,  # 1min
    # 120,  # 2min
    600,  # 10min
    1800,  # 30min
    3600,  # 1h
]


class DelayedKeyboardInterrupt:

    def __init__(self) -> None:
        self.signal_received: tuple[int, FrameType | None] | None = None
        self.old_handler: Callable[[int, FrameType | None], Any | int | signal.Handlers | None] | int | None = None

    def __enter__(self) -> None:
        self.signal_received = None
        self.old_handler = signal.signal(signal.SIGINT, self.handler)

    def handler(self, sig: int, frame: FrameType | None) -> None:
        self.signal_received = (sig, frame)
        logging.warning('SIGINT received. Delaying KeyboardInterrupt.')

    def __exit__(self, type: Any, value: Any, traceback: Any) -> None:
        signal.signal(signal.SIGINT, self.old_handler)
        if self.signal_received and self.old_handler:
            self.old_handler(*self.signal_received)  # type: ignore[operator]


class Progress(BaseModel):
    info: Any | None = None
    link: str | None = None


class LexisNexis:
    """
    Adapter for the LexisNexis API.
    This class provides a context manager. It will try to recover from some known errors and wait out limitations
    of the API. As part of that, it is built to handle result writing as well. This happens in batches and the
    context manager will regularly "commit" chunks to disk. You can define how each line should be written by
    providing a string via `append_output`.
    It also keeps track of the progress, so should the process fail, you can just call the script with the same
    parameters and it should continue where it left off.

    Example usage:
    ```
    with LexisNexis(client_id='secret_client_id', client_secret='secret_client_passphrase',
                    output_file='/path/to/data.jsonl', progress_file='/path/to/progress.jsonl', max_retries=20) as ln:
        for item in ln.get_articles('climate change', filters='Date le 2023-07-31', batch_size=50,
                                    link=ln.progress.link if ln.progress else None):
            itm = item.model_dump()
            ln.append_output(json.dumps(itm, default=str) + '\n')
    ```
    """

    def __init__(self,
                 client_id: str,
                 client_secret: str,
                 progress_file: str | None = None,
                 output_file: str | None = None,
                 timeout: float = 30.,
                 logger_: logging.Logger | None = None,
                 max_retries: int = 20):
        self.max_retries = max_retries
        self.timeout = timeout

        self.logger: logging.Logger = logger_ if logger_ is not None else logger

        # Get credentials
        self._CLIENT_ID: str = client_id
        self._CLIENT_SECRET: str = client_secret

        if len(self._CLIENT_ID) < 2 or len(self._CLIENT_SECRET) < 2:
            self.logger.warning('Looks like you did not set the environment variables for the secret values!')

        self._AUTH_TOKEN: str | None = None
        self._CLIENT_TOKEN: str | None = None

        self._progress_file: str | None = progress_file
        self._progress_fp: TextIO | None = None
        self._output_file: str | None = output_file
        self._output_fp: TextIO | None = None
        self._buffer: list[str] = []

        self._progress: Progress | None = None

    @property
    def token(self) -> str:
        if self._AUTH_TOKEN is not None:
            return self._AUTH_TOKEN
        if self._CLIENT_TOKEN is not None:
            self._AUTH_TOKEN = self._CLIENT_TOKEN
            return self._AUTH_TOKEN

        self._AUTH_TOKEN = self._get_token()
        return self._AUTH_TOKEN

    @property
    def progress(self) -> Progress | None:
        return self._progress

    @property
    def unfinished(self) -> bool:
        return self._progress is not None and self._progress.link is not None and len(self._progress.link.strip()) > 5

    def set_progress_extra(self, info: Any) -> None:
        if self._progress is None:
            self._progress = Progress(info=info)
        else:
            self._progress.info = info

    def _set_progress_from_file(self) -> None:
        latest = None
        if self._progress_file:
            with open(self._progress_file, 'r') as f:
                for line in f:
                    if len(line.strip()) > 10:
                        latest = Progress.model_validate_json(line)
            self.logger.info(f'Last progress is: {latest}')
            if latest is not None:
                self._progress = latest

    def __enter__(self) -> 'LexisNexis':
        self.logger.info('Entering LexisNexis context manager...')
        if self._output_file is not None:
            self.logger.debug(f'Opening output file at {self._output_file}')
            self._output_fp = open(self._output_file, 'a')
        if self._progress_file is not None:
            self.logger.debug(f'Opening progress file at {self._progress_file}')
            self._set_progress_from_file()
            self._progress_fp = open(self._progress_file, 'a')
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, exc_traceback: Any) -> None:
        self.logger.info('Leaving LexisNexis context manager!')
        if self._output_fp is not None:
            self._output_fp.close()
        if self._progress_fp is not None:
            self._progress_fp.close()

    def _commit(self, link: str | None) -> None:
        self.logger.debug(f'Received a commit prompt, attempting to save the '
                          f'buffer ({len(self._buffer):,} lines) and record progress.')
        with DelayedKeyboardInterrupt():
            if self._progress is not None and self._progress_fp is not None:
                self._progress.link = link
                self._progress_fp.write(self._progress.model_dump_json() + '\n')
            if self._output_fp:
                for line in self._buffer:
                    self._output_fp.write(line)
                self._buffer = []

    def _get_token(self) -> str:
        # base64-encode the credentials
        auth_string = base64.b64encode(f'{self._CLIENT_ID}:{self._CLIENT_SECRET}'.encode('utf-8')).decode('utf-8')

        # Fetch an auth token bearer
        self.logger.debug('Getting bearer token')
        response = httpx.post(
            'https://auth-api.lexisnexis.com/oauth/v2/token',
            data={
                'grant_type': 'client_credentials',
                'scope': 'http://oauth.lexisnexis.com/all'
            },
            timeout=self.timeout,
            headers={'Authorization': f'Basic {auth_string}'}).json()
        auth_token: str = response['access_token']
        self.logger.debug(f'AuthToken: {auth_token}')
        return auth_token

    def _request(self, link: str, test_count: bool = False) -> dict[str, Any]:
        self.logger.debug(f'Calling: {link}')
        for retry in range(self.max_retries):
            try:
                request = httpx.get(link, timeout=self.timeout, headers={'Authorization': f'Bearer {self.token}'})

                if request.status_code == 401:  # HTTP UNAUTHORIZED (Probably "Invalid access token")
                    self._AUTH_TOKEN = None
                    self._CLIENT_TOKEN = None
                    self.logger.warning('Auth token was old, trying to reset!')
                    return self._request(link=link, test_count=test_count)

                response: dict[str, Any] = request.json()

                if test_count:
                    cnt = response.get('@odata.count', -1)
                    if cnt < 0:
                        raise AssertionError('Missing count information!')

                return response
            except (httpx.TimeoutException, AssertionError, RemoteProtocolError) as e:
                sleep = SLEEP_TIMES[min(len(SLEEP_TIMES) - 1, retry)]
                self.logger.debug(f'Encountered error "{e}", sleeping for {sleep}s...')
                try:
                    self.logger.warning(request.text)
                except Exception:
                    pass
                time.sleep(sleep)
            except JSONDecodeError as e:
                self.logger.exception(e)
                if 'request' in locals() and request is not None:
                    self.logger.error(f'HTTP {request.status_code}: {request.text}')
                raise e
        raise RuntimeError('Reached max_retries but did not finish current request!')

    def get_count(self, search: str,
                  content_type: ContentType = 'News') -> int | None:
        link = (f'https://services-api.lexisnexis.com/v1/{content_type}'
                f'?$search={parse.quote_plus(search)}'
                f'&$top=5')
        response = self._request(link, test_count=True)
        return response.get('@odata.count')

    def get_results(self,
                    search: str,
                    max_batches: int | None = None,
                    batch_size: int = 50,
                    content_type: ContentType = 'News',
                    select: list[Select] | None = None,
                    expand: list[Expand] | None = None,
                    filters: str | None = None,
                    link: str | None = None) -> Generator[dict[str, Any], None, None]:
        if link is None:
            if batch_size > 50:
                raise ValueError('Max batch size is 50')
            if select is None:
                select = DEFAULT_SELECT
            if expand is None:
                expand = ['Document', 'Source']

            link = (f'https://services-api.lexisnexis.com/v1/{content_type}'
                    f'?$search={parse.quote_plus(search)}'
                    f'&$top={batch_size}'
                    f'&$expand={",".join(expand)}'
                    f'&$select={",".join(select)}'
                    f'&$orderby=Date asc')
            if filters:
                link += f'&$filter={filters}'

        batch = 0
        while True:
            response = self._request(link, test_count=True)
            batch += 1
            cnt = response['@odata.count']
            self.logger.info(f'(Batch {batch:,}) | Found {cnt:,} documents that match the query: {search}')

            yield from response.get('value', [])
            self._commit(link)

            link = response.get('@odata.nextLink')
            self.logger.debug(f'nextLink: {link}')

            if not link:
                self.logger.info(f'Received no `nextLink` after {batch:,} batches, stopping here.')
                break

            if max_batches is not None and max_batches >= batch:
                self.logger.info(f'Reached maximum number of batches ({max_batches:,}), stopping here.')
                break
        self._commit(None)

    def get_articles(self,
                     search: str,
                     max_batches: int | None = None,
                     batch_size: int = 50,
                     content_type: ContentType = 'News',
                     select: list[Select] | None = None,
                     expand: list[Expand] | None = None,
                     filters: str | None = None,
                     link: str | None = None) -> Generator[NewsSearchResult, None, None]:
        for item in self.get_results(search=search,
                                     max_batches=max_batches,
                                     batch_size=batch_size,
                                     content_type=content_type,
                                     select=select,
                                     expand=expand,
                                     filters=filters,
                                     link=link):
            try:
                yield NewsSearchResult.model_validate(item)
            except Exception as e:
                self.logger.exception(e)
                self.logger.warning(item)
                raise e

    def append_output(self, line: str) -> None:
        self._buffer.append(line)
