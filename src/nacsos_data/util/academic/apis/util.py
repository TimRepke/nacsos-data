import json
import logging
import uuid
from abc import ABC, abstractmethod
from typing import Any, Callable, Iterable, Generator, Annotated
from pathlib import Path
from time import perf_counter, sleep
from typing_extensions import override
from httpx import Client, URL, USE_CLIENT_DEFAULT, Response, codes, HTTPError
from httpx._client import UseClientDefault
from httpx._types import (
    RequestContent,
    RequestData,
    RequestFiles,
    QueryParamTypes,
    HeaderTypes,
    CookieTypes,
    AuthTypes,
    TimeoutTypes,
    RequestExtensions,
)

from nacsos_data.models.items import AcademicItemModel


def response_logger(logger: logging.Logger) -> Callable[[Response], dict[str, Any]]:
    def inner(response: Response) -> dict[str, Any]:
        # nonlocal logger
        logger.warning(response.text)
        return {}

    return inner


class RequestClient(Client):
    def __init__(self,  # type: ignore[no-untyped-def]
                 *,
                 max_req_per_sec: int = 5,
                 max_retries: int = 5,
                 backoff_rate: float = 120.,
                 retry_on_status: list[int] | None = None,
                 **kwargs) -> None:
        super().__init__(**kwargs)

        self.max_req_per_sec = max_req_per_sec
        self.time_per_request = 1 / max_req_per_sec
        self.max_retries = max_retries
        self.backoff_rate = backoff_rate
        self.last_request: float | None = None
        self.retry_on_status = retry_on_status or [
            codes.INTERNAL_SERVER_ERROR,  # 500
            codes.BAD_GATEWAY,  # 502
            codes.SERVICE_UNAVAILABLE,  # 503
            codes.GATEWAY_TIMEOUT,  # 504
        ]
        self.kwargs = kwargs
        self.callbacks: dict[int, Callable[..., Any]] = {}

    def on(self, status: int, func: Callable[[Response], dict[str, Any]]) -> None:
        self.callbacks[status] = func

    @override
    def request(
            self,
            method: str,
            url: URL | str,
            *,
            content: RequestContent | None = None,
            data: RequestData | None = None,
            files: RequestFiles | None = None,
            json: Any | None = None,
            params: QueryParamTypes | None = None,
            headers: HeaderTypes | None = None,
            cookies: CookieTypes | None = None,
            auth: AuthTypes | UseClientDefault | None = None,
            follow_redirects: bool | UseClientDefault = True,
            timeout: TimeoutTypes | UseClientDefault = 120,
            extensions: RequestExtensions | None = None,
    ) -> Response:
        for retry in range(self.max_retries):
            # Check if we need to wait before the next request so we are staying below the rate limit
            time = perf_counter() - (self.last_request or 0)
            if time < self.time_per_request:
                logging.debug(f'Sleeping to keep rate limit: {self.time_per_request - time:.4f} seconds')
                sleep(self.time_per_request - time)

            if auth == USE_CLIENT_DEFAULT:
                auth = None
            if follow_redirects == USE_CLIENT_DEFAULT:
                follow_redirects = None
            if timeout == USE_CLIENT_DEFAULT:
                timeout = None

            # Log latest request
            self.last_request = perf_counter()
            response = super().request(
                method=method or self.kwargs.get('method'),
                url=url or self.kwargs.get('url'),
                content=content or self.kwargs.get('content'),
                data=data or self.kwargs.get('data'),
                files=files or self.kwargs.get('files'),
                json=json or self.kwargs.get('json'),
                params=params or self.kwargs.get('params'),
                headers=self.kwargs.get('headers', {}) | (headers or {}),
                cookies=self.kwargs.get('cookies', {}) | (cookies or {}),
                auth=auth or self.kwargs.get('auth', USE_CLIENT_DEFAULT),
                follow_redirects=follow_redirects or self.kwargs.get('follow_redirects', True),
                timeout=timeout or self.kwargs.get('timeout', 120),
                extensions=extensions or self.kwargs.get('extensions'),
            )

            try:
                response.raise_for_status()

                # reset counters after successful request
                self.time_per_request = 1 / self.max_req_per_sec

                return response

            except HTTPError as e:
                if e.response.status_code in self.callbacks:  # type: ignore[attr-defined]
                    logging.debug(f'Found status handler for {e.response.status_code}')  # type: ignore[attr-defined]
                    update = self.callbacks[e.response.status_code](e.response)  # type: ignore[attr-defined]
                    if update and update.get('content'):
                        content = update.get('content')
                    if update and update.get('data'):
                        data = update.get('data')
                    if update and update.get('json'):
                        if not json:
                            json = update.get('json', None)
                        else:
                            json.update(update.get('json', {}))
                    if update and update.get('params'):
                        if not params:
                            params = update.get('params', None)
                        else:
                            params.update(update.get('params', {}))  # type: ignore[union-attr]
                    if update and update.get('headers'):
                        if not headers:
                            headers = update.get('headers', None)
                        else:
                            headers.update(update.get('headers', {}))  # type: ignore[union-attr]

                # if this error is not on the list, pass on error right away; otherwise log and retry
                elif e.response.status_code not in self.retry_on_status and len(self.retry_on_status) > 0:  # type: ignore[attr-defined]
                    logging.warning(e.response.text)  # type: ignore[attr-defined]
                    raise e

                else:
                    logging.warning(f'Retry {retry} after failing to retrieve from {url}: {e}')
                    logging.warning(e.response.text)  # type: ignore[attr-defined]
                    logging.exception(e)

                    # grow the sleep time between requests
                    self.time_per_request = (self.time_per_request + 1) * self.backoff_rate
        else:
            raise RuntimeError('Maximum number of retries reached')


class AbstractAPI(ABC):
    def __init__(self,
                 api_key: str,
                 proxy: str | None = None,
                 max_req_per_sec: int = 5,
                 max_retries: int = 5,
                 backoff_rate: float = 5.,
                 logger: logging.Logger | None = None):
        self.api_key = api_key
        self.proxy = proxy
        self.logger = logger
        self.max_req_per_sec = max_req_per_sec
        self.max_retries = max_retries
        self.backoff_rate = backoff_rate

        if self.logger is None:
            self.logger = logging.getLogger(type(self).__name__)

    @abstractmethod
    def fetch_raw(self, query: str) -> Generator[dict[str, Any], None, None]:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def translate_record(cls, record: dict[str, Any], project_id: str | uuid.UUID | None = None) -> AcademicItemModel:
        raise NotImplementedError

    def fetch_translated(self, query: str, project_id: str | uuid.UUID | None = None) -> Generator[AcademicItemModel, None, None]:
        for record in self.fetch_raw(query):
            yield self.translate_record(record, project_id)

    def download_raw(self, query: str, target: Path) -> None:
        collect_raw_jsonl(target=target, items=self.fetch_raw(query))

    def download_translated(self, query: str, target: Path, project_id: str | uuid.UUID | None = None) -> None:
        collect_jsonl(target=target, items=self.fetch_translated(query=query, project_id=project_id))

    @classmethod
    def read_translated(cls, source: Path, project_id: str | uuid.UUID | None = None) -> Generator[AcademicItemModel, None, None]:
        with open(source, 'r') as f_in:
            for line in f_in:
                record = json.loads(line)
                yield cls.translate_record(record=record, project_id=project_id)

    @classmethod
    def convert_file(cls, source: Path, target: Path, project_id: str | uuid.UUID | None = None):
        with open(target, 'w') as f_out:
            for item in cls.read_translated(source=source, project_id=project_id):
                f_out.write(item.model_dump_json(exclude_defaults=True) + '\n')

    @classmethod
    def test_app(cls,
                 static_files: list[str],
                 logger: logging.Logger | None = None):
        import typer

        logging.basicConfig(format='%(asctime)s [%(levelname)s] %(name)s (%(process)d): %(message)s', level='DEBUG')

        app = typer.Typer()

        @app.command()
        def download(
                api_key: Annotated[str, typer.Option(help='Valid API key')],
                target: Annotated[Path, typer.Option(help='File to write results to')],
                query_file: Annotated[Path | None, typer.Option(help='File containing search query')] = None,
                query: Annotated[str | None, typer.Option(help='Search query')] = None,
                proxy: Annotated[str | None, typer.Option(help='Proxy to use, e.g. "socks5://127.0.0.1:1080')] = None,
        ) -> None:

            if query_file:
                with open(query_file, 'r') as qf:
                    query_str = qf.read()
            elif query:
                query_str = query
            else:
                raise AttributeError('Must provide either `query_file` or `query`')

            instance = cls(api_key=api_key, proxy=proxy, logger=logger)
            instance.download_raw(query=query_str, target=target)

        @app.command()
        def convert(
                source: Annotated[Path, typer.Option(help='File to read results from')],
                target: Annotated[Path, typer.Option(help='File to write results to')],
        ):
            cls.convert_file(source=source, target=target)

        @app.command()
        def translate():
            for fp in static_files or []:
                for item in cls.read_translated(Path(fp)):
                    print(item)

        return app


def _assert_target(target: Path):
    if target.exists():
        raise FileExistsError(f'File {target} already exists')
    target.parent.mkdir(parents=True, exist_ok=True)


def collect_raw_jsonl(target: Path, items: Iterable[dict[str, Any]]) -> None:
    _assert_target(target)
    with open(target, 'w', encoding='utf-8') as f:
        for item in items:
            f.write(json.dumps(item) + '\n')


def collect_jsonl(target: Path, items: Iterable[AcademicItemModel]) -> None:
    _assert_target(target)
    with open(target, 'w', encoding='utf-8') as f:
        for item in items:
            f.write(item.model_dump_json(exclude_defaults=True) + '\n')
