import logging
import uuid
from collections.abc import MutableMapping
from contextlib import contextmanager
from datetime import timedelta
from timeit import default_timer
from typing import Sequence, Generator, TypeVar, Any, AsyncIterator, AsyncGenerator, Callable, TYPE_CHECKING, Optional

if TYPE_CHECKING:
    import pandas as pd

T = TypeVar('T')
D = TypeVar('D')


async def batched_async(lst: AsyncIterator[T] | AsyncGenerator[T, None], batch_size: int) \
        -> AsyncGenerator[list[T], None]:
    batch = []
    async for li in lst:
        batch.append(li)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    yield batch


def batched(lst: Sequence[T] | Generator[T, None, None], batch_size: int) -> Generator[list[T], None, None]:
    batch = []
    for li in lst:
        batch.append(li)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    yield batch


async def gather_async(lst: AsyncIterator[T] | AsyncGenerator[T, None]) -> list[T]:
    return [li async for li in lst]


def clear_empty(obj: Any | None) -> Any | None:
    """
    Recursively checks the object for empty-like things and explicitly sets them to None (or drops keys)

    :param obj:
    :return:
    """
    if obj is None:
        return None

    if isinstance(obj, str):
        if len(obj) == 0:
            return None
        return obj

    if isinstance(obj, list):
        tmp_l = [clear_empty(li) for li in obj]
        tmp_l = [li for li in tmp_l if li is not None]
        if len(tmp_l) > 0:
            return tmp_l
        return None

    if isinstance(obj, dict):
        tmp_d = {key: clear_empty(val) for key, val in obj.items()}
        tmp_d = {key: val for key, val in tmp_d.items() if val is not None}
        if len(tmp_d) > 0:
            return tmp_d
        return None

    return obj


# from https://stackoverflow.com/a/24088493
def fuze_dicts(d1: dict[str, Any] | None, d2: dict[str, Any] | None) -> dict[str, Any] | None:
    if d1 is None:
        return d2
    if d2 is None:
        return d1

    for k, v in d1.items():
        if k in d2:
            # this next check is the only difference!
            if all(isinstance(e, MutableMapping) for e in (v, d2[k])):
                d2[k] = fuze_dicts(v, d2[k])
            # we could further check types and merge as appropriate here.
    d3 = d1.copy()
    d3.update(d2)
    return d3


def ensure_values(o: Any, *attrs: str | tuple[str, Any]) -> tuple[Any, ...]:
    ret = []
    attr: str
    default: Any | None
    for attr_ in attrs:
        if type(attr_) is str:
            attr, default = attr_, None
        elif type(attr_) is tuple:
            attr, default = attr_
        else:
            raise TypeError()

        if type(o) is dict:
            v = o.get(attr, None)
        else:
            v = getattr(o, attr)
        if v is None:
            if default is None:
                raise KeyError(f'Attribute "{attr}" is missing or empty and has no default!')
            v = default
        ret.append(v)
    return tuple(ret)


def ensure_logger_async(fallback_logger: logging.Logger):  # type: ignore[no-untyped-def]
    def decorator(func):  # type: ignore[no-untyped-def]
        async def wrapper(*args,  # type: ignore[no-untyped-def]
                          log: logging.Logger | None = None,
                          **kwargs):
            if log is None:
                log = fallback_logger
            return await func(*args, log=log, **kwargs)

        return wrapper

    return decorator


def ensure_logger(fallback_logger: logging.Logger):  # type: ignore[no-untyped-def]
    def decorator(func):  # type: ignore[no-untyped-def]
        def wrapper(*args,  # type: ignore[no-untyped-def]
                    log: logging.Logger | None = None,
                    **kwargs):
            if log is None:
                log = fallback_logger
            return func(*args, log=log, **kwargs)

        return wrapper

    return decorator


@contextmanager
def elapsed_timer(logger: logging.Logger, tn: str = 'Task') -> Generator[Callable[[], float], None, None]:
    # https://stackoverflow.com/questions/7370801/how-do-i-measure-elapsed-time-in-python
    logger.info(f'{tn}...')
    start = default_timer()
    elapser = lambda: default_timer() - start
    yield lambda: elapser()
    end = default_timer()
    elapser = lambda: end - start
    logger.debug(f'{tn} took {timedelta(seconds=end - start)} to execute.')


def get_attr(obj: Any, key: str, default: T | None = None) -> T | None:
    if hasattr(obj, key):
        val = getattr(obj, key)
        if val is None:
            return default
        return val  # type: ignore[no-any-return]
    return default


def oring(arr: list[Optional['pd.Series[bool]']]) -> 'bool | pd.Series[bool]':
    fixed_arr: list['pd.Series[bool]'] | None = clear_empty(arr)
    if fixed_arr is None:
        return False

    ret = fixed_arr[0]
    for a in fixed_arr[1:]:
        ret |= a
    return ret


def anding(arr: list[Optional['pd.Series[bool]']]) -> 'bool | pd.Series[bool]':
    fixed_arr: list['pd.Series[bool]'] | None = clear_empty(arr)
    if fixed_arr is None:
        return False

    ret = fixed_arr[0]
    for a in fixed_arr[1:]:
        ret &= a
    return ret


def get(obj: dict[str, Any], *keys: str, default: Any = None) -> Any:  # type: ignore[var-annotated]
    for key in keys:
        obj = obj.get(key)  # type: ignore[assignment]
        if obj is None:
            return default  # type: ignore[unreachable]
    return obj


def get_value(val: Callable[[], T], default: D | None = None) -> T | D:
    try:
        ret = val()
        if ret is None:
            return default
        return ret
    except (KeyError, AttributeError):
        return default


def as_uuid(val: str | uuid.UUID | None = None) -> uuid.UUID | None:
    if val is None:
        return None
    if type(val) == str:
        return uuid.UUID(val)
    return val  # type: ignore[return-value]
