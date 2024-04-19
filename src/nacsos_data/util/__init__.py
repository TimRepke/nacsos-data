from typing import Sequence, Generator, TypeVar, Any, AsyncIterator, AsyncGenerator

T = TypeVar('T')


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
