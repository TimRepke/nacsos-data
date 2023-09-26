from typing import Sequence, Generator, TypeVar

T = TypeVar('T')


def batched(lst: Sequence[T] | Generator[T, None, None], batch_size: int) -> Generator[list[T], None, None]:
    batch = []
    for li in lst:
        batch.append(li)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    yield batch
