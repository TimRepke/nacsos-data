from typing_extensions import TypedDict

from pydantic import BaseModel


class SerializedArtefact(TypedDict):
    """
    The SerializedArtefact is the interface definition on how references to artefacts
    are communicated. It is essentially just a proxy for `Artefact`.
    """
    serializer: str
    dtype: str
    filename: str | None
    filenames: str | list[str] | None

    # FIXME mark optional keys as NotRequired once we are on Python 3.11
    #       https://docs.python.org/3.11/library/typing.html#typing.NotRequired


class SerializedArtefactReference(TypedDict):
    task_id: str
    artefact: str


class SerializedUserArtefact(TypedDict):
    user_serializer: str
    user_dtype: str
    filename: str | None
    filenames: list[str] | None


class KWARG(BaseModel):
    # list of allowed types (usually just one entry, otherwise for unions)
    dtype: list[str]
    # whether this is an optional parameter or not (then None)
    optional: bool | None = None
    # default value (None if no default is given)
    default: int | float | bool | str | None = None
    # only used if KWARG has dtype Artefact
    artefact: SerializedArtefact | None = None
    # only used if dtype is more complex and has sub-objects
    # dict of key = param name, value = datatype or tuple(datatype, default value)
    params: dict[str, 'KWARG'] | None = None
    # used for `Literal`
    options: list[str] | None = None
    # used for generics
    generics: list[str] | None = None
