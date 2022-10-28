from typing import NamedTuple

from pydantic import BaseModel
from sqlalchemy import text
from nacsos_data.db.connection import DatabaseEngineAsync
from nacsos_data.models.annotations import AnnotationModel
from uuid import UUID


# ideas for resolving algorithms:
#   - naive majority vote (per key,repeat)
#   - majority vote (including secondary class if available)
#   - weighted vote (with manually assigned "trust" weights per annotator)
#   - weighted vote (compute annotator trust/reliability)
#   - ...

class InvalidFilterError(AssertionError):
    pass


class AnnotationFilters(BaseModel):
    """
    Filter rules for fetching all annotations that match these conditions
    It is up to the user of this function to make sure to provide sensible filters!
    All filters are conjunctive (connected with "AND"); if None, they are not included
    :param scheme_id: if not None: annotation has to be part of this annotation scheme
    :param scope_id: if not None: annotation has to be part of this assignment scope
    :param user_id: if not None: annotation has to be by this user
    :param key: if not None: annotation has to be for this AnnotationSchemeLabel.key (or list/tuple of keys)
    :param exclude_key: if not None: annotation must not be for this AnnotationSchemeLabel.key
    :param repeat: if not None: annotation has to be primary/secondary/...
    """
    scheme_id: UUID | str | None = None
    scope_id: UUID | str | None = None
    user_id: UUID | str | None = None
    key: str | tuple[str] | None = None
    repeat: int | None = None
    exclude_key: str | tuple[str] | None = None


def _get_filter_subquery(filters: AnnotationFilters):
    join = ''
    where = []
    if filters.scope_id is not None:
        join += 'JOIN assignment ass on ass.assignment_id = a.assignment_id '
        where.append('ass.assignment_scope_id = :scope_id ')
    if filters.scheme_id is not None:
        where.append('a.annotation_scheme_id = :scheme_id')
    if filters.user_id is not None:
        where.append('a.user_id = :user_id')
    if filters.key is not None:
        if type(filters.key) == tuple:
            where.append('a.key IN :key')
        else:
            where.append('a.key = :key')
    if filters.exclude_key is not None:
        if type(filters.exclude_key) == tuple:
            where.append('a.key IN :exclude_key')
        else:
            where.append('a.key = :exclude_key')
    if filters.repeat is not None:
        where.append('a.repeat = :repeat')
    if len(where) == 0:
        raise InvalidFilterError('You did not specify any valid filter.')

    return f'{join} WHERE {" AND ".join(where)}'


async def get_item_annotations(engine: DatabaseEngineAsync, filters: AnnotationFilters) -> dict[
    str, list[AnnotationModel]]:
    """
    :param engine: Connection to the database
    :param filters:
    :return: dictionary (keys are item_ids) of all annotations per item that match the filters.
    """
    async with engine.session() as session:
        annotations = (await session.execute(text(
            "SELECT a.item_id, json_agg(a.*) as annotations "
            "FROM annotation a "
            f" {_get_filter_subquery(filters)} "
            "GROUP BY a.item_id;"
        ), filters.dict())).mappings().all()

        return {
            row['item_id']: [AnnotationModel.parse_obj(anno) for anno in row['annotations']]
            for row in annotations
        }


async def get_annotator_ids(engine: DatabaseEngineAsync, filters: AnnotationFilters) -> list[str]:
    # list of all (unique) user_ids that have at least one annotation in the set
    async with engine.session() as session:
        return (await session.execute(text(
            "SELECT a.user_id "
            "FROM annotation a "
            f" {_get_filter_subquery(filters)} "
            "GROUP BY a.user_id;"
        ), filters.dict())).scalars().all()


async def get_key_repeats(engine: DatabaseEngineAsync, filters: AnnotationFilters) -> dict[str, list[int]]:
    # dictionary of (key=) keys (AnnotationSchemeLabel.key) in the set and (values=) the list of available repeats
    async with engine.session() as session:
        keys = (await session.execute(text(
            "SELECT a.key, array_agg(distinct a.repeat) as repeats "
            "FROM annotation a "
            f" {_get_filter_subquery(filters)} "
            "GROUP BY a.key;"
        ), filters.dict())).mappings().all()
        return {
            row['key']: row['repeats']
            for row in keys
        }


class Key(NamedTuple):
    key: str
    repeat: int


AnnotationValue = int | float | bool | str
ItemId = str
UserId = str
AnnotationMatrixDict = dict[ItemId, dict[tuple[Key, ...], dict[UserId, AnnotationValue]]]


class AnnotationMatrix(BaseModel):
    scheme_id: str
    keys: list[tuple[Key, ...]]
    users: list[UserId]
    matrix: dict[ItemId, list[list[AnnotationValue | None] | None]]


def _unpack_nested_keys(annotations: dict[str, AnnotationModel], annotation: AnnotationModel) -> list[Key]:
    if annotation is None:
        return []
    return _unpack_nested_keys(annotations, annotations.get(annotation.parent)) + \
           [Key(annotation.key, annotation.repeat)]


def _get_value(annotation: AnnotationModel) -> AnnotationValue | None:
    if annotation.value_int is not None:
        return annotation.value_int
    if annotation.value_bool is not None:
        return annotation.value_bool
    if annotation.value_str is not None:
        return annotation.value_str
    if annotation.value_float is not None:
        return annotation.value_float
    return None


async def get_item_annotation_matrix_dict(engine: DatabaseEngineAsync,
                                          filters: AnnotationFilters) -> AnnotationMatrixDict:
    async with engine.session() as session:
        result = (await session.execute(text(
            "SELECT a.item_id, json_agg(a.*) as annotations "
            "FROM annotation a "
            f" {_get_filter_subquery(filters)} "
            "GROUP BY a.item_id;"
        ), filters.dict())).mappings().all()

        annotation_matrix = {}

        for row in result:
            item_id = str(row['item_id'])
            annotations = {anno['annotation_id']: AnnotationModel.parse_obj(anno) for anno in row['annotations']}

            for annotation in annotations.values():
                key = tuple(_unpack_nested_keys(annotations, annotation))
                value = _get_value(annotation)

                if value is None:
                    continue

                if item_id not in annotation_matrix:
                    annotation_matrix[item_id] = {}
                if key not in annotation_matrix[item_id]:
                    annotation_matrix[item_id][key] = {}
                annotation_matrix[item_id][key][str(annotation.user_id)] = value

        return annotation_matrix


async def get_item_annotation_matrix(engine: DatabaseEngineAsync, filters: AnnotationFilters) -> AnnotationMatrix:
    matrix_dict = await get_item_annotation_matrix_dict(engine, filters)
    users = []
    keys = []
    for item in matrix_dict.values():
        for key, annotations in item.items():
            keys.append(key)
            users += list(annotations.keys())
    users = {uid: i for i, uid in enumerate(set(users))}
    keys = {lid: i for i, lid in enumerate(set(keys))}

    matrix = {}
    for item_id, item in matrix_dict.items():
        matrix[item_id] = [None] * len(keys)
        for key, annotations in item.items():
            matrix[item_id][keys[key]] = [None] * len(users)
            for user_id, value in annotations.items():
                matrix[item_id][keys[key]][users[user_id]] = value
    async with engine.session() as session:
        scheme_id = (await session.execute(text(
            "SELECT a.annotation_scheme_id "
            "FROM annotation a "
            f" {_get_filter_subquery(filters)} "
            "GROUP BY a.annotation_scheme_id;"
        ), filters.dict())).scalars().first()
    return AnnotationMatrix(users=list(users.keys()), keys=list(keys.keys()), matrix=matrix, scheme_id=str(scheme_id))
