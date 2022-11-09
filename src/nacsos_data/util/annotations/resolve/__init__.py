from sqlalchemy import text
from nacsos_data.db.connection import DatabaseEngineAsync
from nacsos_data.db.crud.annotations import read_annotation_scheme
from nacsos_data.models.annotations import AnnotationModel, AnnotationValue, Label
from nacsos_data.models.bot_annotations import \
    AnnotationFilters, \
    AnnotationFiltersType, \
    AnnotationMatrix, \
    AnnotationMatrixList, \
    ResolutionMethod, \
    ResolvedAnnotations
from nacsos_data.util.annotations.resolve.majority_vote import naive_majority_vote
from nacsos_data.util.annotations.validation import flatten_annotation_scheme
from nacsos_data.util.errors import NotFoundError

# ideas for resolving algorithms:
#   - naive majority vote (per key,repeat)
#   - majority vote (including secondary class if available)
#   - weighted vote (with manually assigned "trust" weights per annotator)
#   - weighted vote (compute annotator trust/reliability)
#   - ...
ItemId = str
UserId = str
AnnotationMatrixDict = dict[ItemId, dict[tuple[Label, ...], dict[UserId, AnnotationValue]]]


class InvalidFilterError(AssertionError):
    pass


class AnnotationFilterObject(AnnotationFilters):
    def get_subquery(self) -> tuple[str, AnnotationFiltersType]:
        where = []
        filters = self.get_filters()
        for db_col, (comp_s, comp_l), key in [('ass.assignment_scope_id', ('=', 'IN'), 'scope_id'),
                                              ('a.annotation_scheme_id', ('=', 'IN'), 'scheme_id'),
                                              ('a.user_id', ('=', 'IN'), 'user_id'),
                                              ('a.key', ('=', 'IN'), 'key'),
                                              ('a.repeat', ('=', 'IN'), 'repeat')]:
            if filters.get(key) is not None:
                where.append(f'{db_col} {comp_l if type(filters[key]) == tuple else comp_s} :{key}')

        if len(where) == 0:
            raise InvalidFilterError('You did not specify any valid filter.')

        subquery = f' WHERE {" AND ".join(where)} '
        if filters.get('scope_id') is not None:
            subquery = f' JOIN assignment ass on ass.assignment_id = a.assignment_id {subquery} '

        return subquery, filters

    def get_filters(self) -> AnnotationFiltersType:
        ret = {}
        for key, value in self.dict().items():
            if value is not None:
                if type(value) == list:
                    if len(value) == 1:
                        ret[key] = value[0]
                    else:
                        ret[key] = tuple(value)
                else:
                    ret[key] = value
        return ret


async def read_item_annotations(filters: AnnotationFilterObject, db_engine: DatabaseEngineAsync) \
        -> dict[str, list[AnnotationModel]]:
    """
    :param db_engine: Connection to the database
    :param filters:
    :return: dictionary (keys are item_ids) of all annotations per item that match the filters.
    """
    async with db_engine.session() as session:
        subquery, query_filters = filters.get_subquery()
        annotations = (await session.execute(text(
            "SELECT a.item_id, json_agg(a.*) as annotations "
            "FROM annotation a "
            f" {subquery} "
            "GROUP BY a.item_id;"
        ), query_filters)).mappings().all()

        return {
            row['item_id']: [AnnotationModel.parse_obj(anno) for anno in row['annotations']]
            for row in annotations
        }


async def read_annotator_ids(filters: AnnotationFilterObject, db_engine: DatabaseEngineAsync) -> list[str]:
    # list of all (unique) user_ids that have at least one annotation in the set
    async with db_engine.session() as session:
        subquery, query_filters = filters.get_subquery()
        return [str(uid) for uid in (await session.execute(text(
            "SELECT DISTINCT a.user_id "
            "FROM annotation a "
            f"{subquery};"
        ), query_filters)).scalars()]


async def read_key_repeats(filters: AnnotationFilterObject, db_engine: DatabaseEngineAsync) -> dict[str, list[int]]:
    # dictionary of (key=) keys (AnnotationSchemeLabel.key) in the set and (values=) the list of available repeats
    async with db_engine.session() as session:
        subquery, query_filters = filters.get_subquery()
        keys = (await session.execute(text(
            "SELECT a.key, array_agg(distinct a.repeat) as repeats "
            "FROM annotation a "
            f" {subquery} "
            "GROUP BY a.key;"
        ), query_filters)).mappings().all()
        return {
            row['key']: row['repeats']
            for row in keys
        }


def _unpack_nested_keys(annotations: dict[str, AnnotationModel], annotation: AnnotationModel | None) -> list[Label]:
    if annotation is None:
        return []
    return _unpack_nested_keys(annotations,
                               annotations.get(str(annotation.parent))
                               if annotation.parent is not None else None) + [Label(annotation.key, annotation.repeat)]


def _get_value(annotation: AnnotationModel) -> AnnotationValue:
    return AnnotationValue(value_int=annotation.value_int, value_float=annotation.value_float,
                           value_bool=annotation.value_bool, value_str=annotation.value_str,
                           multi_int=annotation.multi_int)


async def read_item_annotation_matrix_dict(filters: AnnotationFilterObject,
                                           db_engine: DatabaseEngineAsync,
                                           ignore_order: bool = False,
                                           ignore_hierarchy: bool = False) -> AnnotationMatrixDict:
    async with db_engine.session() as session:
        subquery, query_filters = filters.get_subquery()
        result = (await session.execute(text(
            "SELECT a.item_id, json_agg(a.*) as annotations "
            "FROM annotation a "
            f" {subquery} "
            "GROUP BY a.item_id;"
        ), query_filters)).mappings().all()

        annotation_matrix: AnnotationMatrixDict = {}

        for row in result:
            item_id = str(row['item_id'])
            annotations = {anno['annotation_id']: AnnotationModel.parse_obj(anno) for anno in row['annotations']}

            for annotation in annotations.values():
                key = tuple(_unpack_nested_keys(annotations, annotation))

                if ignore_order:
                    key = tuple([Label(label.key, 1) for label in key])
                if ignore_hierarchy:
                    key = (key[-1],)

                value = _get_value(annotation)

                if item_id not in annotation_matrix:
                    annotation_matrix[item_id] = {}
                if key not in annotation_matrix[item_id]:
                    annotation_matrix[item_id][key] = {}
                annotation_matrix[item_id][key][str(annotation.user_id)] = value

        return annotation_matrix


async def read_item_annotation_matrix(filters: AnnotationFilterObject,
                                      db_engine: DatabaseEngineAsync,
                                      ignore_order: bool = False,
                                      ignore_hierarchy: bool = False) -> AnnotationMatrix:
    matrix_dict = await read_item_annotation_matrix_dict(filters, db_engine=db_engine,
                                                         ignore_hierarchy=ignore_hierarchy, ignore_order=ignore_order)
    _users = []
    _keys = []
    for item in matrix_dict.values():
        for key, annotations in item.items():
            _keys.append(key)
            _users += list(annotations.keys())
    users: dict[str, int] = {uid: i for i, uid in enumerate(set(_users))}
    keys: dict[tuple[Label, ...], int] = {lid: i for i, lid in enumerate(set(_keys))}

    matrix: AnnotationMatrixList = {}
    for item_id, item in matrix_dict.items():
        matrix[item_id] = [None] * len(keys)
        for key, annotations in item.items():
            resolved_key = keys[key]
            matrix[item_id][resolved_key] = [None] * len(users)  # type: ignore[index] # FIXME
            for user_id, value in annotations.items():
                matrix[item_id][resolved_key][users[user_id]] = value  # type: ignore[index] # FIXME

    async with db_engine.session() as session:
        subquery, query_filters = filters.get_subquery()
        scheme_id = (await session.execute(text(
            "SELECT a.annotation_scheme_id "
            "FROM annotation a "
            f" {subquery} "
            "GROUP BY a.annotation_scheme_id;"
        ), query_filters)).scalars().first()

    return AnnotationMatrix(users=list(users.keys()), labels=list(keys.keys()), matrix=matrix, scheme_id=str(scheme_id))


async def get_resolved_item_annotations(strategy: ResolutionMethod, filters: AnnotationFilterObject,
                                        db_engine: DatabaseEngineAsync,
                                        ignore_order: bool = False,
                                        ignore_hierarchy: bool = False) -> tuple[AnnotationMatrix, ResolvedAnnotations]:
    matrix = await read_item_annotation_matrix(db_engine=db_engine, filters=filters,
                                               ignore_hierarchy=ignore_hierarchy, ignore_order=ignore_order)
    scheme = await read_annotation_scheme(annotation_scheme_id=matrix.scheme_id, db_engine=db_engine)

    if not matrix or not scheme:
        raise NotFoundError(f'Matrix empty or no annotation scheme for {matrix.scheme_id}')

    flat_scheme = flatten_annotation_scheme(scheme)

    if strategy == 'majority':
        return matrix, naive_majority_vote(matrix=matrix, scheme=flat_scheme.labels)

    raise NotImplementedError(f'Resolution strategy "{strategy}" not implemented (yet)!')
