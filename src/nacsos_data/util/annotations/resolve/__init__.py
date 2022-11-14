from datetime import datetime
from sqlalchemy import text
from nacsos_data.db.connection import DatabaseEngineAsync
from nacsos_data.db.crud.annotations import read_annotation_scheme
from nacsos_data.models.annotations import \
    AnnotationModel, \
    AnnotationSchemeModel, \
    FlattenedAnnotationSchemeLabel
from nacsos_data.models.bot_annotations import \
    Label, \
    AnnotationFilters, \
    AnnotationFiltersType, \
    ResolutionMethod, \
    AnnotationCollection, \
    GroupedAnnotations, \
    GroupedBotAnnotation
from nacsos_data.models.users import UserModel
from nacsos_data.util.annotations.resolve.majority_vote import naive_majority_vote
from nacsos_data.util.annotations.validation import flatten_annotation_scheme
from nacsos_data.util.errors import NotFoundError


# ideas for resolving algorithms:
#   - naive majority vote (per key,repeat)
#   - majority vote (including secondary class if available)
#   - weighted vote (with manually assigned "trust" weights per annotator)
#   - weighted vote (compute annotator trust/reliability)
#   - ...


class InvalidFilterError(AssertionError):
    pass


class AnnotationFilterObject(AnnotationFilters):
    def get_subquery(self) -> tuple[str, str, AnnotationFiltersType]:
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

        join = f''
        if filters.get('scope_id') is not None:
            join = f'JOIN assignment ass on ass.assignment_id = a.assignment_id'

        return join, ' AND '.join(where), filters

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


async def read_num_annotation_changes_after(timestamp: str | datetime,
                                            filters: AnnotationFilterObject,
                                            db_engine: DatabaseEngineAsync) -> int:
    """
    Looks for `Annotation`s that were added or edited after `timestamp` and returns the count.
    This is assumed to be used in the context of `Annotation` resolution, so the same filter logic is used, so
    it can be re-applied later to see if there have been changes that should be included in the resolution.
    NOTICE: This does *not* recognise deletions (but `Annotation`s should never be deleted anyway).
    If you are looking for the actually changed `Annotation`s, use `read_changed_annotations_after`.
    :param timestamp:
    :param filters:
    :param db_engine:
    :return:
    """
    async with db_engine.session() as session:
        filter_join, filter_where, filter_data = filters.get_subquery()
        filter_data['timestamp'] = timestamp
        num_changes = (await session.execute(text(
            "SELECT count(1) "
            "FROM annotation AS a "
            f" {filter_join} "
            f"WHERE {filter_where} "
            f"      AND (a.time_created > :timestamp OR a.time_updated > :timestamp);"
        ), filter_data)).scalar()
        return num_changes


async def read_changed_annotations_after(timestamp: str | datetime,
                                         filters: AnnotationFilterObject,
                                         db_engine: DatabaseEngineAsync) -> list[AnnotationModel]:
    """
    See `read_num_annotation_changes_after`
    :param timestamp:
    :param filters:
    :param db_engine:
    :return:
    """
    async with db_engine.session() as session:
        filter_join, filter_where, filter_data = filters.get_subquery()
        filter_data['timestamp'] = timestamp
        annotations = (await session.execute(text(
            "SELECT a.* "
            "FROM annotation AS a "
            f" {filter_join} "
            f"WHERE {filter_where} "
            f"      AND (a.time_created > :timestamp OR a.time_updated > :timestamp);"
        ), filter_data)).mappings().all()
        return [AnnotationModel.parse_obj(anno) for anno in annotations]


async def read_item_annotations(filters: AnnotationFilterObject,
                                db_engine: DatabaseEngineAsync,
                                ignore_hierarchy: bool = False,
                                ignore_order: bool = False) -> dict[str, list[GroupedAnnotations]]:
    """
    asd
    :param db_engine: Connection to the database
    :param filters:
    :param ignore_hierarchy: if False, looking at keys linearly (ignoring parents)
    :param ignore_order: if False, the order is ignored and e.g. single-choice with secondary category
                           virtually becomes multi-choice of two categories
    :return: dictionary (keys are item_ids) of all annotations per item that match the filters.
    """
    async with db_engine.session() as session:
        filter_join, filter_where, filter_data = filters.get_subquery()

        repeat = 'a.repeat'
        if ignore_order:  # if repeat is ignored, always forcing it to 1
            repeat = '1'
        if ignore_hierarchy:
            annotations = (await session.execute(text(
                "SELECT a.item_id, "
                f"       array_to_json(ARRAY[(a.key, {repeat})::annotation_label]) as label, "
                "       json_agg(a.*) as annotations "
                "FROM annotation AS a "
                f"         {filter_join} "
                f"      WHERE {filter_where} "
                "GROUP BY a.item_id, a.key, a.repeat;"
            ), filter_data)).mappings().all()
        else:
            annotations = (await session.execute(text(
                "WITH RECURSIVE ctename AS ( "
                f"      SELECT a.*, ARRAY[(a.key, {repeat})::annotation_label] as path "
                "      FROM annotation AS a "
                f"         {filter_join} "
                f"      WHERE {filter_where} "
                "   UNION ALL "
                f"      SELECT a.*, array_append(ctename.path, ((a.key, {repeat})::annotation_label)) "
                "      FROM annotation a "
                "         JOIN ctename ON a.annotation_id = ctename.parent "
                ") "
                "SELECT item_id, array_to_json(path) as label, json_agg(ctename.*) as annotations "
                "FROM ctename "
                "WHERE parent is NULL "
                "GROUP BY item_id, path;"
            ), filter_data)).mappings().all()

        ret = {}
        for row in annotations:
            item_uuid = str(row['item_id'])
            if item_uuid not in ret:
                ret[item_uuid] = []
            ret[item_uuid].append(GroupedAnnotations(path=[Label.parse_obj(label)
                                                           for label in row['label']],
                                                     annotations=[AnnotationModel.parse_obj(anno)
                                                                  for anno in row['annotations']]))

        return ret


async def read_annotators(filters: AnnotationFilterObject, db_engine: DatabaseEngineAsync) -> list[UserModel]:
    # list of all (unique) users that have at least one annotation in the set
    async with db_engine.session() as session:
        filter_join, filter_where, filter_data = filters.get_subquery()
        return [UserModel.parse_obj(user) for user in (await session.execute(text(
            "SELECT DISTINCT u.* "
            "FROM annotation AS a "
            f"   {filter_join} "
            "    JOIN \"user\" u on u.user_id = a.user_id "
            f"WHERE {filter_where};"
        ), filter_data)).mappings().all()]


async def read_labels(filters: AnnotationFilterObject,
                      db_engine: DatabaseEngineAsync,
                      ignore_hierarchy: bool = True,
                      ignore_order: bool = True) -> list[list[Label]]:
    # list of all (unique) labels in this selection
    async with db_engine.session() as session:

        repeat = 'a.repeat'
        if ignore_order:  # if repeat is ignored, always forcing it to 1
            repeat = '1'

        filter_join, filter_where, filter_data = filters.get_subquery()
        if ignore_hierarchy:
            return [[Label.parse_obj(sub_label) for sub_label in label]
                    for label in
                    (await session.execute(text(
                        "SELECT array_to_json(label) as label "
                        "FROM ( "
                        f"   SELECT DISTINCT ARRAY[(a.key, {repeat})::annotation_label] as label "
                        "    FROM annotation AS a "
                        f"      {filter_join} "
                        f"   WHERE {filter_where}) labels;"
                    ), filter_data)).scalars()]
        else:
            return [[Label.parse_obj(sub_label) for sub_label in label]
                    for label in
                    (await session.execute(text(
                        "SELECT array_to_json(label) as label "
                        "FROM ( "
                        "WITH RECURSIVE ctename AS ( "
                        f"      SELECT a.annotation_id, a.parent, ARRAY[(a.key, {repeat})::annotation_label] as label "
                        "       FROM annotation AS a "
                        f"         {filter_join} "
                        f"      WHERE {filter_where} "
                        "   UNION ALL "
                        "      SELECT a.annotation_id, a.parent,"
                        f"             array_append(ctename.label, ((a.key, {repeat})::annotation_label)) "
                        "      FROM annotation a "
                        "         JOIN ctename ON a.annotation_id = ctename.parent "
                        ") "
                        "SELECT DISTINCT label "
                        "FROM ctename "
                        "WHERE parent is NULL) labels;"
                    ), filter_data)).scalars()]


async def get_resolved_item_annotations(strategy: ResolutionMethod, filters: AnnotationFilterObject,
                                        db_engine: DatabaseEngineAsync,
                                        ignore_hierarchy: bool = False,
                                        ignore_order: bool = False) \
        -> tuple[AnnotationSchemeModel, list[FlattenedAnnotationSchemeLabel],
                 AnnotationCollection, dict[str, list[GroupedBotAnnotation]]]:
    annotations = await read_item_annotations(db_engine=db_engine, filters=filters,
                                              ignore_hierarchy=ignore_hierarchy, ignore_order=ignore_order)
    labels = await read_labels(db_engine=db_engine, filters=filters,
                               ignore_hierarchy=ignore_hierarchy, ignore_order=ignore_order)
    annotators = await read_annotators(filters=filters, db_engine=db_engine)
    scheme = await read_annotation_scheme(annotation_scheme_id=filters.scheme_id, db_engine=db_engine)

    if not annotations or not scheme:
        raise NotFoundError(f'No annotations or no annotation scheme for {filters.scheme_id}')

    collection = AnnotationCollection(scheme_id=filters.scheme_id,
                                      labels=labels, annotators=annotators, annotations=annotations)

    flat_scheme = flatten_annotation_scheme(scheme)

    if strategy == 'majority':
        return scheme, flat_scheme.labels, \
               collection, naive_majority_vote(collection=collection, scheme=flat_scheme.labels)

    raise NotImplementedError(f'Resolution strategy "{strategy}" not implemented (yet)!')
