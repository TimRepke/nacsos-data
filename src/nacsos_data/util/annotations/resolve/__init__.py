import logging
from datetime import datetime
from collections import defaultdict
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

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
    GroupedBotAnnotation, BotAnnotationModel
from nacsos_data.models.users import UserModel
from nacsos_data.util.annotations.resolve.majority_vote import naive_majority_vote
from nacsos_data.util.annotations.validation import flatten_annotation_scheme
from nacsos_data.util.errors import NotFoundError, InvalidFilterError

# ideas for resolving algorithms:
#   - naive majority vote (per key,repeat)
#   - majority vote (including secondary class if available)
#   - weighted vote (with manually assigned "trust" weights per annotator)
#   - weighted vote (compute annotator trust/reliability)
#   - ...

logger = logging.getLogger('nacsos_data.util.annotations.resolve')


class AnnotationFilterObject(AnnotationFilters):
    def get_subquery(self) -> tuple[str, str, AnnotationFiltersType]:
        where = []
        filters = self.get_filters()
        for db_col, key in [('ass.assignment_scope_id', 'scope_id'),
                            ('a.annotation_scheme_id', 'scheme_id'),
                            ('a.user_id', 'user_id'),
                            ('a.key', 'key'),
                            ('a.repeat', 'repeat')]:
            if filters.get(key) is not None:
                if type(filters[key]) == list:
                    where.append(f' {db_col} = ANY(:{key}) ')
                else:
                    where.append(f' {db_col} = :{key} ')

        if len(where) == 0:
            raise InvalidFilterError('You did not specify any valid filter.')

        join = f''
        if filters.get('scope_id') is not None:
            join = f' JOIN assignment ass on ass.assignment_id = a.assignment_id '

        return join, ' AND '.join(where), filters

    def get_filters(self) -> AnnotationFiltersType:
        ret = {}
        for key, value in self.model_dump().items():
            if value is not None:
                if type(value) == list:
                    if len(value) == 1:
                        ret[key] = value[0]
                    else:
                        ret[key] = list(value)
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
        filter_data['timestamp'] = str(timestamp)
        num_changes: int = (await session.execute(text(  # type: ignore[assignment] # FIXME mypy
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
        filter_data['timestamp'] = str(timestamp)
        annotations = (await session.execute(text(
            "SELECT a.* "
            "FROM annotation AS a "
            f" {filter_join} "
            f"WHERE {filter_where} "
            f"      AND (a.time_created > :timestamp OR a.time_updated > :timestamp);"
        ), filter_data)).mappings().all()
        return [AnnotationModel.model_validate(anno) for anno in annotations]


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
                "WITH RECURSIVE ctename AS ( \n"
                "      SELECT a.annotation_id, a.time_created, a.time_updated, a.assignment_id, a.user_id, a.item_id, "
                "             a.annotation_scheme_id, a.key, a.repeat,  a.value_bool, a.value_int,a.value_float, "
                "             a.value_str, a.text_offset_start, a.text_offset_stop, a.multi_int, "
                "             a.parent, a.parent as recurse_join, "
                f"            ARRAY[(a.key, {repeat})::annotation_label] as path \n"
                "      FROM annotation AS a \n"
                f"         {filter_join} \n"
                f"      WHERE {filter_where} \n"
                "   UNION ALL \n"
                "      SELECT ctename.annotation_id, ctename.time_created, ctename.time_updated,ctename.assignment_id,"
                "             ctename.user_id, ctename.item_id, ctename.annotation_scheme_id, ctename.key, "
                "             ctename.repeat, ctename.value_bool, ctename.value_int,ctename.value_float, "
                "             ctename.value_str, ctename.text_offset_start, ctename.text_offset_stop, "
                "             ctename.multi_int, ctename.parent, a.parent as recurse_join, "
                f"            array_append(ctename.path, ((a.key, {repeat})::annotation_label)) \n"
                "      FROM annotation a \n"
                "         JOIN ctename ON a.annotation_id = ctename.recurse_join \n"
                ") \n"
                "SELECT item_id, array_to_json(path) as label, json_agg(ctename.*) as annotations \n"
                "FROM ctename \n"
                "WHERE recurse_join is NULL \n"
                "GROUP BY item_id, path;"
            ), filter_data)).mappings().all()

        ret: dict[str, list[GroupedAnnotations]] = {}
        for row in annotations:
            item_uuid = str(row['item_id'])
            if item_uuid not in ret:
                ret[item_uuid] = []
            ret[item_uuid].append(GroupedAnnotations(path=[Label.model_validate(label)
                                                           for label in row['label']],
                                                     annotations=[AnnotationModel.model_validate(anno)
                                                                  for anno in row['annotations']]))

        return ret


async def read_annotators(filters: AnnotationFilterObject, db_engine: DatabaseEngineAsync) -> list[UserModel]:
    # list of all (unique) users that have at least one annotation in the set
    async with db_engine.session() as session:
        filter_join, filter_where, filter_data = filters.get_subquery()
        return [UserModel.model_validate(user) for user in (await session.execute(text(
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
    async with db_engine.session() as session:  # type: AsyncSession

        repeat = 'a.repeat'
        if ignore_order:  # if repeat is ignored, always forcing it to 1
            repeat = '1'
        filter_join, filter_where, filter_data = filters.get_subquery()
        if ignore_hierarchy:
            return [[Label.model_validate(sub_label) for sub_label in label]
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
            return [[Label.model_validate(sub_label) for sub_label in label]
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


async def read_bot_annotations(bot_annotation_metadata_id: str,
                               db_engine: DatabaseEngineAsync) -> dict[str, list[GroupedBotAnnotation]]:
    async with db_engine.session() as session:  # type: AsyncSession
        bot_annotations = (await session.execute(text(
            "WITH RECURSIVE ctename AS ( "
            "    SELECT a.bot_annotation_id, a.bot_annotation_metadata_id, a.time_created, a.time_updated, a.item_id, "
            "           a.key, a.repeat,  a.value_bool, a.value_int,a.value_float, "
            "           a.value_str, a.multi_int, a.confidence, a.parent, a.parent as recurse_join, "
            "           ARRAY [(a.key, a.repeat)::annotation_label] as path "
            "       FROM bot_annotation AS a "
            "       WHERE a.bot_annotation_metadata_id = :bot_annotation_metadata_id "
            "    UNION ALL "
            "      SELECT ctename.bot_annotation_id, ctename.bot_annotation_metadata_id, ctename.time_created, "
            "             ctename.time_updated, ctename.item_id, ctename.key, ctename.repeat,  ctename.value_bool, "
            "             ctename.value_int,ctename.value_float, ctename.value_str, ctename.multi_int, "
            "             ctename.confidence, ctename.parent, a.parent as recurse_join, "
            "             array_append(ctename.path, ((a.key, a.repeat)::annotation_label)) "
            "       FROM bot_annotation a "
            "            JOIN ctename ON a.bot_annotation_id = ctename.recurse_join) "
            "SELECT item_id, array_to_json(path) as label, json_agg(ctename.*)::jsonb->0 as bot_annotation "
            "FROM ctename "
            "WHERE recurse_join is NULL "
            "GROUP BY item_id, path;"),
            {'bot_annotation_metadata_id': bot_annotation_metadata_id})).mappings().all()

        grouped_annotations = defaultdict(list)
        for ba in bot_annotations:
            grouped_annotations[str(ba['item_id'])].append(
                GroupedBotAnnotation(path=ba['label'],
                                     annotation=BotAnnotationModel.model_validate(ba['bot_annotation'])))
        return grouped_annotations


async def get_resolved_item_annotations(strategy: ResolutionMethod, filters: AnnotationFilterObject,
                                        db_engine: DatabaseEngineAsync,
                                        ignore_hierarchy: bool = False,
                                        ignore_order: bool = False) \
        -> tuple[AnnotationSchemeModel, list[FlattenedAnnotationSchemeLabel],
                 AnnotationCollection, dict[str, list[GroupedBotAnnotation]]]:
    logger.debug(f'Fetching all annotations matching filters: {filters} '
                 f'with ignore_hierarchy={ignore_hierarchy} and ignore_order={ignore_order}.')

    annotations = await read_item_annotations(db_engine=db_engine, filters=filters,
                                              ignore_hierarchy=ignore_hierarchy, ignore_order=ignore_order)
    labels = await read_labels(db_engine=db_engine, filters=filters,
                               ignore_hierarchy=ignore_hierarchy, ignore_order=ignore_order)
    annotators = await read_annotators(filters=filters, db_engine=db_engine)
    scheme = await read_annotation_scheme(annotation_scheme_id=filters.scheme_id, db_engine=db_engine)

    if not annotations or not scheme:
        raise NotFoundError(f'No annotations ({bool(annotations)}) or no annotation scheme'
                            f'({bool(scheme)}) for {filters.scheme_id}')

    collection = AnnotationCollection(scheme_id=filters.scheme_id,
                                      labels=labels, annotators=annotators, annotations=annotations)

    flat_scheme = flatten_annotation_scheme(scheme)

    if strategy == 'majority':
        return scheme, flat_scheme.labels, \
               collection, naive_majority_vote(collection=collection, scheme=flat_scheme.labels)

    raise NotImplementedError(f'Resolution strategy "{strategy}" not implemented (yet)!')
