import uuid
import logging
from typing import Type, TYPE_CHECKING

from pydantic import BaseModel
from sqlalchemy import select, \
    func as F, \
    union, \
    distinct, \
    case, \
    and_, \
    or_, \
    any_, \
    literal, \
    String, \
    Integer, \
    Column, \
    ColumnElement, \
    CTE, \
    Label
from sqlalchemy.dialects.postgresql import UUID

from nacsos_data.db.connection import DatabaseEngineAsync
from nacsos_data.db.schemas.annotations import Annotation, Assignment, AssignmentScope, AnnotationScheme
from nacsos_data.db.schemas.bot_annotations import BotAnnotation, BotAnnotationMetaData

from ..errors import NotFoundError
from ...db.schemas import User, ProjectPermissions, Project, ItemType, AcademicItem, TwitterItem

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession  # noqa: F401

logger = logging.getLogger('nacsos_data.util.annotations.export')


class LabelOptions(BaseModel):
    key: str
    options_int: list[int] | None = None
    options_bool: list[bool] | None = None
    options_multi: list[int] | None = None
    strings: bool | None = None


def _bool_label_columns(key: str, repeat: int, cte: CTE) \
        -> list[Label]:  # type: ignore[type-arg]
    conditions = [cte.c.key == key]
    label = lambda x: f'{key}|{x}'  # noqa: E731
    if repeat is not None:
        conditions.append(cte.c.repeat == repeat)
        label = lambda x: f'{key}({repeat})|{x}'  # noqa: E731
    return [
        case((F.count().filter(and_(*conditions)) > 0,
              F.max(case((and_(cte.c.value_bool == vb, *conditions), 1), else_=0))))
        .label(label(vs))
        for vs, vb in [('0', False), ('1', True)]
    ]


def _single_label_columns(key: str, repeat: int | None, values: list[int], cte: CTE) \
        -> list[Label]:  # type: ignore[type-arg]
    conditions = [cte.c.key == key]
    label = lambda x: f'{key}|{x}'  # noqa: E731
    if repeat is not None:
        conditions.append(cte.c.repeat == repeat)
        label = lambda x: f'{key}({repeat})|{x}'  # noqa: E731
    return [
        case((F.count().filter(and_(*conditions)) > 0,
              F.max(case((and_(cte.c.value_int == v, *conditions), 1), else_=0))))
        .label(label(v))
        for v in values
    ]


def _multi_label_columns(key: str, repeat: int | None, values: list[int], cte: CTE) \
        -> list[Label]:  # type: ignore[type-arg]
    conditions = [cte.c.key == key]
    label = lambda x: f'{key}|{x}'  # noqa: E731
    if repeat is not None:
        conditions.append(cte.c.repeat == repeat)
        label = lambda x: f'{key}({repeat})|{x}'  # noqa: E731
    return [
        case((F.count().filter(and_(*conditions)) > 0,
              F.max(case((and_(any_(cte.c.multi_int) == v, *conditions), 1), else_=0))))
        .label(label(v))
        for v in values
    ]


def _get_label_selects(labels: dict[str, LabelOptions],
                       repeats: list[int] | None,
                       cte: CTE) -> list[Label]:  # type: ignore[type-arg]
    # FIXME: we are ignoring `repeat` for child labels for now, hence, exports might be inconsistent for ranked labels
    selects = []
    for label in labels.values():
        for repeat in repeats or [None]:  # type: ignore[list-item]
            if label.options_int:
                selects += _single_label_columns(label.key, repeat=repeat, cte=cte, values=label.options_int)
            elif label.options_bool:
                selects += _bool_label_columns(label.key, repeat=repeat, cte=cte)
            elif label.options_multi:
                selects += _multi_label_columns(label.key, repeat=repeat, cte=cte, values=label.options_multi)
            else:
                pass
                # raise RuntimeError('Invalid state')

    return selects


def _labels_subquery(bot_annotation_metadata_ids: list[str] | list[uuid.UUID] | None,
                     assignment_scope_ids: list[str] | list[uuid.UUID] | None,
                     user_ids: list[str] | list[uuid.UUID] | None,
                     labels: dict[str, LabelOptions] | None,
                     ignore_order: bool) -> CTE:
    def _label_filter(Schema: Type[Annotation] | Type[BotAnnotation],
                      label: LabelOptions) -> ColumnElement[bool] | None:  # type: ignore[type-arg]
        if label.options_int:
            return and_(Schema.key == label.key,
                        Schema.value_int.in_(label.options_int))
        if label.options_bool:
            return and_(Schema.key == label.key,
                        Schema.value_bool.in_(label.options_bool))
        if label.options_multi:
            return and_(Schema.key == label.key,
                        Schema.multi_int.overlap(label.options_multi))
        if label.strings:
            return Schema.key == label.key  # type: ignore[no-any-return]

        return None

    sub_queries = []
    if assignment_scope_ids is not None:
        where = [Assignment.assignment_scope_id.in_(assignment_scope_ids)]
        if user_ids is not None and len(user_ids) > 0:
            where.append(Assignment.user_id.in_(user_ids))
        if labels is not None:
            ors = [_label_filter(Annotation, label_) for label_ in labels.values()]
            ors = [o for o in ors if o is not None]
            if ors is not None and len(ors) > 0:
                where.append(or_(*ors))  # type: ignore[arg-type]

        sub_queries.append(
            select(Assignment.item_id,
                   Assignment.user_id,
                   Annotation.annotation_id.label('label_id'),
                   Annotation.parent,
                   Annotation.key,
                   Annotation.repeat if not ignore_order else literal(1, type_=Integer).label('repeat'),
                   Annotation.value_int,
                   Annotation.value_bool,
                   Annotation.value_str,
                   Annotation.multi_int)
            .join(Annotation, Annotation.assignment_id == Assignment.assignment_id, isouter=True)
            .where(*where)
        )

    if bot_annotation_metadata_ids is not None:
        where = [BotAnnotation.bot_annotation_metadata_id.in_(bot_annotation_metadata_ids)]
        if labels is not None:
            ors = [_label_filter(BotAnnotation, label_) for label_ in labels.values()]
            ors = [o for o in ors if o is not None]
            if len(ors) > 0:
                where.append(or_(*ors))  # type: ignore[arg-type]

        sub_queries.append(
            select(BotAnnotation.item_id,
                   literal(None, type_=UUID).label('user_id'),
                   BotAnnotation.bot_annotation_id.label('label_id'),
                   BotAnnotation.parent,
                   BotAnnotation.key,
                   BotAnnotation.repeat if not ignore_order else literal(1, type_=Integer).label('repeat'),
                   BotAnnotation.value_int,
                   BotAnnotation.value_bool,
                   BotAnnotation.value_str,
                   BotAnnotation.multi_int)
            .where(*where)
        )

    if len(sub_queries) > 1:
        return union(*sub_queries).cte()
    if len(sub_queries) == 1:
        return sub_queries[0].cte()

    raise AssertionError('You need at least on subquery for labels!')


async def get_project_bot_scopes(project_id: str | uuid.UUID, db_engine: DatabaseEngineAsync) -> list[dict[str, str]]:
    async with db_engine.session() as session:  # type: AsyncSession
        stmt = select(BotAnnotationMetaData.bot_annotation_metadata_id.cast(type_=String).label('id'),
                      BotAnnotationMetaData.name) \
            .where(BotAnnotationMetaData.project_id == project_id) \
            .order_by(BotAnnotationMetaData.time_created)
        return [dict(r) for r in (await session.execute(stmt)).mappings().all()]


async def get_project_scopes(project_id: str | uuid.UUID, db_engine: DatabaseEngineAsync) -> list[dict[str, str]]:
    async with db_engine.session() as session:  # type: AsyncSession
        stmt = select(AssignmentScope.assignment_scope_id.cast(type_=String).label('id'),
                      AssignmentScope.name,
                      AnnotationScheme.annotation_scheme_id.cast(type_=String).label('scheme_id'),
                      AnnotationScheme.name.label('scheme_name')) \
            .join(AnnotationScheme, AnnotationScheme.annotation_scheme_id == AssignmentScope.annotation_scheme_id) \
            .where(AnnotationScheme.project_id == project_id) \
            .order_by(AssignmentScope.time_created)
        return [dict(r) for r in (await session.execute(stmt)).mappings().all()]


async def get_project_users(project_id: str | uuid.UUID, db_engine: DatabaseEngineAsync) -> list[dict[str, str]]:
    async with db_engine.session() as session:  # type: AsyncSession
        stmt = select(User.user_id.cast(type_=String).label('id'),
                      User.username.label('name')) \
            .join(ProjectPermissions, ProjectPermissions.user_id == User.user_id) \
            .where(ProjectPermissions.project_id == project_id) \
            .order_by(User.username)
        return [dict(r) for r in (await session.execute(stmt)).mappings().all()]


async def get_labels(stmt_labels: CTE, db_engine: DatabaseEngineAsync) -> dict[str, LabelOptions]:
    stmt_labels_ = union(
        select(stmt_labels.c.key, stmt_labels.c.value_int, stmt_labels.c.value_bool, stmt_labels.c.value_str,
               F.unnest(stmt_labels.c.multi_int).label('multis')),
        select(stmt_labels.c.key, stmt_labels.c.value_int, stmt_labels.c.value_bool, stmt_labels.c.value_str,
               literal(None, type_=Integer).label('multis')),
    ).subquery()

    stmt_options = select(
        stmt_labels_.c.key,
        F.array_agg(distinct(stmt_labels_.c.value_int))
        .filter(stmt_labels_.c.value_int.isnot(None)).label('options_int'),
        F.array_agg(distinct(stmt_labels_.c.value_bool))
        .filter(stmt_labels_.c.value_bool.isnot(None)).label('options_bool'),
        F.array_agg(distinct(stmt_labels_.c.multis))
        .filter(stmt_labels_.c.multis.isnot(None)).label('options_multi'),
        (F.count().filter(stmt_labels_.c.value_str.isnot(None)) > 0).label('strings')
    ) \
        .where(stmt_labels_.c.key.isnot(None)) \
        .group_by(stmt_labels_.c.key) \
        .order_by(stmt_labels_.c.key)

    async with db_engine.session() as session:  # type: AsyncSession
        result = (await session.execute(stmt_options)).mappings().all()

        # construct a lookup map of key->options/values/choices
        return {row['key']: LabelOptions.parse_obj(row) for row in result}


async def get_project_labels(project_id: str | uuid.UUID, db_engine: DatabaseEngineAsync) -> dict[str, LabelOptions]:
    bot_scopes = await get_project_bot_scopes(project_id=project_id, db_engine=db_engine)
    scopes = await get_project_scopes(project_id=project_id, db_engine=db_engine)
    users = await get_project_users(project_id=project_id, db_engine=db_engine)

    bot_annotation_metadata_ids = [r['id'] for r in bot_scopes]
    assignment_scope_ids = [r['id'] for r in scopes]
    user_ids = [r['id'] for r in users]

    stmt_labels = _labels_subquery(bot_annotation_metadata_ids=bot_annotation_metadata_ids,
                                   assignment_scope_ids=assignment_scope_ids,
                                   user_ids=user_ids,
                                   labels=None,
                                   ignore_order=True)

    return await get_labels(stmt_labels=stmt_labels, db_engine=db_engine)


F2CRetType = tuple[Type[AcademicItem] | Type[TwitterItem], list[Type[Column]]] | tuple[None, None]  # type: ignore[type-arg]


async def fields2col(project_id: str | uuid.UUID,
                     fields: list[str] | None,
                     db_engine: DatabaseEngineAsync) -> F2CRetType:
    if fields is None or len(fields) == 0:
        return None, None

    async with db_engine.session() as session:  # type: AsyncSession
        project = await session.get(Project, project_id)

        if project is None:
            raise NotFoundError(f'No project with id={project_id}!')

        if project.type == ItemType.academic:
            ItemSchema = AcademicItem  # type: ignore[assignment]
        elif project.type == ItemType.twitter:
            ItemSchema = TwitterItem  # type: ignore[assignment]

        columns = []
        for field in fields:
            if hasattr(ItemSchema, field):
                columns.append(getattr(ItemSchema, field))

        return ItemSchema, columns


async def prepare_export_table(bot_annotation_metadata_ids: list[str] | list[uuid.UUID] | None,
                               assignment_scope_ids: list[str] | list[uuid.UUID] | None,
                               user_ids: list[str] | list[uuid.UUID] | None,
                               project_id: str | uuid.UUID,
                               labels: list[LabelOptions],
                               item_fields: list[str] | None,
                               ignore_hierarchy: bool,
                               ignore_order: bool,
                               db_engine: DatabaseEngineAsync) -> list[dict[str, bool | int | str | None]]:
    labels_map = {lab.key: lab for lab in labels}
    stmt_labels = _labels_subquery(bot_annotation_metadata_ids=bot_annotation_metadata_ids,
                                   assignment_scope_ids=assignment_scope_ids,
                                   user_ids=user_ids,
                                   labels=labels_map,
                                   ignore_order=ignore_order)

    # Translate the list of strings into table columns (for extra fields from Items)
    ItemSchema, item_columns = await fields2col(project_id=project_id, fields=item_fields, db_engine=db_engine)

    async with db_engine.session() as session:  # type: AsyncSession
        if ignore_hierarchy:
            # Prepare the CASE expressions to spread label values across binary fields
            label_selects = _get_label_selects(labels=labels_map,
                                               repeats=None if ignore_order else [1, 2, 3, 4],
                                               cte=stmt_labels)

            # Finally construct the main query
            stmt_item_labels = (
                select(stmt_labels.c.item_id,
                       stmt_labels.c.user_id,
                       *label_selects)
                .group_by(stmt_labels.c.item_id, stmt_labels.c.user_id)
                .subquery()
            )

            # add additional information, such as Item fields and username
            if item_columns is None:
                stmt = select(F.coalesce(User.username, 'RESOLVED').label('username'),
                              stmt_item_labels) \
                    .join(User, User.user_id == stmt_item_labels.c.user_id, isouter=True) \
                    .order_by(stmt_item_labels.c.item_id, User.username)
            else:
                stmt = (select(F.coalesce(User.username, 'RESOLVED').label('username'),
                               stmt_item_labels,
                               *item_columns)
                        .join(ItemSchema)  # type: ignore[arg-type]
                        .join(User, User.user_id == stmt_item_labels.c.user_id, isouter=True)
                        .order_by(stmt_item_labels.c.item_id, User.username))
        else:
            raise NotImplementedError('This is a bit more tricky, coming up soon.')

        result = (await session.execute(stmt)).mappings().all()

        return [dict(r) for r in result]
