import logging
import uuid
from typing import Type

from pydantic import BaseModel
from sqlalchemy import select, case, func as F, and_, literal, union
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import MappedColumn
from nacsos_data.db.connection import DatabaseEngineAsync
from nacsos_data.db.schemas.annotations import Annotation, Assignment
from nacsos_data.db.schemas.bot_annotations import BotAnnotation

from ...db.schemas import User
from ...models.items import AnyItemModelType

logger = logging.getLogger('nacsos_data.util.annotations.export')


class LabelSelector(BaseModel):
    key: str
    repeats: list[int] | None = None
    values_int: list[int] | None = None
    values_bool: list[bool] | None = None


def _get_label_selects(labels: list[LabelSelector],
                       Schema: Type[Annotation] | Type[BotAnnotation]):
    selects = []

    # FIXME: we are ignoring `repeat` for child labels for now, hence, exports might be inconsistent for ranked labels

    def generate_select(key: str,
                        value_int: int | None,
                        value_bool: bool | None,
                        repeat: int | None):
        assert value_bool is not None or value_int is not None

        value = value_int if value_int is not None else value_bool
        ValueField = Schema.value_int if value_int is not None else Schema.value_bool

        # FIXME: SECURITY RISK
        #   This is potentially dangerous (sql injection), because this is not getting escaped
        #   For now, we just trust that this is always behind login and users won't mess with it.
        if repeat is None:
            col_name = f'{key}|{int(value)}'
        else:
            col_name = f'{key}({repeat})|{int(value)}'

        selects.append(
            case(
                (F.count(Schema.key == key) == 0, None),
                else_=F.max(case(
                    (and_(Schema.key == key, ValueField == value), 1),
                    else_=0))
            ).label(col_name)
        )

    for label in labels:
        if label.values_bool is not None:
            for v in label.values_bool:
                for r in (label.repeats or [None]):
                    generate_select(key=label.key, value_bool=v, value_int=None, repeat=r)
        elif label.values_int is not None:
            for v in label.values_int:
                for r in (label.repeats or [None]):
                    generate_select(key=label.key, value_bool=None, value_int=v, repeat=r)
        else:
            raise ValueError('This should bei EITHER values_int OR values_bool!')

    return selects


async def prepare_export_table(bot_annotation_metadata_ids: list[str] | list[uuid.UUID] | None,
                               assignment_scope_ids: list[str] | list[uuid.UUID] | None,
                               user_ids: list[str] | list[uuid.UUID] | None,
                               labels: list[LabelSelector],
                               item_fields: list[Type[MappedColumn]],
                               item_type: AnyItemModelType,
                               db_engine: DatabaseEngineAsync) -> list[dict[str, bool | int | str | None]]:
    async with db_engine.session() as session:  # type: AsyncSession
        sub_queries = []
        if assignment_scope_ids is not None:
            label_selects = _get_label_selects(labels, Schema=Annotation)
            sub_queries.append(
                select(Assignment.item_id,
                       Assignment.user_id,
                       *label_selects)
                .join(Annotation, Annotation.assignment_id == Assignment.assignment_id, isouter=True)
                .where(Assignment.assignment_scope_id.in_(assignment_scope_ids),
                       Assignment.user_id.in_(user_ids))
                .group_by(Assignment.item_id, Assignment.user_id)
            )

        if bot_annotation_metadata_ids is not None:
            label_selects = _get_label_selects(labels, Schema=BotAnnotation)
            sub_queries.append(
                select(BotAnnotation.item_id,
                       literal(None, type_=UUID).label('user_id'),
                       *label_selects)
                .where(BotAnnotation.bot_annotation_metadata_id.in_(bot_annotation_metadata_ids))
                .group_by(BotAnnotation.item_id)
            )

        if len(sub_queries) > 1:
            label_query = union(*sub_queries).subquery('labels')
        elif len(sub_queries) == 1:
            label_query = sub_queries[0].subquery('labels')
        else:
            AssertionError('You need at least on subquery for labels!')

        stmt = select(label_query,
                      F.coalesce(User.username, 'RESOLVED').label('username'),
                      *item_fields) \
            .join(item_type) \
            .join(User, User.user_id == label_query.c.user_id) \
            .order_by(label_query.c.item_id, User.username)

        result = session.execute(stmt).mappings().all()

        return [dict(r) for r in result]
