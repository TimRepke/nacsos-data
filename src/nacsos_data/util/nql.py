from typing import Type, Sequence
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import MappedColumn, InstrumentedAttribute, Session

from nacsos_data.db.engine import DBSession
from nacsos_data.db.schemas import (
    AcademicItem,
    Annotation,
    BotAnnotation,
    BotAnnotationMetaData,
    Assignment,
    m2m_import_item_table,
    ItemType,
    LexisNexisItem,
    Item,
    LexisNexisItemSource,
    GenericItem,
    Project,
    AnyItemSchema,
)
from nacsos_data.models.items import AcademicItemModel, LexisNexisItemModel, GenericItemModel, FullLexisNexisItemModel, AnyItemModel
from nacsos_data.models.nql import (
    NQLFilter,
    SetComparator,
    ComparatorExt,
    ImportFilter,
    AssignmentFilter,
    FieldFilter,
    FieldFilters,
    SubQuery,
    LabelFilterMulti,
    LabelFilterInt,
    LabelFilterBool,
    AnnotationFilter,
    Field,
    NQLFilterParser,
    MetaFilterBool,
    MetaFilterInt,
    MetaFilterStr,
    AbstractFilter,
)
from nacsos_data.db.crud.items.lexis_nexis import lexis_orm_to_model


class InvalidNQLError(Exception):
    pass


def _field_cmp(cmp: ComparatorExt, value: int | float | bool | str, field: InstrumentedAttribute | sa.Function) -> sa.ColumnExpressionArgument:  # type: ignore[type-arg]
    if cmp == '>':
        return sa.and_(field > value, field.isnot(None))
    if cmp == '>=':
        return sa.and_(field >= value, field.isnot(None))
    if cmp == '=':
        return sa.and_(field == value, field.isnot(None))
    if cmp == '<':
        return sa.and_(field < value, field.isnot(None))
    if cmp == '<=':
        return sa.and_(field <= value, field.isnot(None))
    if cmp == '!=':
        return sa.and_(field != value, field.isnot(None))
    if cmp == 'LIKE':
        return sa.and_(field.ilike(f'%{value}%'), field.isnot(None))
    if cmp == 'SIMILAR':
        raise NotImplementedError('Unfortunately, "SIMILAR" is not implemented yet.')

    raise InvalidNQLError(f'Unexpected comparator "{cmp}".')


def _field_cmp_lst(cmp: SetComparator, values: list[int], field: MappedColumn) -> sa.ColumnExpressionArgument:  # type: ignore[type-arg]
    if cmp == '==':
        return sa.and_(field == values, field.isnot(None))
    if cmp == '@>':
        return sa.and_(field.contains(values), field.isnot(None))
    if cmp == '!>':
        return sa.and_(sa.not_(field.overlap(values)), field.isnot(None))  # type: ignore[attr-defined]
    if cmp == '&&':
        return sa.and_(field.overlap(values), field.isnot(None))  # type: ignore[attr-defined]

    raise InvalidNQLError(f'Unexpected comparator "{cmp}".')


def get_select_base(project_type: ItemType | str = ItemType.academic) -> tuple[Type[AnyItemSchema], Type[AnyItemModel], sa.Select]:  # type: ignore[type-arg]
    if project_type == ItemType.academic:
        return (  # type: ignore[return-value]
            AcademicItem,
            AcademicItemModel,
            sa.select(AcademicItem).distinct(AcademicItem.item_id),
        )
    if project_type == ItemType.lexis:
        return (  # type: ignore[return-value]
            LexisNexisItem,
            LexisNexisItemModel,
            sa.select(
                LexisNexisItem,
                sa.func.array_agg(
                    sa.func.row_to_json(
                        LexisNexisItemSource.__table__.table_valued(),  # type: ignore[attr-defined]
                    ),
                ).label('sources_grp'),
            )
            .join(LexisNexisItemSource, LexisNexisItemSource.item_id == LexisNexisItem.item_id)
            .group_by(LexisNexisItem.item_id, Item.item_id),
        )
    if project_type == ItemType.generic:
        return (  # type: ignore[return-value]
            GenericItem,
            GenericItemModel,
            sa.select(GenericItem).distinct(GenericItem.item_id),
        )

    raise NotImplementedError(f"Can't use NQL for {project_type} yet.")


class NQLQuery:
    def __init__(self, project_id: str, query: NQLFilter | None = None, project_type: ItemType | str = ItemType.academic):
        self.project_id = project_id
        self.project_type = project_type

        self.query = query
        self.Schema, self.Model, self._stmt = get_select_base(project_type=project_type)
        self._project_items = sa.select(Item.item_id).where(Item.project_id == project_id).cte('project_items')

        if query is None:
            self._stmt = self._stmt.where(self._stmt.c.project_id == project_id)
        else:
            filter_cte = self._assemble_filters(query)
            self._stmt = (
                sa.select(AcademicItem)
                .where(AcademicItem.project_id == project_id)
                .join(self._project_items, self._project_items.c.item_id == AcademicItem.item_id)
                .join(filter_cte, filter_cte.c.item_id == self._project_items.c.item_id)
            )

            # self._stmt = self._stmt.join(filter_cte, filter_cte.c.item_id==self._stmt.c.item_id).where(self._stmt.c.project_id==project_id)

    def __str__(self) -> str:
        return str(self.query)

    @classmethod
    async def get_query(
        cls,
        session: DBSession | AsyncSession,
        project_id: str,
        project_type: ItemType | None = None,
        query: NQLFilter | None = None,
    ) -> 'NQLQuery':
        if project_type is None:
            project_type = await session.scalar(sa.select(Project.type).where(Project.project_id == project_id))

            if project_type is None:
                raise KeyError(f'Found no matching project for {project_id}. This should NEVER happen!')

        return cls(query=query, project_id=str(project_id), project_type=project_type)

    @property
    def stmt(self) -> sa.Select:  # type: ignore[type-arg]
        return self._stmt

    def _get_column(self, field: Field) -> tuple[Type[LexisNexisItemSource | Item | GenericItem | AcademicItem], InstrumentedAttribute]:  # type: ignore[type-arg]
        if field in {'abstract', 'text'}:
            return Item, Item.text
        if self.project_type == ItemType.academic:
            # TODO: include AcademicItemVariant
            return AcademicItem, getattr(AcademicItem, field)
        if self.project_type == ItemType.lexis:
            if field in {'pub_year', 'year', 'publication_year', 'py'}:
                return LexisNexisItemSource, sa.sql.extract('year', LexisNexisItemSource.published_at)  # type: ignore[return-value]
            if field == 'date':
                return LexisNexisItemSource, LexisNexisItemSource.published_at
            if field == 'source':
                return LexisNexisItemSource, LexisNexisItemSource.name
            return LexisNexisItemSource, getattr(LexisNexisItemSource, field)
        if self.project_type == ItemType.generic:
            if field == 'meta':
                return GenericItem, GenericItem.meta

        raise InvalidNQLError(f'Field "{field}" in {self.project_type} is not valid.')

    def _assemble_filters(self, subquery: NQLFilter) -> sa.CTE:  # noqa: C901
        if isinstance(subquery, SubQuery):
            #  | query __ AND __ query             {% (d) => ({ filter: "sub", "and_": [d[0], d[4]]            }) %}
            #  | query __ OR  __ query             {% (d) => ({ filter: "sub", "or_":  [d[0], d[4]]            }) %}
            #  | "NOT" __ query                    {% (d) => ({ filter: "sub", "not_": d[2]                    }) %}
            if subquery.not_ is not None:
                children = [self._assemble_filters(subquery.not_)]
            elif subquery.and_ is not None:
                children = [self._assemble_filters(child) for child in subquery.and_]
            elif subquery.or_ is not None:
                children = [self._assemble_filters(child) for child in subquery.or_]
            else:
                raise InvalidNQLError('Missing subquery!')

            query = sa.select(self._project_items)
            for child in children:
                query = query.join(child, self._project_items.c.item_id == child.c.item_id, isouter=True)

            if subquery.not_ is not None:
                query = query.where(children[0].c.item_id.is_(None))
            elif subquery.and_ is not None:
                query = query.where(sa.and_(*(child.c.item_id.isnot(None) for child in children)))
            elif subquery.or_ is not None:
                query = query.where(sa.or_(*(child.c.item_id.isnot(None) for child in children)))

            return query.cte()

        if isinstance(subquery, FieldFilter):
            #     TITLE    ":" _ dqstring           {% (d) => ({ filter: "field", field: "title",       value:  d[3]               }) %}
            #   | ABSTRACT ":" _ dqstring           {% (d) => ({ filter: "field", field: "abstract",    value:  d[3]               }) %}
            #   | PYEAR    ":" _ COMP _ year        {% (d) => ({ filter: "field", field: "pub_year",    value:  d[5], comp: d[3]   }) %}
            #   | PDATE    ":" _ COMP _ date        {% (d) => ({ filter: "field", field: "date",        value:  d[5], comp: d[3]   }) %}
            #   | SRC      ":" _ dqstring           {% (d) => ({ filter: "field", field: "source",      value:  d[3], comp: "LIKE" }) %}
            schema, col = self._get_column(subquery.field)
            comp = subquery.comp
            if subquery.field in {'title', 'text', 'abstract'}:
                comp = 'LIKE'
            if comp is None:
                raise InvalidNQLError(f'Missing comparator: {subquery}!')
            return (
                sa.select(sa.distinct(self._project_items.c.item_id).label('item_id'))
                .join(schema, schema.item_id == self._project_items.c.item_id)
                .where(_field_cmp(comp, subquery.value, col))
                .cte()
            )

        elif isinstance(subquery, FieldFilters):
            #   | "DOI"i   ":" _ dois               {% (d) => ({ filter: "field_mul", field: "doi",         values: d[3]               }) %}
            #   | "OA"i    ":" _ oa_ids             {% (d) => ({ filter: "field_mul", field: "openalex_id", values: d[3]               }) %}
            #   | "ID"i    ":" _ uuids              {% (d) => ({ filter: "field_mul", field: "item_id",     values: d[3]               }) %}
            if subquery.values is None:
                raise InvalidNQLError('Missing values!')
            schema, col = self._get_column(subquery.field)
            return (
                sa.select(sa.distinct(self._project_items.c.item_id).label('item_id'))
                .join(schema, schema.item_id == self._project_items.c.item_id)
                .where(col.in_(subquery.values))
                .cte()
            )

        elif isinstance(subquery, MetaFilterBool) or isinstance(subquery, MetaFilterInt) or isinstance(subquery, MetaFilterStr):
            #     KEY _  "="      _  bool      {% (d) => ({ filter: "meta_bool", value_type: "bool", field: d[0], comp: "=",    value: d[4] }) %}
            #   | KEY _  COMP     _  uint      {% (d) => ({ filter: "meta_int",  value_type: "int",  field: d[0], comp: d[2],   value: d[4] }) %}
            #   | KEY __ "LIKE"i  __ dqstring  {% (d) => ({ filter: "meta_str",  value_type: "str",  field: d[0], comp: "LIKE", value: d[4] }) %}
            schema, col = self._get_column('meta')
            query = sa.select(sa.distinct(self._project_items.c.item_id).label('item_id')).join(schema, schema.item_id == self._project_items.c.item_id)
            if isinstance(subquery, MetaFilterBool):
                return query.where(col[subquery.field].as_boolean() == bool(subquery.value)).cte()
            if isinstance(subquery, MetaFilterInt):
                return query.where(col[subquery.field].as_integer() == subquery.value).cte()
            if isinstance(subquery, MetaFilterStr):
                return query.where(sa.and_(col[subquery.field].isnot(None), col[subquery.field].as_string().ilike(f'%{subquery.value}%'))).cte()

        elif isinstance(subquery, LabelFilterMulti) or isinstance(subquery, LabelFilterInt) or isinstance(subquery, LabelFilterBool):
            return self._label_filter(subquery)

        elif isinstance(subquery, AssignmentFilter):
            #     "IS ASSIGNED"i                        {% (d) => ({ filter: "assignment", mode: 1,              }) %}
            #   | "IS ASSIGNED IN"i           __ uuids  {% (d) => ({ filter: "assignment", mode: 2, scopes: d[2] }) %}
            #   | "IS ASSIGNED BUT NOT IN"i   __ uuids  {% (d) => ({ filter: "assignment", mode: 3, scopes: d[2] }) %}
            #   | "IS NOT ASSIGNED"i                    {% (d) => ({ filter: "assignment", mode: 4               }) %}
            #   | "IS NOT ASSIGNED IGNORING"i __ uuids  {% (d) => ({ filter: "assignment", mode: 5, scopes: d[2] }) %}
            #   | "IS ASSIGNED WITH"i         __ UUID   {% (d) => ({ filter: "assignment", mode: 6, scheme: d[2] }) %}
            #   | "IS ASSIGNED BUT NOT WITH"i __ UUID   {% (d) => ({ filter: "assignment", mode: 7, scheme: d[2] }) %}
            if subquery.mode == 1:
                return (
                    sa.select(sa.distinct(self._project_items.c.item_id).label('item_id')).join(Assignment, Assignment.item_id == self._project_items.c.item_id)
                ).cte()

            if subquery.mode == 1:
                if subquery.scopes is None:
                    raise InvalidNQLError('No scopes defined!')
                return (
                    sa.select(sa.distinct(self._project_items.c.item_id).label('item_id')).join(
                        Assignment,
                        sa.and_(Assignment.item_id == self._project_items.c.item_id, Assignment.assignment_scope_id.in_(subquery.scopes)),
                    )
                ).cte()

            if subquery.mode == 3:
                if subquery.scopes is None:
                    raise InvalidNQLError('No scopes defined!')
                raise NotImplementedError('"IS ASSIGNED BUT NOT IN" filter not implemented yet')

            if subquery.mode == 4:
                return (
                    sa.select(sa.distinct(self._project_items.c.item_id).label('item_id'))
                    .join(Assignment, Assignment.item_id == self._project_items.c.item_id, isouter=True)
                    .where(Assignment.item_id.is_(None))  # noqa: E711
                ).cte()

            if subquery.mode == 5:
                if subquery.scopes is None:
                    raise InvalidNQLError('No scopes defined!')
                return (
                    sa.select(sa.distinct(self._project_items.c.item_id).label('item_id'))
                    .join(Assignment, sa.and_(Assignment.item_id == self._project_items.c.item_id, Assignment.assignment_scope_id.notin_(subquery.scopes)))
                    .where(Assignment.item_id.is_(None))  # noqa: E711
                ).cte()

            if subquery.mode == 6:
                if subquery.scheme is None:
                    raise InvalidNQLError('No scheme defined!')
                return (
                    sa.select(sa.distinct(self._project_items.c.item_id).label('item_id')).join(
                        Assignment,
                        sa.and_(Assignment.item_id == self._project_items.c.item_id, Assignment.annotation_scheme_id == subquery.scheme),
                    )
                ).cte()

            if subquery.mode == 7:
                if subquery.scheme is None:
                    raise InvalidNQLError('No scheme defined!')
                return (
                    sa.select(sa.distinct(self._project_items.c.item_id).label('item_id')).join(
                        Assignment,
                        sa.and_(Assignment.item_id == self._project_items.c.item_id, Assignment.annotation_scheme_id != subquery.scheme),
                    )
                ).cte()

        elif isinstance(subquery, AnnotationFilter):
            if subquery.scheme is not None:
                query = (
                    sa.select(sa.distinct(self._project_items.c.item_id).label('item_id'))
                    .join(
                        Assignment,
                        sa.and_(Assignment.annotation_scheme_id == subquery.scheme, Assignment.item_id == self._project_items.c.item_id),
                        isouter=True,
                    )
                    .join(Annotation, Annotation.assignment_id == Annotation.assignment_id, isouter=True)
                )
            elif subquery.scopes is not None:
                query = (
                    sa.select(sa.distinct(self._project_items.c.item_id).label('item_id'))
                    .join(
                        Assignment,
                        sa.and_(Assignment.assignment_scope_id.in_(subquery.scopes), Assignment.item_id == self._project_items.c.item_id),
                        isouter=True,
                    )
                    .join(Annotation, Annotation.assignment_id == Assignment.assignment_id, isouter=True)
                )
            else:
                query = sa.select(sa.distinct(self._project_items.c.item_id).label('item_id')).join(
                    Annotation,
                    Annotation.item_id == self._project_items.c.item_id,
                    isouter=True,
                )
            if subquery.incl:
                return query.where(Annotation.item_id.isnot(None)).cte()  # noqa: E711
            return query.where(Annotation.item_id.is_(None)).cte()  # noqa: E711

        elif isinstance(subquery, AbstractFilter):
            query = sa.select(Item.item_id).where(Item.project_id == self.project_id)
            if subquery.comp is not None and subquery.size is not None:
                return query.where(sa.and_(Item.text.isnot(None), _field_cmp(subquery.comp, subquery.size, sa.func.char_length(Item.text)))).cte()
            elif subquery.empty is True:
                return query.where(sa.and_(Item.text.is_(None))).cte()
            elif subquery.empty is False:
                return query.where(sa.and_(Item.text.isnot(None))).cte()

        elif isinstance(subquery, ImportFilter):
            includes = (
                sa.select(sa.distinct(m2m_import_item_table.c.item_id).label('item_id'))
                .where(m2m_import_item_table.c.import_id.in_([iid.uuid for iid in subquery.import_ids if iid.incl]))
                .alias()
            )
            excludes = (
                sa.select(sa.distinct(m2m_import_item_table.c.item_id).label('item_id'))
                .where(m2m_import_item_table.c.import_id.in_([iid.uuid for iid in subquery.import_ids if not iid.incl]))
                .alias()
            )
            return (
                sa.select(sa.distinct(self._project_items.c.item_id).label('item_id'))
                .join(includes, includes.c.item_id == self._project_items.c.item_id, isouter=True)
                .join(excludes, excludes.c.item_id == self._project_items.c.item_id, isouter=True)
                .where(sa.and_(includes.c.item_id.isnot(None), excludes.c.item_id.is_(None)))  # noqa: E711
                .cte()
            )

        raise InvalidNQLError(f'Not sure what to do with this: {subquery}!')

    def _label_filter(self, subquery: LabelFilterMulti | LabelFilterBool | LabelFilterInt) -> sa.CTE:  # noqa: C901

        def _value_where(annotation: Type[Annotation | BotAnnotation]) -> sa.ColumnExpressionArgument | MappedColumn:  # type: ignore[type-arg]
            if isinstance(subquery, LabelFilterBool):
                if subquery.value_bool is None:
                    raise InvalidNQLError('Missing value!')
                return annotation.value_bool == subquery.value_bool
            if isinstance(subquery, LabelFilterInt):
                if subquery.value_int is None:
                    raise InvalidNQLError('Missing value!')
                return _field_cmp(subquery.comp, subquery.value_int, annotation.value_int)
            if isinstance(subquery, LabelFilterMulti):
                if subquery.multi_int is None:
                    raise InvalidNQLError('Missing value!')
                return _field_cmp_lst(subquery.comp, subquery.multi_int, annotation.multi_int)  # type: ignore[arg-type]
            raise ValueError('Unexpected annotation label value filter clause.')

        if subquery.type in {'resolved', 'bot'}:
            if subquery.users is not None:
                raise InvalidNQLError('You cannot filter by users for BotAnnotations!')
            query = (
                sa.select(sa.distinct(self._project_items.c.item_id).label('item_id'))
                .join(
                    BotAnnotation,
                    sa.and_(BotAnnotation.item_id == self._project_items.c.item_id, BotAnnotation.key == subquery.key, _value_where(BotAnnotation)),
                )
                .join(
                    BotAnnotationMetaData,
                    sa.and_(
                        BotAnnotationMetaData.bot_annotation_metadata_id == BotAnnotation.bot_annotation_metadata_id,
                        BotAnnotationMetaData.kind == 'RESOLVE' if subquery.type == 'resolved' else BotAnnotationMetaData.kind != 'RESOLVE',
                    ),
                )
            )
            if subquery.repeats is not None:
                query = query.where(BotAnnotation.repeat.in_(subquery.repeats))
            if subquery.scopes is not None:
                return query.where(BotAnnotation.bot_annotation_metadata_id.in_(subquery.scopes)).cte()
            if subquery.scheme is not None:
                return query.where(BotAnnotationMetaData.annotation_scheme_id == subquery.scheme).cte()
            return query.cte()
        else:

            def _annotation() -> sa.Select:  # type: ignore[type-arg]
                _query = (
                    sa.select(sa.distinct(Annotation.item_id).label('item_id'))
                    .join(self._project_items, Annotation.item_id == self._project_items.c.item_id)
                    .where(sa.and_(Annotation.key == subquery.key, _value_where(Annotation)))
                )
                if subquery.repeats is not None:
                    return _query.where(Annotation.repeat.in_(subquery.repeats))
                if subquery.scheme is not None:
                    return _query.where(Annotation.annotation_scheme_id == subquery.scheme)
                if subquery.scopes is not None:
                    return _query.join(
                        Assignment, sa.and_(Assignment.assignment_id == Annotation.assignment_id, Assignment.assignment_scope_id.in_(subquery.scopes))
                    )
                return _query

            if subquery.users is not None and subquery.users.mode == 'ANY':
                return _annotation().where(Annotation.user_id.in_(subquery.users.user_ids)).cte()
            if subquery.users is not None and subquery.users.mode == 'ALL':
                query = sa.select(sa.distinct(self._project_items.c.item_id).label('item_id'))
                for user in subquery.users.user_ids:
                    user_annotations = _annotation().where(Annotation.user_id == user).alias()
                    query = query.join(user_annotations, user_annotations.c.item_id == self._project_items.c.item_id, isouter=True).where(
                        user_annotations.c.item_id.isnot(None)
                    )
                return query.cte()
            return _annotation().cte()

    def count(self, session: Session) -> int:
        stmt = self.stmt.subquery()
        cnt_stmt = sa.func.count(stmt.c.item_id)
        return session.execute(cnt_stmt).scalar()  # type: ignore[return-value]

    def _transform_results(self, rslt: Sequence[sa.RowMapping]) -> list[FullLexisNexisItemModel] | list[AcademicItemModel] | list[GenericItemModel]:
        if self.project_type == ItemType.lexis:
            return lexis_orm_to_model(rslt)
        elif self.project_type == ItemType.academic:
            return [AcademicItemModel.model_validate(item['AcademicItem'].__dict__) for item in rslt]
        elif self.project_type == ItemType.generic:
            return [GenericItemModel.model_validate(item['GenericItem'].__dict__) for item in rslt]
        else:
            raise NotImplementedError(f'Unexpected project type: {self.project_type}')

    def results(
        self,
        session: Session,
        limit: int | None = 20,
        offset: int | None = None,
    ) -> list[FullLexisNexisItemModel] | list[AcademicItemModel] | list[GenericItemModel]:
        """
        Query the database for results (mappings) either from an existing `session`.
        :param session:
        :param limit: how many results to return
        :param offset:
        :return:
        """
        stmt = self.stmt
        if limit is not None:
            stmt = stmt.limit(limit)

        if offset is not None:
            stmt = stmt.offset(offset)

        rslt = session.execute(stmt).mappings().all()
        return self._transform_results(rslt)

    async def results_async(
        self,
        session: AsyncSession | DBSession,
        limit: int | None = 20,
        offset: int | None = None,
    ) -> list[FullLexisNexisItemModel] | list[AcademicItemModel] | list[GenericItemModel]:
        stmt = self.stmt
        if limit is not None:
            stmt = stmt.limit(limit)

        if offset is not None:
            stmt = stmt.offset(offset)

        rslt = (await session.execute(stmt)).mappings().all()
        return self._transform_results(rslt)


def query_to_sql(query: NQLFilter, project_id: str, project_type: ItemType | str = ItemType.academic) -> sa.Select:  # type: ignore[type-arg]
    query_object = NQLQuery(query=query, project_id=project_id, project_type=project_type)
    return query_object.stmt


def nql_to_sql(query: NQLFilter, project_id: str, project_type: ItemType | str = ItemType.academic) -> sa.Select:  # type: ignore[type-arg]
    return query_to_sql(query=query, project_id=project_id, project_type=project_type)


__all__ = ['query_to_sql', 'nql_to_sql', 'NQLFilter', 'NQLFilterParser', 'InvalidNQLError', 'NQLQuery', 'get_select_base']
