from typing import Type, Sequence
from sqlalchemy import select, and_, not_, Select, ColumnExpressionArgument, func, Function
from sqlalchemy.orm import MappedColumn, aliased, InstrumentedAttribute, Session

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
    GenericItem
)
from nacsos_data.models.items import AcademicItemModel, LexisNexisItemModel, GenericItemModel, FullLexisNexisItemModel
from nacsos_data.models.nql import (
    NQLFilter,
    SetComparator,
    ComparatorExt,
    ImportFilter,
    AssignmentFilter,
    MetaFilter,
    FieldFilter,
    FieldFilters,
    SubQuery,
    LabelFilterMulti,
    LabelFilterInt,
    LabelFilterBool,
    AnnotationFilter,
    Field,
    NQLFilterParser
)
from nacsos_data.db.crud.items.lexis_nexis import lexis_orm_to_model


class InvalidNQLError(Exception):
    pass


def _field_cmp(cmp: ComparatorExt, value: int | float | bool | str,
               field: InstrumentedAttribute | Function) -> ColumnExpressionArgument:  # type: ignore[type-arg]
    if cmp == '>':
        return field > value  # type: ignore[no-any-return]
    if cmp == '>=':
        return field >= value  # type: ignore[no-any-return]
    if cmp == '=':
        return field == value  # type: ignore[no-any-return]
    if cmp == '<':
        return field < value  # type: ignore[no-any-return]
    if cmp == '<=':
        return field <= value  # type: ignore[no-any-return]
    if cmp == '!=':
        return field != value  # type: ignore[no-any-return]
    if cmp == 'LIKE':
        return field.ilike(f'{value}')
    if cmp == 'SIMILAR':
        raise NotImplementedError('Unfortunately, "SIMILAR" is not implemented yet.')

    raise InvalidNQLError(f'Unexpected comparator "{cmp}".')


def _field_cmp_lst(cmp: SetComparator, values: list[int],
                   field: MappedColumn) -> ColumnExpressionArgument:  # type: ignore[type-arg]
    if cmp == '==':
        return field == values  # type: ignore[no-any-return]
    if cmp == '@>':
        return field.contains(values)  # type: ignore[no-any-return]
    if cmp == '!>':
        return not_(field.overlap(values))  # type: ignore[no-any-return,attr-defined]
    if cmp == '&&':
        return field.overlap(values)  # type: ignore[no-any-return,attr-defined]

    raise InvalidNQLError(f'Unexpected comparator "{cmp}".')


class NQLQuery:
    def __init__(self, query: NQLFilter, project_id: str,
                 project_type: ItemType | str = ItemType.academic):
        self.project_id = project_id
        self.project_type = project_type

        self.query = query

        if project_type == ItemType.academic:
            self.Schema = AcademicItem
            self.Model = AcademicItemModel
            self._stmt = (
                select(AcademicItem)
                .distinct(AcademicItem.item_id)
            )
        elif project_type == ItemType.lexis:
            self.Schema = LexisNexisItem  # type: ignore[assignment]
            self.Model = LexisNexisItemModel  # type: ignore[assignment]
            self._stmt = (
                select(LexisNexisItem,  # type: ignore[assignment]
                       func.array_agg(
                           func.row_to_json(
                               LexisNexisItemSource.__table__.table_valued()  # type: ignore[attr-defined]
                           )
                       ).label('sources'))
                .join(LexisNexisItemSource, LexisNexisItemSource.item_id == LexisNexisItem.item_id)
                .group_by(LexisNexisItem.item_id, Item.item_id)
            )
        elif project_type == ItemType.generic:
            self.Schema = GenericItem  # type: ignore[assignment]
            self.Model = GenericItemModel  # type: ignore[assignment]
            self._stmt = (
                select(GenericItem)  # type: ignore[assignment]
                .distinct(GenericItem.item_id)
            )
        else:
            raise NotImplementedError(f"Can't use NQL for {project_type} yet.")

        filters = self._assemble_filters(self.query)
        filters = and_(self.Schema.project_id == self.project_id, filters)
        self._stmt = self._stmt.where(filters)

    def __str__(self) -> str:
        return str(self.query)

    @property
    def stmt(self) -> Select:  # type: ignore[type-arg]
        return self._stmt

    def _get_column(self, field: Field) -> InstrumentedAttribute | Function:  # type: ignore[type-arg]
        if self.project_type == ItemType.academic:
            # TODO: include AcademicItemVariant
            if field == 'title':
                return AcademicItem.title
            if field == 'abstract':
                return AcademicItem.text
            if field == 'pub_year':
                return AcademicItem.publication_year
            if field == 'source':
                return AcademicItem.source
            if field == 'item_id':
                return AcademicItem.item_id
            if field == 'openalex_id':
                return AcademicItem.openalex_id
            if field == 'doi':
                return AcademicItem.doi
            if field == 'meta':
                return AcademicItem.meta
        elif self.project_type == ItemType.lexis:
            if field == 'title':
                return LexisNexisItemSource.title
            if field == 'abstract':
                return LexisNexisItem.text
            if field == 'date':
                return LexisNexisItemSource.published_at
            if field == 'pub_year':
                return func.year(LexisNexisItemSource.published_at)
            if field == 'source':
                return LexisNexisItemSource.name
            if field == 'item_id':
                return LexisNexisItemSource.item_id
            if field == 'meta':
                return LexisNexisItemSource.meta
        elif self.project_type == ItemType.generic:
            if field == 'abstract':
                return GenericItem.text
            if field == 'meta':
                return GenericItem.meta

        raise InvalidNQLError(f'Field "{field}" in {self.project_type} is not valid.')

    def _assemble_filters(self, subquery: NQLFilter) -> ColumnExpressionArgument:  # type: ignore[type-arg]
        if isinstance(subquery, SubQuery):
            if subquery.and_ is not None:
                return and_(*(self._assemble_filters(child) for child in subquery.and_))
            if subquery.or_ is not None:
                return and_(*(self._assemble_filters(child) for child in subquery.or_))
            raise InvalidNQLError('Missing subquery!')

        elif isinstance(subquery, FieldFilter):
            col = self._get_column(subquery.field)
            return _field_cmp(subquery.comp, subquery.value, col)
        elif isinstance(subquery, FieldFilters):
            col = self._get_column(subquery.field)
            return col.in_(subquery.values)
        elif isinstance(subquery, MetaFilter):
            raise NotImplementedError('Meta fields cannot be filtered yet.')

        elif isinstance(subquery, LabelFilterMulti):
            return self._label_filter(subquery)
        elif isinstance(subquery, LabelFilterInt):
            return self._label_filter(subquery)
        elif isinstance(subquery, LabelFilterBool):
            return self._label_filter(subquery)

        elif isinstance(subquery, AssignmentFilter):
            if subquery.mode == 1:
                self._stmt = self.stmt.join(Assignment, Assignment.item_id == self.Schema.item_id)
                return Assignment.item_id != None  # noqa: E711
            if subquery.mode == 2:
                if subquery.scopes is None:
                    raise InvalidNQLError('No scopes defined!')
                self._stmt = self.stmt.join(Assignment, and_(Assignment.item_id == self.Schema.item_id,
                                                             Assignment.assignment_scope_id.in_(subquery.scopes)))
                return Assignment.item_id != None  # noqa: E711
            if subquery.mode == 3:
                if subquery.scopes is None:
                    raise InvalidNQLError('No scopes defined!')
                raise NotImplementedError('"IS ASSIGNED BUT NOT IN" filter not implemented yet')
            if subquery.mode == 4:
                self._stmt = self.stmt.join(Assignment, Assignment.item_id == self.Schema.item_id, isouter=True)
                return Assignment.item_id == None  # noqa: E711
            if subquery.mode == 5:
                if subquery.scopes is None:
                    raise InvalidNQLError('No scopes defined!')
                self._stmt = self.stmt.join(Assignment, and_(Assignment.item_id == self.Schema.item_id,
                                                             Assignment.assignment_scope_id.in_(subquery.scopes)),
                                            isouter=True)
                return Assignment.item_id == None  # noqa: E711
            raise InvalidNQLError(f'Invalid mode in {subquery}')

        elif isinstance(subquery, AnnotationFilter):
            raise NotImplementedError('"HAS ANNOTATION ..." filter not implemented yet')

        elif isinstance(subquery, ImportFilter):
            self._stmt = self.stmt.join(m2m_import_item_table, m2m_import_item_table.c.item_id == Item.item_id)
            included = [iid.uuid for iid in subquery.import_ids if iid.incl]
            excluded = [iid.uuid for iid in subquery.import_ids if not iid.incl]
            wheres = []
            if len(included) > 0:
                wheres.append(m2m_import_item_table.c.import_id.in_(included))
            if len(excluded) > 0:
                wheres.append(m2m_import_item_table.c.import_id.notin_(excluded))
            return and_(*tuple(wheres))

        raise InvalidNQLError(f'Not sure what to do with this: {subquery}!')

    def _label_filter(self, subquery: LabelFilterMulti | LabelFilterBool | LabelFilterInt) \
            -> ColumnExpressionArgument:  # type: ignore[type-arg]
        Annotation_: Type[Annotation | BotAnnotation] = Annotation if subquery.type == 'user' else BotAnnotation

        def _value_where(Alias):  # type: ignore[no-untyped-def]
            if isinstance(subquery, LabelFilterBool):
                if subquery.value_bool is None:
                    raise InvalidNQLError('Missing value!')
                return Alias.value_bool == subquery.value_bool  # type: ignore[union-attr]
            if isinstance(subquery, LabelFilterInt):
                if subquery.value_int is None:
                    raise InvalidNQLError('Missing value!')
                return _field_cmp(subquery.comp, subquery.value_int, Alias.value_int)
            if isinstance(subquery, LabelFilterMulti):
                if subquery.multi_int is None:
                    raise InvalidNQLError('Missing value!')
                return _field_cmp_lst(subquery.comp, subquery.multi_int, Alias.multi_int)
            raise ValueError('Unexpected annotation label value filter clause.')

        def _inner_where(Schema) -> Sequence[ColumnExpressionArgument]:  # type: ignore[type-arg,no-untyped-def]
            inner_wheres = (Schema.key == subquery.key,
                            _value_where(Schema))
            if subquery.repeats is not None:
                Schema.repeats.in_(subquery.repeats)

            if subquery.type == 'resolved':
                if subquery.scopes is not None:
                    inner_wheres += (and_(  # type: ignore[assignment]
                        Schema.bot_annotation_metadata_id.in_(subquery.scopes),
                        Schema.bot_annotation_metadata_id == BotAnnotationMetaData.bot_annotation_metadata_id,
                        BotAnnotationMetaData.kind == 'RESOLVE'),)
                else:
                    inner_wheres += (and_(  # type: ignore[unreachable]
                        Schema.bot_annotation_metadata_id == BotAnnotationMetaData.bot_annotation_metadata_id,
                        BotAnnotationMetaData.kind == 'RESOLVE'),)
            elif subquery.type == 'bot':
                if subquery.scopes is not None:
                    inner_wheres += (and_(  # type: ignore[assignment]
                        BotAnnotation.bot_annotation_metadata_id.in_(subquery.scopes),
                        Schema.bot_annotation_metadata_id == BotAnnotationMetaData.bot_annotation_metadata_id,
                        BotAnnotationMetaData.kind != 'RESOLVE'),)
                else:
                    inner_wheres += (and_(  # type: ignore[unreachable]
                        Schema.bot_annotation_metadata_id == BotAnnotationMetaData.bot_annotation_metadata_id,
                        BotAnnotationMetaData.kind != 'RESOLVE'),)
            elif subquery.type == 'user':
                if subquery.scopes is not None:
                    self._stmt = self.stmt.join(Assignment, Assignment.assignment_id == Schema.assignment_id)
                    inner_wheres += (Assignment.assignment_scope_id.in_(subquery.scopes),)  # type: ignore[assignment]

            return inner_wheres

        if subquery.users is not None:
            if subquery.type != 'user':
                raise InvalidNQLError('You cannot filter by users for BotAnnotations!')

            if subquery.users.mode == 'ANY':
                AnnotationAlias = aliased(Annotation_)
                self._stmt = self._stmt.join(
                    AnnotationAlias,
                    self.Schema.item_id == AnnotationAlias.item_id  # type: ignore[attr-defined]
                )
                return and_(AnnotationAlias.user_id.in_(subquery.users.user_ids),  # type: ignore[attr-defined]
                            and_(*_inner_where(AnnotationAlias)))

            elif subquery.users.mode == 'ALL':
                wheres = []
                for user in subquery.users.user_ids:
                    AnnotationAlias = aliased(Annotation_)
                    self._stmt = self._stmt.join(
                        AnnotationAlias,
                        self.Schema.item_id == AnnotationAlias.item_id)  # type: ignore[attr-defined]
                    _wheres = _inner_where(AnnotationAlias)
                    _wheres += (AnnotationAlias.user_id == user,)  # type: ignore[attr-defined, operator]
                    wheres.append(and_(*_wheres))
                return and_(*wheres)
            else:
                raise ValueError(f'Unexpected mode {subquery.users.mode}')

        else:
            AnnotationAlias = aliased(Annotation_)  # type: ignore[unreachable]
            self._stmt = self._stmt.join(AnnotationAlias,
                                         self.Schema.item_id == AnnotationAlias.item_id)  # type: ignore[attr-defined]
            return and_(*_inner_where(AnnotationAlias))

    def count(self, session: Session) -> int:
        stmt = self.stmt.subquery()
        cnt_stmt = func.count(stmt.c.item_id)
        return session.execute(cnt_stmt).scalar()  # type: ignore[return-value]

    def results(self, session: Session, limit: int | None = 20, offset: int | None = None) \
            -> list[FullLexisNexisItemModel] | list[AcademicItemModel] | list[GenericItemModel]:
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

        rslt = session.execute(stmt).scalars().all()

        if self.project_type == ItemType.lexis:
            return lexis_orm_to_model(rslt)
        elif self.project_type == ItemType.academic:
            return [AcademicItemModel.model_validate(item.__dict__) for item in rslt]
        elif self.project_type == ItemType.generic:
            return [GenericItemModel.model_validate(item.__dict__) for item in rslt]
        else:
            raise NotImplementedError(f'Unexpected project type: {self.project_type}')


def query_to_sql(query: NQLFilter, project_id: str) -> Select:  # type: ignore[type-arg]
    query_object = NQLQuery(query=query, project_id=project_id)
    return query_object.stmt


def nql_to_sql(query: NQLFilter, project_id: str) -> Select:  # type: ignore[type-arg]
    return query_to_sql(query=query, project_id=project_id)


__all__ = ['query_to_sql', 'nql_to_sql', 'NQLFilter', 'NQLFilterParser', 'InvalidNQLError', 'NQLQuery']