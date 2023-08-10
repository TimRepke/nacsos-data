from typing import Literal, Type, Sequence
from uuid import UUID

from lark import Tree, Token
from sqlalchemy import select, and_, or_, not_, Select, ColumnExpressionArgument
from sqlalchemy.orm import MappedColumn, aliased, Session
import sqlalchemy.sql.functions as func

from nacsos_data.db import DatabaseEngine, DatabaseEngineAsync
from nacsos_data.db.schemas import (
    AcademicItem,
    Import,
    Annotation,
    BotAnnotation,
    BotAnnotationMetaData, Assignment
)
from nacsos_data.models.items import AcademicItemModel
from .parse import parse_str


def _fulltext_filter(clause: Tree | Token, Field: MappedColumn) -> ColumnExpressionArgument:
    if isinstance(clause, Token):
        if clause.type == 'WORD':
            return Field.ilike(f'%{clause.value}%')
        if clause.type == 'ESCAPED_STRING':
            return Field.ilike(f'%{clause.value[1:-1]}%')
    raise NotImplementedError('"Complex" title/abstract filtering not implemented, yet.')


def _field_cmp(cmp, val, Field: MappedColumn) -> ColumnExpressionArgument:
    if cmp == '>':
        return Field > val
    if cmp == '>=':
        return Field >= val
    if cmp == '=':
        return Field == val
    if cmp == '<':
        return Field < val
    if cmp == '<=':
        return Field <= val
    if cmp == '!=':
        return Field != val


def _field_cmp_clause(clause: Tree | Token, Field: MappedColumn,
                      value_clause: Literal['uint_clause', 'int_clause', 'float_clause']) \
        -> ColumnExpressionArgument:
    if isinstance(clause, Tree):
        if clause.data == value_clause:
            cmp = clause.children[0]
            val = clause.children[1]
            return _field_cmp(cmp, val, Field)
        elif clause.data == 'and':
            return and_(*(_field_cmp_clause(child, Field, value_clause) for child in clause.children))
        elif clause.data == 'or':
            return or_(*(_field_cmp_clause(child, Field, value_clause) for child in clause.children))
    raise ValueError(f'Unexpected: {clause}')


class Query:
    def __init__(self, query: str, project_id: str | UUID = None):
        self.project_id = project_id

        self.query = query
        self.query_tree: Tree = parse_str(query)

        self._stmt = select(AcademicItem).distinct(AcademicItem.item_id)
        self._stmt = self._to_sql()

    def __str__(self):
        return self.query

    @property
    def stmt(self) -> Select:
        return self._stmt

    @property
    def pretty(self) -> str:
        return self.query_tree.pretty()

    def count(self, db_engine: DatabaseEngine | None = None, session: Session | None = None) -> int:
        stmt = self.stmt.subquery()
        stmt = func.count(stmt.c.item_id)
        if db_engine is not None:
            with db_engine.session() as session:  # type: Session
                return session.execute(stmt).scalar()
        return session.execute(stmt).scalar()

    def results(self, db_engine: DatabaseEngine | None = None, session: Session | None = None,
                limit: int | None = 20) -> list[AcademicItemModel]:
        """
        Query the database for results (mappings) either from an existing `session` or connected `db_engine`.
        :param db_engine:
        :param session:
        :param limit:
        :return:
        """
        stmt = self.stmt
        if limit is not None:
            stmt = stmt.limit(limit)

        if db_engine is not None:
            with db_engine.session() as session:  # type: Session
                items = session.execute(stmt).scalars().all()
        else:
            items = session.execute(stmt).scalars().all()
        return [AcademicItemModel.model_validate(item.__dict__) for item in items]

    def _to_sql(self) -> Select:
        filters = self._assemble_filters(self.query_tree)
        if self.project_id is not None:
            filters = and_(AcademicItem.project_id == self.project_id, filters)
        self._stmt = self._stmt.where(filters)
        return self._stmt

    def _assemble_filters(self, subtree: Tree | Token) -> ColumnExpressionArgument:
        if isinstance(subtree, Tree):
            if subtree.data == 'title_filter':
                return _fulltext_filter(subtree.children[0], AcademicItem.title)
            if subtree.data == 'abstract_filter':
                return _fulltext_filter(subtree.children[0], AcademicItem.text)
            if subtree.data == 'py_filter':
                return self._year_filter(subtree.children[0])
            if subtree.data == 'doi_filter':
                return self._doi_filter(subtree.children[0])
            if subtree.data == 'import_filter':
                return self._import_filter(subtree.children[0])
            if subtree.data == 'annotation_filter':
                return self._annotation_filter(subtree.children[0])
            if subtree.data == 'and':
                return and_(*(self._assemble_filters(child) for child in subtree.children))
            if subtree.data == 'or':
                return or_(*(self._assemble_filters(child) for child in subtree.children))

            raise ValueError(f'Unexpected clause "{subtree.data}"!')
        raise ValueError(f'Unexpected {subtree.type} token: "{subtree.value}"!')

    def _year_filter(self, clause: Tree | Token) -> ColumnExpressionArgument:
        return _field_cmp_clause(clause, Field=AcademicItem.publication_year, value_clause='uint_clause')

    def _doi_filter(self, clause: Tree | Token) -> ColumnExpressionArgument[bool]:
        if isinstance(clause, Token) and clause.type == 'ESCAPED_STRING':
            return AcademicItem.doi == clause.value[1:-1]
        raise NotImplementedError(f'Encountered invalid DOI filter {clause}.')

    def _import_filter(self, clause: Tree | Token) -> ColumnExpressionArgument:
        def recurse(subtree: Tree | Token):
            if isinstance(subtree, Tree):
                if subtree.data == 'or':
                    return or_(*(recurse(child) for child in subtree.children))
                if subtree.data == 'and':
                    return and_(*(recurse(child) for child in subtree.children))
                if subtree.data == 'not':
                    return not_(*(recurse(child) for child in subtree.children))
            elif isinstance(subtree, Token):
                return Import.import_id == subtree.value

        return recurse(clause)

    def _annotation_filter(self, clause: Tree) -> ColumnExpressionArgument:
        params = clause.children
        anno_type: Literal['H', 'B', 'R'] = params[1].children[0].type
        AnnotationScheme: Type[Annotation | BotAnnotation] = Annotation if anno_type == 'H' else BotAnnotation

        key = params[0].children[0].value

        def _value_where(Alias):
            value_tree = params[2]
            if value_tree.data == 'value_bool':
                return Alias.value_bool == value_tree.children[0].value
            if value_tree.data == 'value_int':
                return _field_cmp_clause(value_tree.children[0],
                                         Field=Alias.value_int,
                                         value_clause='int_clause')
            if value_tree.data == 'value_float':
                return _field_cmp_clause(value_tree.children[0],
                                         Field=Alias.value_float,
                                         value_clause='float_clause')
            if value_tree.data == 'multi_int':
                raise ValueError('Filter for multi-label fields not supported, yet.')
            raise ValueError('Unexpected annotation label value filter clause.')

        def _param(param_key: str) -> Tree | None:
            for param_tree in params[3:]:
                if isinstance(param_tree, Tree) and param_tree.data == param_key:
                    return param_tree
            return None

        repeat_tree = _param('repeat')
        user_tree = _param('users')
        schema_tree = _param('schemas')
        scope_tree = _param('scopes')

        def _inner_where(Schema) -> Sequence[ColumnExpressionArgument]:
            inner_wheres = (Schema.key == key,
                            _value_where(Schema))

            if repeat_tree is not None:
                inner_wheres += (_field_cmp_clause(repeat_tree.children[0],
                                                   Field=Schema.repeat,
                                                   value_clause='int_clause'),)

            if schema_tree is not None:
                inner_wheres += (Schema.annotation_scheme_id.in_([
                    schema.value for schema in schema_tree.children[0].children
                ]),)

            if scope_tree is not None:
                scope_ids = [scope.value for scope in scope_tree.children[0].children]
                if anno_type == 'H':
                    inner_wheres += (and_(Assignment.assignment_id == Schema.assignment_id,
                                          Assignment.assignment_scope_id.in_(scope_ids)),)
                else:
                    inner_wheres += (BotAnnotation.bot_annotation_metadata_id.in_(scope_ids),)

            if anno_type == 'R':
                inner_wheres += (and_(
                    BotAnnotation.bot_annotation_metadata_id == BotAnnotationMetaData.bot_annotation_metadata_id,
                    BotAnnotationMetaData.kind == 'RESOLVE'),)

            return inner_wheres

        if user_tree is not None:
            if anno_type != 'H':
                raise ValueError('Invalid combination, pick annotation type "H"!')

            wheres = []
            user_set_mode = user_tree.children[0].value
            users = [user.value for user in user_tree.children[1].children]

            for user in users:
                AnnotationAlias = aliased(AnnotationScheme)
                self._stmt = self._stmt.join(AnnotationAlias, AcademicItem.item_id == AnnotationAlias.item_id)
                _wheres = _inner_where(AnnotationAlias)
                _wheres += (AnnotationAlias.user_id == user,)
                wheres.append(and_(*_wheres))

            if user_set_mode == 'ALL':
                where = and_(*wheres)
            elif user_set_mode == 'ANY':
                where = or_(*wheres)
            else:
                raise ValueError(f'Unexpected mode {user_set_mode}')
        else:
            self._stmt = self._stmt.join(AnnotationScheme, AcademicItem.item_id == AnnotationScheme.item_id)
            where = and_(*_inner_where(AnnotationScheme))

        return where


def query_to_sql(query: str, project_id: str | UUID | None = None) -> Select:
    query_object = Query(query=query, project_id=project_id)
    return query_object.stmt
