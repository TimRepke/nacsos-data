from typing import Literal, Type, Sequence
from uuid import UUID

from lark import Tree, Token
from sqlalchemy import select, and_, or_, not_, Select, ColumnExpressionArgument
from sqlalchemy.orm import MappedColumn, aliased, Session
import sqlalchemy.sql.functions as func

from nacsos_data.db import DatabaseEngine
from nacsos_data.db.schemas import (
    AcademicItem,
    Annotation,
    BotAnnotation,
    BotAnnotationMetaData,
    Assignment,
    m2m_import_item_table
)
from nacsos_data.models.items import AcademicItemModel
from .parse import parse_str


def _fulltext_filter(clause: Tree | Token, Field: MappedColumn) -> ColumnExpressionArgument:  # type: ignore[type-arg]
    if isinstance(clause, Token):  # type: ignore[type-arg]
        if clause.type == 'WORD':
            return Field.ilike(f'%{clause.value}%')
        if clause.type == 'ESCAPED_STRING':
            return Field.ilike(f'%{clause.value[1:-1]}%')
    raise NotImplementedError('"Complex" title/abstract filtering not implemented, yet.')


def _field_cmp(cmp: str, val: int | float, Field: MappedColumn) -> ColumnExpressionArgument:  # type: ignore[type-arg]
    if cmp == '>':
        return Field > val  # type: ignore[no-any-return]
    if cmp == '>=':
        return Field >= val  # type: ignore[no-any-return]
    if cmp == '=':
        return Field == val  # type: ignore[no-any-return]
    if cmp == '<':
        return Field < val  # type: ignore[no-any-return]
    if cmp == '<=':
        return Field <= val  # type: ignore[no-any-return]
    if cmp == '!=':
        return Field != val  # type: ignore[no-any-return]
    raise ValueError(f'Unexpected comparator "{cmp}".')


def _field_cmp_clause(clause: Tree | Token, Field: MappedColumn,  # type: ignore[type-arg]
                      value_clause: Literal['uint_clause', 'int_clause', 'float_clause']) \
        -> ColumnExpressionArgument:  # type: ignore[type-arg]
    if isinstance(clause, Tree):
        if clause.data == value_clause:
            cmp: str = clause.children[0]  # type: ignore[assignment]
            val: int | float = clause.children[1]  # type: ignore[assignment]
            return _field_cmp(cmp, val, Field)
        elif clause.data == 'and':
            return and_(*(_field_cmp_clause(child, Field, value_clause) for child in clause.children))
        elif clause.data == 'or':
            return or_(*(_field_cmp_clause(child, Field, value_clause) for child in clause.children))
    raise ValueError(f'Unexpected: {clause}')


class Query:
    def __init__(self, query: str, project_id: str | UUID | None = None):
        self.project_id = project_id

        self.query = query
        self.query_tree: Tree = parse_str(query)  # type: ignore[type-arg]

        self._stmt = select(AcademicItem).distinct(AcademicItem.item_id)
        self._stmt = self._to_sql()

    def __str__(self) -> str:
        return self.query

    @property
    def stmt(self) -> Select:  # type: ignore[type-arg]
        return self._stmt

    @property
    def pretty(self) -> str:
        return self.query_tree.pretty()

    def count(self, db_engine: DatabaseEngine | None = None, session: Session | None = None) -> int:
        stmt = self.stmt.subquery()
        cnt_stmt = func.count(stmt.c.item_id)
        if db_engine is not None:
            new_session: Session
            with db_engine.session() as new_session:
                return new_session.execute(cnt_stmt).scalar()  # type: ignore[return-value]
        if session is not None:
            return session.execute(cnt_stmt).scalar()  # type: ignore[return-value]
        raise RuntimeError('No connection to database.')

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
            new_session: Session
            with db_engine.session() as new_session:
                items = new_session.execute(stmt).scalars().all()
        elif session is not None:
            items = session.execute(stmt).scalars().all()
        else:
            raise RuntimeError('No connection to database.')
        return [AcademicItemModel.model_validate(item.__dict__) for item in items]

    def _to_sql(self) -> Select:  # type: ignore[type-arg]
        filters = self._assemble_filters(self.query_tree)
        if self.project_id is not None:
            filters = and_(AcademicItem.project_id == self.project_id, filters)
        self._stmt = self._stmt.where(filters)
        return self._stmt

    def _assemble_filters(self, subtree: Tree | Token) -> ColumnExpressionArgument:  # type: ignore[type-arg]
        if isinstance(subtree, Tree):
            if subtree.data == 'title_filter':
                return _fulltext_filter(subtree.children[0], AcademicItem.title)  # type: ignore[arg-type]
            if subtree.data == 'abstract_filter':
                return _fulltext_filter(subtree.children[0], AcademicItem.text)  # type: ignore[arg-type]
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

    def _year_filter(self, clause: Tree | Token) -> ColumnExpressionArgument:  # type: ignore[type-arg]
        return _field_cmp_clause(clause, Field=AcademicItem.publication_year,  # type: ignore[arg-type]
                                 value_clause='uint_clause')

    def _doi_filter(self, clause: Tree | Token) -> ColumnExpressionArgument:  # type: ignore[type-arg]
        if isinstance(clause, Token) and clause.type == 'ESCAPED_STRING':
            return AcademicItem.doi == clause.value[1:-1]  # type: ignore[no-any-return]
        raise NotImplementedError(f'Encountered invalid DOI filter {clause}.')

    def _import_filter(self, clause: Tree | Token) -> ColumnExpressionArgument:  # type: ignore[type-arg]
        def recurse(subtree: Tree | Token):  # type: ignore[type-arg,no-untyped-def]
            if isinstance(subtree, Tree):
                if subtree.data == 'or':
                    return or_(*(recurse(child) for child in subtree.children))
                if subtree.data == 'and':
                    return and_(*(recurse(child) for child in subtree.children))
                if subtree.data == 'not':
                    return not_(*(recurse(child) for child in subtree.children))
            elif isinstance(subtree, Token):
                return m2m_import_item_table.c.import_id == subtree.value

            raise ValueError(f'Invalid subtree for import error ({subtree})')

        # FIXME: for the AND logic, this probably needs to be extended to using aliases
        self._stmt = self.stmt.join(m2m_import_item_table, m2m_import_item_table.c.item_id == AcademicItem.item_id)
        return recurse(clause)  # type: ignore[no-any-return]

    def _annotation_filter(self, clause: Tree) -> ColumnExpressionArgument:  # type: ignore[type-arg]
        params = clause.children
        anno_type: Literal['H', 'B', 'R'] = params[1].children[0].type  # type: ignore[union-attr]
        AnnotationScheme: Type[Annotation | BotAnnotation] = Annotation if anno_type == 'H' else BotAnnotation

        key = params[0].children[0].value  # type: ignore[union-attr]

        def _value_where(Alias):  # type: ignore[no-untyped-def]
            value_tree = params[2]
            if value_tree.data == 'value_bool':
                return Alias.value_bool == value_tree.children[0].value  # type: ignore[union-attr]
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

        def _param(param_key: str) -> Tree | None:  # type: ignore[type-arg]
            for param_tree in params[3:]:
                if isinstance(param_tree, Tree) and param_tree.data == param_key:
                    return param_tree
            return None

        repeat_tree = _param('repeat')
        user_tree = _param('users')
        schema_tree = _param('schemas')
        scope_tree = _param('scopes')

        def _inner_where(Schema) -> Sequence[ColumnExpressionArgument]:  # type: ignore[type-arg,no-untyped-def]
            inner_wheres = (Schema.key == key,
                            _value_where(Schema))

            if repeat_tree is not None:
                inner_wheres += (_field_cmp_clause(repeat_tree.children[0],  # type: ignore[assignment]
                                                   Field=Schema.repeat,
                                                   value_clause='int_clause'),)

            if schema_tree is not None:
                inner_wheres += (Schema.annotation_scheme_id.in_([  # type: ignore[assignment]
                    schema.value for schema in schema_tree.children[0].children  # type: ignore[union-attr]
                ]),)

            if scope_tree is not None:
                scope_ids = [scope.value for scope in scope_tree.children[0].children]  # type: ignore[union-attr]
                if anno_type == 'H':
                    self._stmt = self.stmt.join(Assignment, Assignment.assignment_id == Schema.assignment_id)
                    inner_wheres += (Assignment.assignment_scope_id.in_(scope_ids),)  # type: ignore[assignment]
                else:
                    inner_wheres += (  # type: ignore[assignment]
                        BotAnnotation.bot_annotation_metadata_id.in_(scope_ids),)

            if anno_type == 'R':
                inner_wheres += (and_(  # type: ignore[assignment]
                    BotAnnotation.bot_annotation_metadata_id == BotAnnotationMetaData.bot_annotation_metadata_id,
                    BotAnnotationMetaData.kind == 'RESOLVE'),)

            return inner_wheres

        if user_tree is not None:
            if anno_type != 'H':
                raise ValueError('Invalid combination, pick annotation type "H"!')

            wheres = []
            user_set_mode = user_tree.children[0].value  # type: ignore[union-attr]
            users = [user.value for user in user_tree.children[1].children]  # type: ignore[union-attr]

            for user in users:
                AnnotationAlias = aliased(AnnotationScheme)
                self._stmt = self._stmt.join(
                    AnnotationAlias,
                    AcademicItem.item_id == AnnotationAlias.item_id)  # type: ignore[attr-defined]
                _wheres = _inner_where(AnnotationAlias)
                _wheres += (AnnotationAlias.user_id == user,)  # type: ignore[attr-defined, operator]
                wheres.append(and_(*_wheres))

            if user_set_mode == 'ALL':
                where = and_(*wheres)
            elif user_set_mode == 'ANY':
                where = or_(*wheres)
            else:
                raise ValueError(f'Unexpected mode {user_set_mode}')
        else:
            AnnotationAlias = aliased(AnnotationScheme)
            self._stmt = self._stmt.join(AnnotationAlias, AcademicItem.item_id == AnnotationAlias.item_id)
            where = and_(*_inner_where(AnnotationAlias))

        return where


def query_to_sql(query: str, project_id: str | UUID | None = None) -> Select:  # type: ignore[type-arg]
    query_object = Query(query=query, project_id=project_id)
    return query_object.stmt
