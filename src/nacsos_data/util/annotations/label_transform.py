import uuid
from typing import Literal, TypeVar, Callable
from uuid import UUID
from lark import Lark, Transformer, Tree, Token
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from nacsos_data.db.engine import ensure_session_async

GRAMMAR = '''
?clause: expr
       | clause _and clause             -> and
       | clause _or  clause             -> or
       | "(" clause ")"
       | _negate clause                 -> not

?key: "'" CNAME "'" | CNAME
?expr: key EQNEQ BOOLEAN -> bool_clause
     | key COMPARATOR INT->int_clause
     | key COMPARATOR FLOAT->float_clause
     | key MULTI_COMP ui->multi_clause

EQNEQ: "=" | "!="
COMPARATOR: EQNEQ | ">" | ">=" | "<" | "<="
MULTI_COMP: "OVERLAPS" | "CONTAINS" | "EXACTLY"

?ui: "[" UINT [("," UINT)*] "]"
     | UINT [("," UINT)*]

_negate: "-" | "~" | "NOT"
BOOLEAN: "true"i | "false"i

UINT: DIGIT+
INT: ["+"|"-"] UINT
UFLOAT: UINT "." UINT? | "." UINT
FLOAT: ["+"|"-"] UFLOAT
UNUMBER: UINT | UFLOAT
NUMBER: INT | FLOAT

_and: "AND"i | "&"
_or: "OR"i | "|"

%import common.DIGIT
%import common.CNAME
%import common.WS

%ignore WS
'''


class TypeTransformer(Transformer):  # type: ignore[type-arg]
    def INT(self, tok: Token) -> Token:
        return tok.update(value=int(tok))

    def UINT(self, tok: Token) -> Token:
        return self.INT(tok)

    def UUID(self, tok: Token) -> Token:
        return tok.update(value=UUID(tok))

    def FLOAT(self, tok: Token) -> Token:
        return tok.update(value=float(tok))

    def BOOLEAN(self, tok: Token) -> Token:
        return tok.update(value=True if tok.lower() == 'true' else False)


def parse_rule(query: str) -> Tree[Token]:
    parser = Lark(GRAMMAR, parser='earley', start='clause')
    transformer = TypeTransformer()
    tree = parser.parse(query)
    tree = transformer.transform(tree)
    return tree


class SortedAnnotationLabel(BaseModel):
    value_int: int | None = None
    values_int: list[int] | None = None
    value_bool: bool | None = None
    values_bool: list[bool] | None = None


class SortedAnnotation(BaseModel):
    source_order: int
    source_id: str | uuid.UUID
    source_type: Literal['R', 'H']
    item_order: int
    item_id: str | uuid.UUID
    labels: dict[str, SortedAnnotationLabel]


class SortedAnnotationUser(SortedAnnotation):
    user_id: str | uuid.UUID


@ensure_session_async
async def get_annotations_by_user(session: AsyncSession, assignment_scope_ids: list[str] | None = None) \
        -> list[SortedAnnotationUser]:
    if assignment_scope_ids is None:
        return []
    stmt = text('''
        WITH sources as (SELECT row_number() over () source_order, source_id
                         FROM unnest(:scope_ids ::uuid[]) as source_id),
             labels as (SELECT source_order,
                               ann.user_id as user_id,
                               source_id,
                               'H'                                                                   as source_type,
                               min(ass."order")                                                      as item_order,
                               ann.item_id                                                           as item_id,
                               ann.key                                                               as key,
                               array_agg(ann.value_int) FILTER ( WHERE ann.value_int is not null )   as values_int,
                               mode() WITHIN GROUP ( ORDER BY ann.value_int )                        as value_int,
                               array_agg(ann.value_bool) FILTER ( WHERE ann.value_bool is not null ) as values_bool,
                               mode() WITHIN GROUP ( ORDER BY ann.value_bool )                       as value_bool
                        FROM sources
                                 LEFT JOIN assignment ass ON ass.assignment_scope_id = source_id::uuid
                                 LEFT JOIN annotation ann ON ann.assignment_id = ass.assignment_id
                        WHERE ass.item_id is not null AND ann.key is not null
                        GROUP BY source_order, source_id, ann.user_id, ann.item_id, ann.key)
        SELECT source_order,
               source_id,
               source_type,
               min(item_order)                                                as item_order,
               item_id,
               user_id,
               json_object_agg(key,
                               json_build_object('value_int', value_int,
                                                 'values_int', values_int,
                                                 'value_bool', value_bool,
                                                 'values_bool', values_bool)) as labels
        FROM labels
        GROUP BY source_order, source_id, source_type, user_id, item_id
        ORDER BY source_order, item_order;
    ''')
    rslt = await session.execute(stmt, {'scope_ids': assignment_scope_ids})
    return [SortedAnnotationUser.model_validate(r) for r in rslt.mappings().all()]


@ensure_session_async
async def get_annotations(session: AsyncSession, source_ids: list[str] | None = None) -> list[SortedAnnotation]:
    if source_ids is None:
        return []
    stmt = text('''
        WITH sources as (SELECT row_number() over () source_order, source_id
                         FROM unnest(:source_ids) as source_id),
             labels as (SELECT source_order,
                               source_id,
                               'H'                                                                   as source_type,
                               min(ass."order")                                                      as item_order,
                               ann.item_id                                                           as item_id,
                               ann.key                                                               as key,
                               array_agg(ann.value_int) FILTER ( WHERE ann.value_int is not null )   as values_int,
                               mode() WITHIN GROUP ( ORDER BY ann.value_int )                        as value_int,
                               array_agg(ann.value_bool) FILTER ( WHERE ann.value_bool is not null ) as values_bool,
                               mode() WITHIN GROUP ( ORDER BY ann.value_bool )                       as value_bool
                        FROM sources
                                 LEFT JOIN assignment ass ON ass.assignment_scope_id = source_id::uuid
                                 LEFT JOIN annotation ann ON ann.assignment_id = ass.assignment_id
                        WHERE ass.item_id is not null AND ann.key is not null
                        GROUP BY source_order, source_id, ann.item_id, ann.key

                        UNION

                        SELECT source_order,
                               source_id,
                               'R'                                                                 as source_type,
                               min(ba."order")                                                     as item_order,
                               ba.item_id                                                          as item_id,
                               ba.key                                                              as key,
                               array_agg(ba.value_int) FILTER ( WHERE ba.value_int is not null )   as values_int,
                               mode() WITHIN GROUP ( ORDER BY ba.value_int )                       as value_int,
                               array_agg(ba.value_bool) FILTER ( WHERE ba.value_bool is not null ) as values_bool,
                               mode() WITHIN GROUP ( ORDER BY ba.value_bool )                      as value_bool
                        FROM sources
                                 LEFT JOIN bot_annotation ba ON ba.bot_annotation_metadata_id = source_id::uuid
                        WHERE ba.item_id is not null AND ba.key is not null
                        GROUP BY source_order, source_id, ba.item_id, ba.key)
        SELECT source_order,
               source_id,
               source_type,
               min(item_order)                                                as item_order,
               item_id,
               json_object_agg(key,
                               json_build_object('value_int', value_int,
                                                 'values_int', values_int,
                                                 'value_bool', value_bool,
                                                 'values_bool', values_bool)) as labels
        FROM labels
        GROUP BY source_order, source_id, source_type, item_id
        ORDER BY source_order, item_order;
    ''')
    rslt = await session.execute(stmt, {'source_ids': source_ids})
    return [SortedAnnotation.model_validate(r) for r in rslt.mappings().all()]


class AnnotationValues(BaseModel):
    value_int: dict[str, list[int] | None]
    value_bool: dict[str, list[bool] | None]
    value_str: dict[str, list[str] | None]
    value_multi: dict[str, list[list[int]] | None]


ItemAnnotationValues = dict[str, AnnotationValues]


@ensure_session_async
async def get_annotations_flat(session: AsyncSession,
                               assignment_scope_ids: list[str] | None = None,
                               bot_annotation_metadata_ids: list[str] | None = None,
                               key_username: bool = False) -> dict[str, ItemAnnotationValues]:
    if ((assignment_scope_ids is None or len(assignment_scope_ids) == 0)
            and (bot_annotation_metadata_ids is None or len(bot_annotation_metadata_ids) == 0)):
        raise AssertionError('Need at least one non-empty source id for assignments or resolutions')

    source_ids: list[str] = []
    if assignment_scope_ids is not None:
        source_ids += assignment_scope_ids
    if bot_annotation_metadata_ids is not None:
        source_ids += bot_annotation_metadata_ids
    user_identifier = 'username' if key_username else 'user_id'
    stmt = text(f'''
        WITH sources as (SELECT row_number() over () source_order, source_id
                         FROM unnest(:source_ids ::uuid[]) as source_id),
             annos as (SELECT a.item_id,
                              a.key,
                              a.user_id::text,
                              u.username,
                              array_agg(a.value_bool) filter ( where a.value_bool is not null ) as value_bool,
                              array_agg(a.value_int) filter ( where a.value_int is not null )   as value_int,
                              array_agg(a.value_str) filter ( where a.value_str is not null )   as value_str,
                              array_agg(a.multi_int) filter ( where a.multi_int is not null )   as multi_int
                       FROM sources
                                LEFT JOIN assignment ass ON ass.assignment_scope_id = source_id
                                LEFT JOIN annotation a ON a.assignment_id = ass.assignment_id
                                JOIN "user" u ON u.user_id = a.user_id
                       GROUP BY a.item_id, a.key, a.user_id::text, u.username),
             bot_annos as (SELECT ba.item_id,
                                  ba.key,
                                  'resolved'                                                         as user_id,
                                  'resolved'                                                         as username,
                                  CASE WHEN ba.value_bool is not null THEN array [ba.value_bool] END as value_bool,
                                  CASE WHEN ba.value_int is not null THEN array [ba.value_int] END   as value_int,
                                  CASE WHEN ba.value_str is not null THEN array [ba.value_str] END   as value_str,
                                  CASE WHEN ba.multi_int is not null THEN array [ba.multi_int] END   as multi_int
                           FROM sources
                                    LEFT JOIN bot_annotation ba ON ba.bot_annotation_metadata_id = source_id::uuid
                           WHERE ba.item_id is not null),
             grouped as (SELECT aba.item_id,
                                aba.key,
                                jsonb_object_agg(aba.{user_identifier}, aba.value_int)  as value_int,
                                jsonb_object_agg(aba.{user_identifier}, aba.value_bool) as value_bool,
                                jsonb_object_agg(aba.{user_identifier}, aba.value_str)  as value_str,
                                jsonb_object_agg(aba.{user_identifier}, aba.multi_int)  as value_multi
                         FROM (SELECT * FROM annos UNION SELECT * FROM bot_annos) as aba
                         GROUP BY aba.item_id, aba.key)
        SELECT g.item_id,
               jsonb_object_agg(g.key,
                                jsonb_build_object(
                                        'value_int', g.value_int,
                                        'value_bool', g.value_bool,
                                        'value_str', g.value_str,
                                        'value_multi', g.value_multi
                                )) as values
        FROM grouped g
        GROUP BY g.item_id;''')
    rslt = (await session.execute(stmt, {'source_ids': source_ids})).mappings().all()
    return {r['item_id']: ItemAnnotationValues(**r['values']) for r in rslt}


T = TypeVar('T')
A = TypeVar('A', ItemAnnotationValues, dict[str, SortedAnnotationLabel])
Plucker = Callable[[A, str, str], int | bool | None]


def pluck_value_flat(annotation: ItemAnnotationValues, key: str, user: str, field: str) -> int | bool | None:
    if (key in annotation
            and user in getattr(annotation[key], field)
            and getattr(annotation[key], field)[user] is not None
            and len(getattr(annotation[key], field)[user]) > 0):
        return getattr(annotation[key], field)[user][0]  # type: ignore[no-any-return]
    return None


def get_flat_plucker(user: str) -> Plucker[ItemAnnotationValues]:
    def inner(annotation: ItemAnnotationValues, key: str, field: str) -> int | bool | None:
        return pluck_value_flat(annotation=annotation, key=key, field=field, user=user)

    return inner


def pluck_value_nest(annotation: dict[str, SortedAnnotationLabel], key: str, field: str) -> int | bool | None:
    if key in annotation:
        return getattr(annotation[key], field)  # type: ignore[no-any-return]
    return None


def cmp(op: str, v1: int | bool | None, v2: int | bool | None) -> bool:
    if v1 is None or v2 is None:
        return False
    if op == '>':
        return v1 > v2
    if op == '>=':
        return v1 >= v2
    if op == '=':
        return v1 == v2
    if op == '<':
        return v1 < v2
    if op == '<=':
        return v1 <= v2
    if op == '!=':
        return v1 != v2
    raise ValueError(f'Unexpected comparator "{op}".')


def test_entry(subtree: Tree | Token,  # type: ignore[type-arg]
               annotation: A,
               plucker: Plucker[A],
               majority: bool = True) -> bool:
    if isinstance(subtree, Tree):
        if subtree.data == 'and':
            return (test_entry(subtree.children[0], annotation, plucker, majority)
                    and test_entry(subtree.children[1], annotation, plucker, majority))
        if subtree.data == 'or':
            return (test_entry(subtree.children[0], annotation, plucker, majority)
                    or test_entry(subtree.children[1], annotation, plucker, majority))
        if subtree.data == 'not':
            return not test_entry(subtree.children[0], annotation, plucker, majority)

        if subtree.data == 'int_clause':
            key = str(subtree.children[0])
            value = plucker(annotation, key, 'value_int')
            if majority:
                return cmp(subtree.children[1],  # type: ignore[arg-type]
                           value,
                           subtree.children[2].value)  # type: ignore[union-attr]

        if subtree.data == 'bool_clause':
            key = str(subtree.children[0])
            value = plucker(annotation, key, 'value_bool')
            if majority:
                return cmp(subtree.children[1],  # type: ignore[arg-type]
                           value,
                           subtree.children[2].value)  # type: ignore[union-attr]

        # if subtree.data == 'float_clause': TODO
        # if subtree.data == 'multi_clause': TODO
    raise SyntaxError('Invalid inclusion query.')


def annotations_to_sequence(inclusion_rule: str,
                            annotations: list[SortedAnnotation],
                            majority: bool = True) -> list[int]:
    """
    Transform labels to sequence when using the annotation-matrix-style format
    (e.g. via `get_annotations_by_user` `get_annotations`)
    :param inclusion_rule:
    :param annotations:
    :param majority: if True, make decision based on majority vote; else, consider all values and test for best fit
    :return:
    """
    if not majority:
        raise NotImplementedError('any matching not implemented, yet')

    rule_tree = parse_rule(inclusion_rule)
    return [int(test_entry(rule_tree,
                           annotation=anno.labels,
                           plucker=pluck_value_nest,
                           majority=majority))
            for anno in annotations]


def flat_annotations_to_sequence(inclusion_rule: str,
                                 annotations: dict[str, ItemAnnotationValues],
                                 user: str,
                                 item_ids: list[str] | None = None) -> list[int]:
    """
    Transform labels to sequence when using the user-centric flat format
    (e.g. via `get_annotations_flat`)
    :param inclusion_rule:
    :param annotations:
    :param user:
    :param item_ids:
    :return:
    """
    if item_ids is None:
        item_ids = list(annotations.keys())
    rule_tree = parse_rule(inclusion_rule)
    plucker = get_flat_plucker(user)
    return [int(test_entry(rule_tree,
                           annotation=annotations[item_id],
                           plucker=plucker,
                           majority=True))
            for item_id in item_ids]
