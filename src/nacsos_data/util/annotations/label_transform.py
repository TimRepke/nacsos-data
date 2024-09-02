import uuid
from typing import Literal, TypeVar, Callable
from uuid import UUID
from lark import Lark, Transformer, Tree, Token
from pydantic import BaseModel
from sqlalchemy import text

from nacsos_data.db.engine import ensure_session_async, DBSession

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

CNAME: ("_"|LETTER) ("_"|"-"|LETTER|DIGIT)*

_and: "AND"i | "&"
_or: "OR"i | "|"

%import common.DIGIT
%import common.LETTER
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
    multis: list[list[int]] | None = None


class SortedAnnotation(BaseModel):
    source_order: int
    source_id: str | uuid.UUID
    user_id: str | None = None
    source_type: Literal['R', 'H']
    item_order: int
    item_id: str | uuid.UUID
    labels: dict[str, SortedAnnotationLabel]


@ensure_session_async
async def get_annotations(session: DBSession, source_ids: list[str] | None = None) -> list[SortedAnnotation]:
    if source_ids is None:
        return []
    stmt = text('''
        WITH sources as (SELECT row_number() over () source_order, source_id
                         FROM unnest(:source_ids ::uuid[]) as source_id),
             labels as (SELECT source_order,
                               source_id,
                               'H'                                                                   as source_type,
                               ann.user_id::text                                                     as user_id,
                               min(ass."order")                                                      as item_order,
                               ann.item_id                                                           as item_id,
                               ann.key                                                               as key,
                               array_agg(ann.value_int) FILTER ( WHERE ann.value_int is not null )   as values_int,
                               mode() WITHIN GROUP ( ORDER BY ann.value_int )                        as value_int,
                               array_agg(ann.value_bool) FILTER ( WHERE ann.value_bool is not null ) as values_bool,
                               mode() WITHIN GROUP ( ORDER BY ann.value_bool )                       as value_bool,
                               array_agg(ann.multi_int) FILTER ( WHERE ann.multi_int is not null
                                                                   AND array_length(ann.multi_int, 1) > 0 ) as multis
                        FROM sources
                                 LEFT JOIN assignment ass ON ass.assignment_scope_id = source_id::uuid
                                 LEFT JOIN annotation ann ON ann.assignment_id = ass.assignment_id
                        WHERE ass.item_id is not null AND ann.key is not null
                        GROUP BY source_order, source_id, ann.user_id, ann.item_id, ann.key

                        UNION

                        SELECT source_order,
                               source_id,
                               'R'                                                                 as source_type,
                               'resolved'                                                          as user_id,
                               min(ba."order")                                                     as item_order,
                               ba.item_id                                                          as item_id,
                               ba.key                                                              as key,
                               array_agg(ba.value_int) FILTER ( WHERE ba.value_int is not null )   as values_int,
                               mode() WITHIN GROUP ( ORDER BY ba.value_int )                       as value_int,
                               array_agg(ba.value_bool) FILTER ( WHERE ba.value_bool is not null ) as values_bool,
                               mode() WITHIN GROUP ( ORDER BY ba.value_bool )                      as value_bool,
                               array_agg(ba.multi_int) FILTER ( WHERE ba.multi_int is not null
                                                                  AND array_length(ba.multi_int, 1) > 0 ) as multis
                        FROM sources
                                 LEFT JOIN bot_annotation ba ON ba.bot_annotation_metadata_id = source_id::uuid
                        WHERE ba.item_id is not null AND ba.key is not null
                        GROUP BY source_order, source_id, ba.item_id, ba.key)
        SELECT source_order,
               source_id,
               source_type,
               user_id,
               min(item_order)                                                as item_order,
               item_id,
               json_object_agg(key,
                               json_build_object('value_int', value_int,
                                                 'values_int', values_int,
                                                 'value_bool', value_bool,
                                                 'values_bool', values_bool,
                                                 'multis', multis)) as labels
        FROM labels
        GROUP BY source_order, source_id, source_type, item_id, user_id
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
