from uuid import UUID
from lark import Lark, Transformer, Tree, Token


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


def parse_str(query: str) -> Tree[Token]:
    parser = Lark('''
?clause: _TITLE      ":" str_clause     -> title_filter
       | _ABSTRACT   ":" str_clause     -> abstract_filter
       | _PYEAR      ":" uint_clause    -> py_filter
       | _DOI        ":" ESCAPED_STRING -> doi_filter
       | _IMPORT     ":" uuid_clause    -> import_filter
       | _ANNOTATION     anno_params    -> annotation_filter
       | clause _and clause             -> and
       | clause _or  clause             -> or
       | "(" clause ")"

_TITLE: "ti"i | "title"i
_ABSTRACT: "abs"i | "abstract"i
_DOI: "doi"i
_PYEAR: "py"i | "year"i | "yr"i
_IMPORT: "imp"i | "import"i
_ANNOTATION: "anno"i | "annotation"i | "label"i

// Boolean string search
?str_clause: sitem
       | str_clause _and str_clause   -> and
       | str_clause _or str_clause    -> or
?sitem: string
       | _negate sitem         -> not
       | "(" str_clause ")"

?uint_clause: COMPARATOR UINT
            | uint_clause _and uint_clause -> and
            | uint_clause _or  uint_clause -> or
            | "(" uint_clause ")"
?int_clause: COMPARATOR INT
             | int_clause _and int_clause -> and
             | int_clause _or  int_clause -> or
             | "(" int_clause ")"
?float_clause: COMPARATOR FLOAT
             | float_clause _and float_clause -> and
             | float_clause _or  float_clause -> or
             | "(" float_clause ")"
?multi_clause: multi_subclause
             | MULTI_COMP uints
?multi_subclause: COMPARATOR UINT
                | multi_subclause _and multi_subclause -> and
                | multi_subclause _or  multi_subclause -> or
                | "(" multi_subclause ")"

?uuid_clause: ie_uuid
            | uuid_clause _and uuid_clause -> and
            | uuid_clause _or  uuid_clause -> or
            | "(" uuid_clause ")"

anno_params: "(" key "," ltype "," lvalue ("," lrepeat)? ("," lusers)? ("," lschemas)? ("," lscopes)? ")"
key: "'" CNAME "'" | CNAME

lvalue: BOOLEAN      -> value_bool
      | int_clause   -> value_int
      | float_clause -> value_float
      | multi_clause -> multi_int
lrepeat :          _REPEAT       int_clause  -> repeat
lusers  : SET_MODE _USER   "IN"? uuids       -> users
lschemas:          _SCHEMA "="   uuids       -> schemas
lscopes :          _SCOPE  "="   uuids       -> scopes

_USER  : "user"   | "users"
_REPEAT: "rep"    | "repeat"
_SCHEMA: "schema" | "schemas"
_SCOPE : "scope"  | "scopes"

COMPARATOR: "=" | ">" | ">=" | "<" | "<=" | "!="
MULTI_COMP: "OVERLAPS" | "CONTAINS" | "EXACTLY"
SET_MODE: "ANY" | "ALL"

!ltype: "h"i // human
      | "r"i // resolved
      | "b"i // bot

uints: "[" UINT [("," UINT)*] "]"
     | UINT [("," UINT)*]

uuids: "[" UUID [("," UUID)*] "]"
     | UUID [("," UUID)*]
ie_uuids: "[" ie_uuid [("," ie_uuid)*] "]"
        | ie_uuid [("," ie_uuid)*]

uuid: UUID
?ie_uuid: UUID
        | _negate UUID -> not

UUID: /[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}/
    | /[A-Fa-f0-9]{32}/

_negate: "-" | "~" | "NOT"
?string: ESCAPED_STRING | WORD
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
%import common.WORD
%import common.CNAME
%import common.ESCAPED_STRING
%import common.WS

%ignore WS
    ''', parser='earley', start='clause')
    transformer = TypeTransformer()
    tree = parser.parse(query)
    tree = transformer.transform(tree)
    return tree

# UUID: /[A-Fa-f0-9]{3}-[A-Fa-f0-9]{3}/
#     | /[A-Fa-f0-9]{6}/
