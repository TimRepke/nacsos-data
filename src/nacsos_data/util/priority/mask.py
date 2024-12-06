import logging
from collections import defaultdict
from typing import TYPE_CHECKING, TypedDict, Optional

from lark import Lark, Tree, Token

from nacsos_data.util import oring, anding

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger('nacsos_data.util.priority.labels')

# https://www.lark-parser.org/ide/
# add `start: clause`

GRAMMAR = '''
?clause: cols
       | clause _and  clause            -> and
       | clause _or  clause            -> or
       | "(" clause ")"
       | _neg clause                 -> not

cols: col [(("," | " ") col)*]  -> anded
    | "OR" "[" col [(("," | " ") col)*] "]"  -> ored
    | "AND" "[" col [(("," | " ") col)*] "]"  -> anded

col: SRC     -> maybeyes
   | SRC "!" -> forceyes
   | SRC "?" -> maybeyes
   | _neg SRC     -> maybeno
   | _neg SRC "!" -> forceno
   | _neg SRC "?" -> maybeno
   | ANYSRC  -> anyyes
   | ANYSRC "*" -> allyes
   | ANYSRC "!*" -> forceallyes
   | _neg ANYSRC  -> anyno
   | _neg ANYSRC "*" -> allno
   | _neg ANYSRC "!*" -> forceallno
   | _rpref ANYSRC  -> resanyyes
   | _neg _rpref ANYSRC  -> resanyno

SRC: USER "|" LAB ":" DIGIT+
ANYSRC: LAB ":" DIGIT+
LAB: (LETTER|DIGIT|"-"|"_")+
USER: (LETTER|DIGIT|"-"|"_"|".")+
_neg: "-" | "~"
_rpref: "/"

_and: "AND"i | "&"
_or: "OR"i | "|"

%import common.DIGIT
%import common.LETTER
%import common.WS

%ignore WS
'''


class ColSet(TypedDict):
    res: str | None
    users: list[str]


def parse_rule(rule: str) -> Tree[Token]:
    parser = Lark(GRAMMAR, parser='earley', start='clause')
    # transformer = TypeTransformer()
    tree = parser.parse(rule)
    # tree = transformer.transform(tree)
    return tree


def get_inclusion_mask(rule: str, df: 'pd.DataFrame', label_cols: list[str] | None = None,
                       ignore_missing: bool = False) -> 'pd.Series':
    import pandas as pd
    tree = parse_rule(rule)

    columns: set[str] = set(df.columns) if label_cols is None else set(label_cols)

    anycols: dict[str, list[str]] = defaultdict(list)
    for col in columns:
        if '|' in col:
            anycols['|'.join(col.split('|')[1:])].append(col)

    resanycols: dict[str, ColSet] = defaultdict(lambda: ColSet(res=None, users=[]))
    for col in columns:
        if '|' in col:
            parts = col.split('|')
            if parts[0] == 'res':
                resanycols['|'.join(parts[1:])]['res'] = col
            else:
                resanycols['|'.join(parts[1:])]['users'].append(col)

    logger.debug(f'Query: {rule}')
    logger.debug(f'Allowed columns: {columns}')
    logger.debug(f'Allowed meta-columns: {anycols.keys()}')

    logger.debug(f'Tree: {tree}')

    def as_series(ret: Optional['pd.Series']) -> 'pd.Series':
        if ret is None:
            return pd.Series(pd.NA, index=df.index)
        return ret

    def recurse(subtree: Tree | Token) -> Optional['pd.Series']:  # type: ignore[type-arg]
        if isinstance(subtree, Tree):
            # -----------------------
            # combine AND/OR branches
            # -----------------------
            if subtree.data == 'and':
                return anding([recurse(subtree.children[0]), recurse(subtree.children[1])])
            if subtree.data == 'or':
                return oring([recurse(subtree.children[0]), recurse(subtree.children[1])])

            # ----------------------------------
            # combine lists of column statements
            # ----------------------------------
            if subtree.data == 'ored':
                return oring([recurse(child) for child in subtree.children])
            if subtree.data == 'anded':
                return anding([recurse(child) for child in subtree.children])

            # ------------------------
            # column statement to mask
            # ------------------------
            # column type
            ctp = subtree.children[0].type  # type: ignore[union-attr]
            # column name
            col = subtree.children[0].value  # type: ignore[union-attr]

            if ctp == 'SRC' and col not in columns:
                if not ignore_missing:
                    raise KeyError(f'`{col}` not in dataframe!')
                return None
            elif ctp == 'ANYSRC' and col not in anycols:
                if not ignore_missing:
                    raise KeyError(f'`*|{col}` not in dataframe!')
                return None

            # specific columns
            if subtree.data == 'maybeyes':
                return df[col].astype('boolean')
            if subtree.data == 'maybeno':
                return df[col].astype('boolean') is False
            if subtree.data == 'forceyes':
                return df[col] == 1
            if subtree.data == 'forceno':
                return df[col] == 0

            # any-user column
            if subtree.data == 'anyyes':
                return oring([df[c].astype('boolean') for c in anycols[col]])
            if subtree.data == 'allyes':
                return anding([oring([df[c].astype('boolean').isna() for c in anycols[col]]),
                               anding([df[c].astype('boolean') | df[c].astype('boolean').isna() for c in anycols[col]])])
            if subtree.data == 'forceallyes':
                return anding([df[c] == 1 for c in anycols[col]])
            if subtree.data == 'anyno':
                return oring([df[c].astype('boolean') is False for c in anycols[col]])
            if subtree.data == 'allno':
                return anding([oring([df[c].astype('boolean').isna() for c in anycols[col]]),
                               anding([(df[c].astype('boolean') is False) | df[c].astype('boolean').isna() for c in anycols[col]])])
            if subtree.data == 'forceallno':
                return anding([df[c] == 0 for c in anycols[col]])

            # any-user column (use resolution if available)
            if subtree.data == 'resanyyes':
                if resanycols[col]['res']:
                    return ((df[resanycols[col]['res']].notna()
                             & df[resanycols[col]['res']] is True)
                            | (df[resanycols[col]['res']].isna()
                               & oring([df[c].astype('boolean') for c in resanycols[col]['users']])))
                return oring([df[c].astype('boolean') is True for c in anycols[col]])

            if subtree.data == 'resanyno':
                if resanycols[col]['res']:
                    return ((df[resanycols[col]['res']].notna()
                             & df[resanycols[col]['res']] is False)
                            | (df[resanycols[col]['res']].isna()
                               & oring([df[c].astype('boolean') is False for c in resanycols[col]['users']])))

                return oring([df[c].astype('boolean') for c in anycols[col]])

        raise SyntaxError('You shouldn\'t end up here.')

    return as_series(recurse(tree))
