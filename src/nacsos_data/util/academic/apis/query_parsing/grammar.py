import re

from lark import Lark, Tree, Token, Transformer

grammar = Lark(
    '''
    ?start: op_or

    ?op_or: op_and 
        | op_or "OR" op_and           -> op_or
        | op_or near op_and           -> near

    ?near: "NEAR/" NUMBER
         | "W/" NUMBER
         | NUMBER "N"
         | NUMBER "W"

    ?op_and: atom
        | op_and "AND NOT" atom    -> op_not
        | op_and "NOT" atom        -> op_not
        | op_and "AND" atom        -> op_and

    ?phrase_inner: token
         | token phrase_inner
         | token "-" token
    ?phrase: _QUOT phrase_inner _QUOT

    ?token: WORD
         | WORD "*"             -> wild_pre
         | WORD "?"             -> wild_one
         | "*" WORD             -> wild_post
         | "*" WORD "*"         -> wild_prepost
         | WORD "$" WORD        -> wild_in
         | WORD "?" WORD        -> wild_in

    ?atom: token
         | phrase
         | "(" op_or ")"

    _QUOT : "\\""


    DIGIT: "0".."9"
    INT: DIGIT+
    SIGNED_INT: ["+"|"-"] INT
    DECIMAL: INT "." INT? | "." INT
    FLOAT: DECIMAL
    NUMBER: FLOAT | INT

    LCASE_LETTER: "a".."z"
    UCASE_LETTER: "A".."Z"

    LETTER: UCASE_LETTER | LCASE_LETTER | NUMBER | "."
    WORD: LETTER+

    %import common.WS

    %ignore WS
    ''', parser='earley', start='start',
)

NEAR_OFFSET = 0


class Node:
    def __init__(self, label, children=None):
        self.label = label
        self.children = children or []

    def render(self, indent=0):
        pad = '  ' * indent
        if not self.children:
            return self.label
        lines = [child.render(indent + 1) for child in self.children]

        return f'{pad}(\n' + (f'\n{pad} {self.label} '.join(lines)) + f'\n{pad})'


def expand_wildcard(token: str, prefix: str, postfix: str, expansions: dict[str, list[str]]):
    if token in expansions:
        return f'({' OR '.join(expansions[token])})'
    return f'{prefix}{token}{postfix}'


class QueryTransformer(Transformer):
    def __init__(self, visit_tokens: bool = True, expansions: dict[str, list[str]] | None = None):
        super().__init__(visit_tokens=visit_tokens)
        self.expansions = expansions or {}

    def WORD(self, t):
        return Node(str(t))

    def NUMBER(self, t):
        return Node(str(t))

    def phrase_inner(self, items):
        return Node('(' + (' W '.join(i.label for i in items)) + ')')

    def wild_pre(self, items):
        return Node(expand_wildcard(items[0].label, prefix='', postfix='*', expansions=self.expansions))

    def wild_post(self, items):
        # FIXME: surround QueryParser doesn't like wildcards as prefix
        # return Node(self.expand_wildcard(items[0].label, prefix='*', postfix=''))
        return Node(expand_wildcard(items[0].label, prefix='', postfix='', expansions=self.expansions))

    def wild_prepost(self, items):
        # FIXME: surround QueryParser doesn't like wildcards as prefix
        # return Node(self.expand_wildcard(items[0].label, prefix='*', postfix='*'))
        return Node(expand_wildcard(items[0].label, prefix='', postfix='*', expansions=self.expansions))

    def wild_one(self, items):
        return Node(f'{items[0].label}?')

    def wild_in(self, items):
        return Node(f'{items[0].label}?{items[1].label}')

    def op_and(self, items):
        return Node('AND', items)

    def op_or(self, items):
        return Node('OR', items)

    def op_not(self, items):
        return Node('NOT', items)

    def near(self, items):
        distance = int(items[1].label) + NEAR_OFFSET
        return Node('W' if distance < 2 else f'{distance}W', items[:1] + items[2:])


def parse(query, expansions: dict[str, list[str]] | None = None, parent: str | None = None):
    comm = re.compile(r'# .*\n')
    tree = grammar.parse(comm.sub('', query))
    expansions = {} if expansions is None else expansions

    def recurse(subtree: Tree | Token, indent: str = '') -> str:
        if isinstance(subtree, Token):
            return subtree.value

        if not isinstance(subtree, Tree):
            raise SyntaxError('This is not a tree!')

        if subtree.data == 'op_or':
            return (
                '(\n'
                f'{indent}   {recurse(subtree.children[0], indent=indent + '  ')}\n'
                f'{indent}OR {recurse(subtree.children[1], indent=indent + '  ')}\n'
                f'{indent[:-2]})'
            )

        if subtree.data == 'op_and':
            return (
                '(\n'
                f'{indent}    {recurse(subtree.children[0], indent=indent + '  ')}\n'
                f'{indent}AND {recurse(subtree.children[1], indent=indent + '  ')}\n'
                f'{indent[:-2]})'
            )

        if subtree.data == 'op_not':
            return (
                '(\n'
                f'{indent}    {recurse(subtree.children[0], indent=indent + '  ')}\n'
                f'{indent}NOT {recurse(subtree.children[1], indent=indent + '  ')}\n'
                f'{indent[:-2]})'
            )

        if subtree.data == 'near':
            near = int(subtree.children[1].value) + NEAR_OFFSET
            near_str = 'W' if near < 2 else f'{near}W'
            return (
                '(\n'
                f'{indent}  {recurse(subtree.children[0], indent=indent + '  ')}\n'
                f'{indent}{near_str} '
                f'{recurse(subtree.children[2], indent=indent + '  ')}\n'
                f'{indent[:-2]})'
            )

        if subtree.data == 'phrase_inner':
            return f'({' W '.join([recurse(child) for child in subtree.children])})'

        if subtree.data == 'wild_pre':
            return expand_wildcard(subtree.children[0].value, prefix='', postfix='*', expansions=expansions)
        if subtree.data == 'wild_post':
            return expand_wildcard(subtree.children[0].value, prefix='', postfix='',
                                   expansions=expansions)  # FIXME: postfix not available in surround
        if subtree.data == 'wild_prepost':
            # FIXME: postfix not available in surround
            # return ('('
            #         f'{expand_wildcard(subtree.children[0].value, prefix='', postfix='*', expansions=expansions)} OR '
            #         f'{expand_wildcard(subtree.children[0].value, prefix='*', postfix='', expansions=expansions)}'
            #         ')')
            return expand_wildcard(subtree.children[0].value, prefix='', postfix='', expansions=expansions)
        if subtree.data == 'wild_in':
            return f'{subtree.children[0].value}?{subtree.children[1].value}'
        if subtree.data == 'wild_one':
            return f'{subtree.children[0].value}?'

        raise SyntaxError("You shouldn't end up here.")

    return recurse(tree)


if __name__ == '__main__':
    q = '''
    (
        (
            heat 
            W/2 (
                   stress
                OR fatigue
                OR burn*
                OR stroke
                OR exhaustion
                OR cramp*
            )
        )
        OR skin
        OR fever*
        OR rash*
        OR eczema*
        OR "thermal stress*"
        OR hypertherm*
        OR hypotherm*
        OR "thermal stability"
    )'''
    # print(parse(q))
    print('--------------')
    prepared = re.compile(r'# .*\n').sub('', q)
    tree = grammar.parse(prepared)
    print(tree)
    processed = QueryTransformer(expansions={'burn': ['burn', 'burned', 'burns']}).transform(tree)
    print(processed.render())