import asyncio
import logging
from uuid import UUID

from nacsos_data.db import get_engine_async
from nacsos_data.models.nql import (
    FieldFilter,
    FieldFilters,
    SubQuery,
    LabelFilterInt,
    LabelFilterBool,
    AssignmentFilter,
    AnnotationFilter,
    AbstractFilter,
    NQLFilterParser,
)
from nacsos_data.util.conf import load_settings
from nacsos_data.util.nql import nql_to_sql
from nacsos_data.db.schemas import ItemType
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import dialect

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(name)s (%(process)d): %(message)s', level='DEBUG')
logging.getLogger('matplotlib').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# pd.options.display.max_columns = 650
# pd.options.display.max_rows = 40
# pd.options.display.width = 50000

queries = [
    # 1. single leaf: title LIKE "climate"
    (
        UUID('db6ee519-afb5-4813-822b-bfbc7dfd2237'),
        FieldFilter(field='title', value='climate'),
    ),
    # 2. single leaf on a joined table: resolved label incl = 1
    (
        UUID('db6ee519-afb5-4813-822b-bfbc7dfd2237'),
        LabelFilterInt(type='resolved', value_int=1, key='incl', comp='='),
    ),
    # 3. (label:RES incl=1 AND PY:>=2024) OR NOT title:"climate"
    (
        UUID('db6ee519-afb5-4813-822b-bfbc7dfd2237'),
        SubQuery(
            or_=[
                SubQuery(not_=FieldFilter(field='title', value='climate')),
                SubQuery(
                    and_=[
                        FieldFilter(field='pub_year', value='2024', comp='>='),
                        LabelFilterInt(type='resolved', value_int=1, key='incl', comp='='),
                    ]
                ),
            ],
        ),
    ),
    # 4. AND of several same-table field filters (title AND abstract AND year range)
    (
        UUID('db6ee519-afb5-4813-822b-bfbc7dfd2237'),
        SubQuery(
            and_=[
                FieldFilter(field='title', value='climate'),
                FieldFilter(field='abstract', value='adaptation'),
                FieldFilter(field='pub_year', value='2015', comp='>='),
                FieldFilter(field='pub_year', value='2024', comp='<='),
            ]
        ),
    ),
    # 5. OR of many leaf branches (union-heavy)
    (
        UUID('db6ee519-afb5-4813-822b-bfbc7dfd2237'),
        SubQuery(
            or_=[
                FieldFilter(field='title', value='climate'),
                FieldFilter(field='title', value='warming'),
                FieldFilter(field='title', value='carbon'),
                FieldFilter(field='title', value='emission'),
            ]
        ),
    ),
    # 6. deep nesting: NOT ( (A AND B) OR NOT C )
    (
        UUID('db6ee519-afb5-4813-822b-bfbc7dfd2237'),
        SubQuery(
            not_=SubQuery(
                or_=[
                    SubQuery(
                        and_=[
                            FieldFilter(field='title', value='climate'),
                            LabelFilterBool(type='resolved', value_bool=True, key='incl'),
                        ]
                    ),
                    SubQuery(not_=FieldFilter(field='abstract', value='mitigation')),
                ]
            )
        ),
    ),
    # 7. assignment + annotation combined with a label filter
    (
        UUID('db6ee519-afb5-4813-822b-bfbc7dfd2237'),
        SubQuery(
            and_=[
                AssignmentFilter(mode=1),
                AnnotationFilter(incl=True),
                LabelFilterInt(type='user', value_int=1, key='rel', comp='='),
            ]
        ),
    ),
    # 8. field-multi (id list) OR abstract length constraint
    (
        UUID('db6ee519-afb5-4813-822b-bfbc7dfd2237'),
        SubQuery(
            or_=[
                FieldFilters(field='doi', values=['10.1000/abc', '10.1000/def']),
                AbstractFilter(comp='>', size=500),
            ]
        ),
    ),
    (
        UUID('c720711e-ce5b-4de7-a2ba-ef9365ff1127'),
        NQLFilterParser.validate_python(
            {
                'filter': 'sub',
                'and_': None,
                'or_': [
                    {
                        'filter': 'import',
                        'import_ids': [
                            {'incl': True, 'uuid': 'f7c22196-ac25-422a-9496-058ccfe035f1'},
                            {'incl': True, 'uuid': '6e9a5789-880b-40b5-a7bc-89640c9d357f'},
                            {'incl': True, 'uuid': '1b4fc151-1bc6-4af8-a937-3e6c46af8853'},
                        ],
                    },
                    {'filter': 'annotation', 'incl': True, 'scopes': None, 'scheme': None},
                ],
                'not_': None,
            }
        ),
    ),
    (
        UUID('0f92592b-86f7-4b7c-9970-4a4bbd07fdfe'),
        NQLFilterParser.validate_python(
            {
                'filter': 'sub',
                'and_': None,
                'or_': [
                    {'filter': 'annotation', 'incl': True, 'scopes': None, 'scheme': '71376bd4-f0bb-4aa0-850e-0e14b6e8a446'},
                    {'filter': 'field', 'field': 'pub_year', 'value': 2023, 'comp': '>='},
                ],
                'not_': None,
            }
        ),
    ),
    (
        UUID('6d1a8df2-c93a-4172-ba80-11f091d9393e'),
        NQLFilterParser.validate_python(
            {
                'filter': 'sub',
                'or_': None,
                'and_': [
                    {
                        'filter': 'import',
                        'import_ids': [
                            {'incl': True, 'uuid': 'f305a03a-8564-49b1-ab13-8ec31690dfb7'},
                            {'incl': False, 'uuid': '7563a1a0-7f24-45d1-b386-8843c5d7a3e8'},
                        ],
                    },
                    {'filter': 'assignment', 'mode': 4},
                ],
                'not_': None,
            }
        ),
    ),
]


async def main():
    settings = load_settings('config/secret.env')
    db_engine = get_engine_async(settings=settings.DB, debug=False)
    logger.info('Fetching wide export table...')

    async with db_engine.session() as session:  # type: AsyncSession
        for qi, (pid, nql) in enumerate(queries):
            print('=' * 80)
            print(f'QUERY {qi}: {nql}')
            print('=' * 80)

            stmt = nql_to_sql(query=nql, project_id=pid, project_type=ItemType.academic)
            sql = str(stmt.compile(dialect=dialect(), compile_kwargs={'literal_binds': True}))
            print('COMPILED OK, length', len(sql))
            print('has INTERSECT:', 'INTERSECT' in sql, '| UNION:', 'UNION' in sql, '| EXCEPT:', 'EXCEPT' in sql)

            print('-' * 80)
            print(sql)
            print('-' * 80)

            explain = await session.execute(sa.text(f'EXPLAIN (ANALYZE, BUFFERS, VERBOSE) {sql}'))
            for row in explain:
                print(row[0])


asyncio.run(main())
