from .shared import (
    FIELDS_API,
    FIELDS_META,
    FIELDS_CUSTOM,
    FIELDS_SOLR,
    FIELDS_REDUNDANT,
    NESTED_FIELDS,
    translate_work_to_solr,
    translate_work_to_item,
    translate_authorship,
)
from .solr import OpenAlexSolrAPI, wildcards
from .api import OpenAlexAPI

__all__ = [
    'FIELDS_API',
    'FIELDS_META',
    'FIELDS_CUSTOM',
    'FIELDS_SOLR',
    'FIELDS_REDUNDANT',
    'NESTED_FIELDS',
    'OpenAlexAPI',
    'OpenAlexSolrAPI',
    'wildcards',
    'translate_work_to_solr',
    'translate_work_to_item',
    'translate_authorship',
]

# if __name__ == '__main__':
#     import typer
#
#     logging.basicConfig(format='%(asctime)s [%(levelname)s] %(name)s (%(process)d): %(message)s', level='DEBUG')
#
#     fin = Path('../../../../../scratch/snippet.jsonl').resolve()
#     print(fin)
#     with open(fin) as f:
#         for li, line in enumerate(f):
#             print(li)
#             work_ = WorksSchema.model_validate_json(line)
#             # print(work.model_dump(exclude_unset=True, exclude_none=True))
#             print(translate_work_to_solr(work_))
#
#     app = typer.Typer()
#
#     @app.command()
#     def download(
#         target: Annotated[Path, typer.Option(help='File to write results to')],
#         api_key: Annotated[str | None, typer.Option(help='Valid API key')] = None,
#         kind: Annotated[Literal['SOLR', 'API'], typer.Option(help='database to use')] = 'SOLR',
#         openalex_conf: Annotated[str | None, typer.Option(help='NACSOS config with solr settings')] = None,
#         batch_size: Annotated[int, typer.Option(help='File to write results to')] = 5,
#         query_file: Annotated[Path | None, typer.Option(help='File containing search query')] = None,
#         query: Annotated[str | None, typer.Option(help='Search query')] = None,
#     ) -> None:
#         if query_file:
#             with open(query_file, 'r') as qf:
#                 query_str = qf.read()
#         elif query:
#             query_str = query
#         else:
#             raise AttributeError('Must provide either `query_file` or `query`')
#
#         conf = load_settings(conf_file=openalex_conf)
#
#         api: OpenAlexSolrAPI | OpenAlexAPI
#         if kind == 'SOLR':
#             api = OpenAlexSolrAPI(openalex_conf=conf.OPENALEX, batch_size=batch_size)
#         elif kind == 'API':
#             api = OpenAlexAPI(api_key=api_key or conf.OPENALEX.API_KEY)
#         else:
#             raise AttributeError(f'Unknown API type: {kind}')
#         api.download_raw(query=query_str, target=target)
#
#     @app.command()
#     def convert(
#         source: Annotated[Path, typer.Option(help='File to read results from')],
#         target: Annotated[Path, typer.Option(help='File to write results to')],
#         kind: Annotated[Literal['SOLR', 'API'], typer.Option(help='database to use')] = 'SOLR',
#     ) -> None:
#         cls = OpenAlexSolrAPI if kind == 'SOLR' else OpenAlexAPI
#         cls.convert_file(source=source, target=target)
#
#     @app.command()
#     def translate(kind: Annotated[Literal['SOLR', 'API'], typer.Option(help='database to use')] = 'SOLR') -> None:
#         cls = OpenAlexSolrAPI if kind == 'SOLR' else OpenAlexAPI
#         for fp in []:  # type: ignore[var-annotated]
#             for item in cls.read_translated(Path(fp)):
#                 print(item)
