from pathlib import Path
from typing import Annotated

from nacsos_data.util.academic.apis import (
    DimensionsAPI,
    WoSAPI,
    OpenAlexAPI,
    OpenAlexSolrAPI,
    PubmedAPI,
    ScopusAPI,
    APIEnum,
    APIMap,
)


def run():
    import typer
    import logging

    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(name)s (%(process)d): %(message)s', level='DEBUG')

    app = typer.Typer()

    @app.command()
    def download(
            kind: APIEnum,
            target: Annotated[Path, typer.Option(help='File to write results to')],
            api_key: Annotated[str | None, typer.Option(help='Valid API key')] = None,
            openalex_endpoint: Annotated[str | None, typer.Option(help='solr endpoint')] = None,
            batch_size: Annotated[int, typer.Option(help='File to write results to')] = 5,
            page_size: int = 5,
            database: str = 'WOK',
            proxy: str | None = None,
            query_file: Annotated[Path | None, typer.Option(help='File containing search query')] = None,
            query: Annotated[str | None, typer.Option(help='Search query')] = None,
    ) -> None:
        if query_file:
            with open(query_file, 'r') as qf:
                query_str = qf.read()
        elif query:
            query_str = query
        else:
            raise AttributeError('Must provide either `query_file` or `query`')

        if kind == APIEnum.SOLR and openalex_endpoint is not None:
            api = OpenAlexSolrAPI(api_key='', openalex_endpoint=openalex_endpoint, batch_size=batch_size)
        elif kind == APIEnum.OA and api_key is not None:
            api = OpenAlexAPI(api_key=api_key)
        elif kind == APIEnum.WOS and api_key is not None:
            api = WoSAPI(api_key=api_key, proxy=proxy, page_size=page_size, database=database)
        elif kind == APIEnum.DIMENSIONS and api_key is not None:
            api = DimensionsAPI(api_key=api_key, proxy=proxy, page_size=page_size)
        elif kind == APIEnum.PUBMED and api_key is not None:
            api = PubmedAPI(api_key=api_key, proxy=proxy)
        elif kind == APIEnum.SCOPUS and api_key is not None:
            api = ScopusAPI(api_key=api_key, proxy=proxy)
        else:
            raise AttributeError('No.')
        api.download_raw(query=query_str, target=target)

    @app.command()
    def convert(
            kind: APIEnum,
            source: Annotated[Path, typer.Option(help='File to read results from')],
            target: Annotated[Path, typer.Option(help='File to write results to')],
    ):
        APIMap[kind].convert_file(source=source, target=target)

    app()
