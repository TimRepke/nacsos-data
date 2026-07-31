import asyncio
import typer
import re
import json

from enum import Enum
from pathlib import Path
from typing import Annotated, Any, Dict, List, Self
from datetime import datetime as dt

from nacsos_data.models.nql import NQLFilterParser
from nacsos_data.util.export.dict import (
    prepare_export_table,
)
from nacsos_data.util.export.file import get_author_names, write_csv, write_excel, write_jsonl, write_ris
from nacsos_data.util.export.util import LabelOptions

app = typer.Typer()


class ExportTypeEnum(str, Enum):
    CSV = ('CSV', 'Export to csv file')
    EXCEL = ('EXCEL', 'Export to excel file')
    RIS = ('RIS', 'Export to RIS file')
    JSONL = ('JSONL', 'Export to jsonl file')
    description: str

    def __new__(cls, value: str, description: str) -> Self:
        obj = str.__new__(cls, value)
        obj._value_ = value
        obj.description = description
        return obj

    @classmethod
    def help(cls) -> str:
        return '\n\n'.join([f'  {getattr(ExportTypeEnum, entry).value:<10} -> {getattr(ExportTypeEnum, entry).description}' for entry in cls.__members__])


# Logic source:
# - src/util/nql/grammar.ne:34-47
#   annotation_clause -> "HAS ANNOTATION"i ... -> { filter: "annotation", incl: true, scopes, scheme }
#   and "HAS NO ANNOTATION"i ... -> { filter: "annotation", incl: false, scopes, scheme }
# - src/util/nql/index.ts:59-69
#   parse(query) returns [] when parsing fails.
def parse_nql(query: str | None) -> List[Dict[str, Any]]:
    if query is None:
        return []

    text = query.strip()

    # Mirrors the grammar's optional:
    #   (IN uuids | null) and (WITH UUID | null)
    # without forcing any app-specific UUID parsing beyond token capture.
    pattern = re.compile(
        r'^(HAS\s+(?:NO\s+)?ANNOTATION)'
        r'(?:\s+IN\s+(?P<scopes>\[[^\]]*\]|\{[^}]*\}|.+?))?'
        r'(?:\s+WITH\s+(?P<scheme>\S+))?'
        r'$',
        re.IGNORECASE,
    )

    match = pattern.match(text)
    if not match:
        return []

    incl = not match.group(1).upper().startswith('HAS NO')
    result: Dict[str, Any] = {
        'filter': 'annotation',
        'incl': incl,
    }

    scopes_text = match.group('scopes')
    if scopes_text is not None:
        scopes_text = scopes_text.strip()
        if (scopes_text.startswith('[') and scopes_text.endswith(']')) or (scopes_text.startswith('{') and scopes_text.endswith('}')):
            scopes_text = scopes_text[1:-1].strip()

        if scopes_text:
            result['scopes'] = [part.strip() for part in scopes_text.split(',') if part.strip()]

    scheme = match.group('scheme')
    if scheme is not None:
        result['scheme'] = scheme

    return [result]


sample_request = {
    'labels': [],
    'nql_filter': {'filter': 'annotation', 'incl': True},
    'ignore_hierarchy': True,
    'ignore_repeat': True,
    'bot_annotation_metadata_ids': [],
    'assignment_scope_ids': [],
    'user_ids': ['7018e426-857e-4e2b-a800-9402c0532af2', 'ebc438e4-051c-46c8-9e92-7c4b019c6ef2'],
    'columns_to_drop': ['type', 'time_edited', 'project_id', 'title_slug', 'keywords', 'meta', 'authors_raw'],
    'project_id': '5a1fc4b5-7eef-4640-b863-7b506cc3dad6',
}

sample_request = {
    'labels': [
        {'key': 'climPolRel', 'options_bool': [False, True]},
        {'key': 'climRel', 'options_bool': [True]},
        {'key': 'evSyn', 'options_bool': [False, True]},
    ],
    'nql_filter': {'filter': 'annotation', 'incl': True},
    'ignore_hierarchy': True,
    'ignore_repeat': True,
    'bot_annotation_metadata_ids': [],
    'assignment_scope_ids': ['b2803c42-f355-4768-9065-988810a763ba'],
    'user_ids': [],
    'project_id': '5a1fc4b5-7eef-4640-b863-7b506cc3dad6',
}


@app.command('export', help='Export annotations into file', epilog=ExportTypeEnum.help())
def exporter(
    format: ExportTypeEnum,
    project_id: Annotated[str, typer.Option(help='Project ID')],
    # permissions: UserPermissions = Depends(UserPermissionChecker('annotations_read')),
    config_file: Annotated[Path, typer.Option(help='Path to config .env')],
    bot_annotation_metadata_ids: Annotated[
        str | None, typer.Option(help='Comma separated Bot Annotation Metadata IDs', rich_help_panel='Secondary Options')
    ] = None,
    assignment_scope_ids: Annotated[str | None, typer.Option(help='Comma separated Assignment Scope IDs', rich_help_panel='Secondary Options')] = None,
    user_ids: Annotated[str | None, typer.Option(help='Comma separated User IDs', rich_help_panel='Secondary Options')] = None,
    labels: Annotated[
        str | None,
        typer.Option(help=r'JSON formatted labels similar to [{"key": "climPolRel", "options_bool": \[false, true]}]', rich_help_panel='Secondary Options'),
    ] = None,
    nql_filter: Annotated[str, typer.Option(help='NQL query to use as filter', rich_help_panel='Secondary Options')] = 'HAS ANNOTATION',
    ignore_repeat: Annotated[bool, typer.Option(help='Ignore annotation order', rich_help_panel='Secondary Options')] = True,
    ignore_hierarchy: Annotated[bool, typer.Option(help='Ignore annotation hierarchy', rich_help_panel='Secondary Options')] = True,
    max_results: Annotated[int, typer.Option(help='Max results', rich_help_panel='Secondary Options', show_default=False, hidden=True)] = 15000,
    filename: Annotated[str | None, typer.Option(help='File name for the output file. Defaults to export_YYYYmmdd_HHMM.ext')] = None,
    all: Annotated[bool, typer.Option(help='Export all annotations from project')] = False,
    params: Annotated[str | None, typer.Option(help='JSON formatted extra params')] = None,
    loglevel: Annotated[str, typer.Option(help='Log level for importing (defaults to INFO)')] = 'INFO',
) -> None:

    from nacsos_data.util import async_essentials

    logger, settings, db_engine = async_essentials(loglevel=loglevel, config=config_file, logger_name='export', run_log_init=True)

    async def _run() -> Path:

        bot_annotation_metadata_ids_list = [id.strip() for id in bot_annotation_metadata_ids.split(',')] if bot_annotation_metadata_ids is not None else []
        assignment_scope_ids_list = [id.strip() for id in assignment_scope_ids.split(',')] if assignment_scope_ids is not None else []
        user_ids_list = [id.strip() for id in user_ids.split(',')] if user_ids is not None else []
        labels_list = json.loads(labels) if labels is not None else []
        labels_list = [LabelOptions(**label) for label in labels_list]

        nql_filter_ = parse_nql(nql_filter)

        if not nql_filter_:
            raise ValueError(f'Invalid NQL filter: {nql_filter!r}')

        nql_filters = NQLFilterParser.validate_python(nql_filter_[0])

        result: list[dict[str, Any]] = await prepare_export_table(
            bot_annotation_metadata_ids=bot_annotation_metadata_ids_list,
            assignment_scope_ids=assignment_scope_ids_list,
            user_ids=user_ids_list,
            project_id=project_id,
            labels=labels_list,
            nql_filter=nql_filters,
            ignore_repeat=ignore_repeat,
            ignore_hierarchy=ignore_hierarchy,
            db_engine=db_engine,
            max_results=max_results,
        )

        cols_to_drop = ['type', 'time_edited', 'project_id', 'title_slug', 'keywords', 'meta', 'authors_raw']
        result = [
            {k: v for k, v in (row | {'authors': get_author_names(row.get('authors')), 'authors_raw': row.get('authors')}).items() if k not in cols_to_drop}
            for row in result
        ]

        match format:
            case 'CSV':
                fp = write_csv(result)
            case 'EXCEL':
                fp = write_excel(result)
            case 'RIS':
                fp = write_ris(result, labels_list)
            case 'JSONL':
                fp = write_jsonl(result)
            case _:
                raise Exception(f"Requested export format '{format}' is not one of the recognized formats: ['CSV', 'EXCEL', 'RIS', 'JSONL']")

        logger.info('Got result from DB, now writing to file...')

        src = Path(fp)
        logger.debug(f'Temp file path: {src}')

        if filename is None:
            now = dt.now().strftime('%Y%m%d_%H%M')
            dest = Path(f'export_{now}{src.suffix}')
        else:
            dest = Path(filename)
        dest.parent.mkdir(parents=True, exist_ok=True)

        with open(src, 'rb') as fsrc, open(dest, 'wb') as fdst:
            fdst.write(fsrc.read())

        # remove the temp file after copy
        src.unlink()

        logger.info(f'File path: {dest}')
        return dest

    asyncio.run(_run())
