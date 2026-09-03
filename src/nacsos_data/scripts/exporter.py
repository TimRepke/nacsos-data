import asyncio
import typer
import json

from enum import Enum
from pathlib import Path
from typing import Annotated, Any, Self
from datetime import datetime as dt
from dotenv import load_dotenv

from nacsos_data.db.connection import DatabaseEngineAsync
from nacsos_data.db.crud.annotations import (
    read_assignment_scopes_for_scheme_info,
    read_annotation_schemes_for_project_info,
    read_resolution_scopes_for_scheme_info,
)
from nacsos_data.db.crud.users import read_users
from nacsos_data.models.nql import NQLFilterParser
from nacsos_data.util.export.dict import (
    prepare_export_table,
    get_project_labels,
)
from nacsos_data.util.export.file import get_author_names, write_csv, write_excel, write_jsonl, write_ris, DEFAULT_COLUMNS_TO_DROP
from nacsos_data.util.export.util import LabelOptions
from nacsos_data.util import async_essentials, pluck

app = typer.Typer()


class ExportTypeEnum(str, Enum):
    csv = ('csv', 'Export to csv file')
    excel = ('excel', 'Export to excel file')
    ris = ('ris', 'Export to RIS file')
    jsonl = ('jsonl', 'Export to jsonl file')
    description: str

    def __new__(cls, value: str, description: str) -> Self:
        obj = str.__new__(cls, value)
        obj._value_ = value
        obj.description = description
        return obj

    @classmethod
    def help(cls) -> str:
        return '\n\n'.join([f'  {getattr(ExportTypeEnum, entry).value:<10} -> {getattr(ExportTypeEnum, entry).description}' for entry in cls.__members__])


async def _prepare_user_ids(
    db_engine: DatabaseEngineAsync,
    export_all: bool = True,
    user_ids: str | None = None,
    project_id: str | None = None,
) -> list[str]:
    if user_ids is not None:
        return [user_id.strip() for user_id in user_ids.split(',')]
    if not export_all:
        return []
    if not project_id:
        raise ValueError('Need to provide project_id!')
    users = (await read_users(project_id=project_id, order_by_username=True, engine=db_engine)) or []
    return [str(user_id) for user_id in pluck(users, 'user_id')]


async def _prepare_scope_ids(
    db_engine: DatabaseEngineAsync,
    export_all: bool = True,
    annotation_scheme_id: str | None = None,
    scope_ids: str | None = None,
) -> list[str]:
    if scope_ids is not None:
        return [scope_id.strip() for scope_id in scope_ids.split(',')]
    if not export_all:
        return []
    if not annotation_scheme_id:
        raise ValueError('Need to provide scheme_id!')
    assignment_scopes = await read_assignment_scopes_for_scheme_info(annotation_scheme_id=annotation_scheme_id, db_engine=db_engine)
    return [str(scope_id) for scope_id in pluck(assignment_scopes, 'assignment_scope_id')]


async def _prepare_bot_scope_ids(
    db_engine: DatabaseEngineAsync,
    export_all: bool = True,
    annotation_scheme_id: str | None = None,
    scope_ids: str | None = None,
) -> list[str]:
    if scope_ids is not None:
        return [scope_id.strip() for scope_id in scope_ids.split(',')]
    if not export_all:
        return []
    if not annotation_scheme_id:
        raise ValueError('Need to provide scheme_id!')
    bot_scopes = await read_resolution_scopes_for_scheme_info(annotation_scheme_id=annotation_scheme_id, db_engine=db_engine)
    return [str(scope_id) for scope_id in pluck(bot_scopes, 'bot_annotation_metadata_id')]


async def _prepare_labels(
    db_engine: DatabaseEngineAsync,
    export_all: bool = True,
    project_id: str | None = None,
    labels: str | None = None,
) -> list[LabelOptions]:
    if labels is not None:
        return [LabelOptions(**label) for label in json.loads(labels)]
    if not export_all:
        return []
    if not project_id:
        raise ValueError('Need to provide project_id!')
    return list((await get_project_labels(project_id=project_id, db_engine=db_engine)).values())


async def _prepare_annotation_scheme_id(db_engine: DatabaseEngineAsync, annotation_scheme_id: str | None = None, project_id: str | None = None) -> str:
    # require annotation_scheme but list possible ones to help
    if annotation_scheme_id is None:
        if not project_id:
            raise ValueError('Need to provide project_id!')
        schemes = await read_annotation_schemes_for_project_info(project_id=project_id, db_engine=db_engine)
        schemes_msg = '\n'.join([f'name: {scheme.name}, scheme_id: {scheme.annotation_scheme_id}' for scheme in schemes])
        msg = (
            'Please select an annotation scheme to start exporting, re-run command with adding flag: \n'
            '--annotation-scheme <scheme-id> \n Available schemes are: \n' + schemes_msg
        )
        raise typer.BadParameter(msg)
    return annotation_scheme_id


def _load_export_config(config_file: Path) -> Path:
    load_dotenv(config_file)
    return config_file


@app.command(
    'generate-config',
    help='Generate a config file that lists all IDs in given project. Modify the config file to change the filters for the export.',
    epilog=ExportTypeEnum.help(),
)
def generate_config(
    project_id: Annotated[str, typer.Option(help='Project ID')],
    credentials_file: Annotated[Path, typer.Option(help='Path to credentials configuration .env')],
    scheme_id: Annotated[str | None, typer.Option(help='Annotation Scheme ID')] = None,
    config_file: Annotated[Path, typer.Option(help='Path to write options config')] = Path('config/export_options.env'),
    loglevel: Annotated[str, typer.Option(help='Log level for importing (defaults to INFO)')] = 'INFO',
) -> None:
    logger, _, db_engine = async_essentials(loglevel=loglevel, config=credentials_file, logger_name='export_config', run_log_init=True)

    async def _run() -> None:
        annotation_scheme_id = await _prepare_annotation_scheme_id(annotation_scheme_id=scheme_id, project_id=project_id, db_engine=db_engine)

        async with asyncio.TaskGroup() as tg:
            task_users = tg.create_task(_prepare_user_ids(export_all=True, project_id=project_id, db_engine=db_engine))
            task_scopes = tg.create_task(_prepare_scope_ids(export_all=True, annotation_scheme_id=annotation_scheme_id, db_engine=db_engine))
            task_bot_scopes = tg.create_task(_prepare_bot_scope_ids(export_all=True, annotation_scheme_id=annotation_scheme_id, db_engine=db_engine))
            task_labels = tg.create_task(_prepare_labels(export_all=True, project_id=project_id, db_engine=db_engine))

        users = task_users.result()
        scopes = task_scopes.result()
        bot_scopes = task_bot_scopes.result()
        labels = task_labels.result()

        config_file.parent.mkdir(parents=True, exist_ok=True)

        with open(config_file, 'w') as config:
            config.write(
                '## Options configuration for exports. Lists all IDs found in project. If you want to remove an ID from results, simply remove it. All values in this file can be overwritten with CLI flags.\n\n'
            )
            config.write('### Options\n')
            config.write(f'PROJECT = {project_id}\n')
            config.write(f'ANNOTATION_SCHEME = {annotation_scheme_id}\n')
            config.write(f'CREDENTIALS_FILE = {credentials_file}\n')
            config.write('# OUT = export_YYYYmmdd_HHMM.ext\n')
            config.write('EXPORT_ALL = False\n')
            config.write('LOGLEVEL = INFO\n')
            config.write('\n')
            config.write('### Secondary Options\n')
            config.write('HAS_ANNOTATION = True\n')
            config.write(f'COLUMNS_TO_DROP = {",".join(DEFAULT_COLUMNS_TO_DROP)}\n')
            config.write('IGNORE_REPEAT = True\n')
            config.write('IGNORE_HIERARCHY = True\n')
            config.write('MAX_RESULTS = 15000\n')
            config.write('\n')
            config.write(f'USERS = {",".join(users)}\n')
            config.write(f'SCOPES = {",".join(scopes)}\n')
            config.write(f'BOT_SCOPES = {",".join(bot_scopes)}\n')
            config.write(f'LABELS = {json.dumps([l.model_dump() for l in labels])}\n')

    asyncio.run(_run())


@app.command(
    'run',
    help='Export items & annotations into file. Can optionally utilise the config file generated through `generate-config` command. Precedence for options: CLI flag > config file > default',
    epilog=ExportTypeEnum.help(),
)
def exporter(
    export_format: ExportTypeEnum,
    project_id: Annotated[str, typer.Option(envvar='PROJECT', help='Project ID')],
    credentials_file: Annotated[Path, typer.Option(envvar='CREDENTIALS_FILE', help='Path to credentials configuration .env')],
    scheme_id: Annotated[str | None, typer.Option(envvar='ANNOTATION_SCHEME', help='Annotation Scheme ID')] = None,
    config_file: Annotated[
        Path | None,
        typer.Option(
            callback=_load_export_config,
            is_eager=True,  # to force processing of this CLI parameter before the others
            help='Path to options configuration .env. If not specified, only flags will be used. If specified together with flags, precedence is: flag > config file > default.',
        ),
    ] = None,
    out: Annotated[str | None, typer.Option(envvar='OUT', help='File name for the output file. Defaults to export_YYYYmmdd_HHMM.ext')] = None,
    export_all: Annotated[
        bool,
        typer.Option(
            envvar='EXPORT_ALL', help='If secondary options are not specified, find all available items & annotations from Annotation Scheme ID and export all'
        ),
    ] = False,
    loglevel: Annotated[str, typer.Option(envvar='LOGLEVEL', help='Log level for importing (defaults to INFO)')] = 'INFO',
    has_annotation: Annotated[
        bool, typer.Option(envvar='HAS_ANNOTATION', help='Export items with annotations only', rich_help_panel='Secondary Options')
    ] = True,
    columns_to_drop: Annotated[
        str, typer.Option(envvar='COLUMNS_TO_DROP', help='Comma separated column names that are not wanted in the export', rich_help_panel='Secondary Options')
    ] = ','.join(DEFAULT_COLUMNS_TO_DROP),
    ignore_repeat: Annotated[bool, typer.Option(envvar='IGNORE_REPEAT', help='Ignore annotation order', rich_help_panel='Secondary Options')] = True,
    ignore_hierarchy: Annotated[bool, typer.Option(envvar='IGNORE_HIERARCHY', help='Ignore annotation hierarchy', rich_help_panel='Secondary Options')] = True,
    max_results: Annotated[
        int, typer.Option(envvar='MAX_RESULTS', help='Max results', rich_help_panel='Secondary Options', show_default=False, hidden=True)
    ] = 15000,
    scope_ids: Annotated[str | None, typer.Option(envvar='SCOPES', help='Comma separated Assignment Scope IDs', rich_help_panel='Secondary Options')] = None,
    bot_scope_ids: Annotated[
        str | None, typer.Option(envvar='BOT_SCOPES', help='Comma separated Bot Annotation Metadata IDs', rich_help_panel='Secondary Options')
    ] = None,
    user_ids: Annotated[str | None, typer.Option(envvar='USERS', help='Comma separated User IDs', rich_help_panel='Secondary Options')] = None,
    labels: Annotated[
        str | None,
        typer.Option(
            envvar='LABELS',
            help=r'JSON formatted labels similar to [{"key": "climPolRel", "options_bool": \[false, true]}]',
            rich_help_panel='Secondary Options',
        ),
    ] = None,
) -> None:

    logger, _, db_engine = async_essentials(loglevel=loglevel, config=credentials_file, logger_name='export', run_log_init=True)

    # Precedence: CLI flag > env var > default

    async def _run() -> Path:

        annotation_scheme_id = await _prepare_annotation_scheme_id(annotation_scheme_id=scheme_id, project_id=project_id, db_engine=db_engine)

        async with asyncio.TaskGroup() as tg:
            task_users = tg.create_task(_prepare_user_ids(export_all=export_all, project_id=project_id, user_ids=user_ids, db_engine=db_engine))
            task_scopes = tg.create_task(
                _prepare_scope_ids(export_all=export_all, annotation_scheme_id=annotation_scheme_id, scope_ids=scope_ids, db_engine=db_engine)
            )
            task_bot_scopes = tg.create_task(
                _prepare_bot_scope_ids(export_all=export_all, annotation_scheme_id=annotation_scheme_id, scope_ids=bot_scope_ids, db_engine=db_engine)
            )
            task_labels = tg.create_task(_prepare_labels(export_all=export_all, project_id=project_id, labels=labels, db_engine=db_engine))

        nql_filter = NQLFilterParser.validate_python({'filter': 'annotation', 'incl': True}) if has_annotation else None

        result: list[dict[str, Any]] = await prepare_export_table(
            bot_annotation_metadata_ids=task_bot_scopes.result(),
            assignment_scope_ids=task_scopes.result(),
            user_ids=task_users.result(),
            project_id=project_id,
            labels=task_labels.result(),
            nql_filter=nql_filter,
            ignore_repeat=ignore_repeat,
            ignore_hierarchy=ignore_hierarchy,
            db_engine=db_engine,
            max_results=max_results,
        )

        cols_to_drop = [col.strip() for col in columns_to_drop.split(',')]
        result = [
            {k: v for k, v in (row | {'authors': get_author_names(row.get('authors')), 'authors_raw': row.get('authors')}).items() if k not in cols_to_drop}
            for row in result
        ]

        match export_format:
            case 'csv':
                fp = write_csv(result)
            case 'excel':
                fp = write_excel(result)
            case 'ris':
                fp = write_ris(result, task_labels.result())
            case 'jsonl':
                fp = write_jsonl(result)

        logger.info('Got result from DB, now writing to file...')

        # src: temp file, dest: file to save
        src = Path(fp)
        logger.debug(f'Temp file path: {src}')

        if out is None:
            now = dt.now().strftime('%Y%m%d_%H%M')
            dest = Path(f'export_{now}{src.suffix}')
        else:
            dest = Path(out)
        dest.parent.mkdir(parents=True, exist_ok=True)

        with open(src, 'rb') as fsrc, open(dest, 'wb') as fdst:
            fdst.write(fsrc.read())

        # remove the temp file after copy
        src.unlink()

        logger.info(f'File path: {dest}')
        return dest

    asyncio.run(_run())
