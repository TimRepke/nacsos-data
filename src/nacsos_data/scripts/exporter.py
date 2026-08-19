import asyncio
import typer
import json
import logging

from enum import Enum
from pathlib import Path
from typing import Annotated, Any, Self
from datetime import datetime as dt
from dotenv import load_dotenv

from nacsos_data.db.connection import DatabaseEngineAsync
from nacsos_data.models.nql import NQLFilterParser
from nacsos_data.util.export.dict import (
    prepare_export_table,
    get_project_labels,
    get_project_scopes,
    get_project_bot_scopes,
    get_project_users,
    get_project_schemes,
)
from nacsos_data.util.export.file import get_author_names, write_csv, write_excel, write_jsonl, write_ris, DEFAULT_COLUMNS_TO_DROP
from nacsos_data.util.export.util import LabelOptions
from nacsos_data.util import async_essentials

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


async def _set_user_filters(export_all: bool, project_id: str, db_engine: DatabaseEngineAsync, users: str | None, logger: logging.Logger) -> list[str]:
    if users is not None:
        users_list = [id.strip() for id in users.split(',')]
    elif export_all and users is None:
        users_ = await get_project_users(project_id=project_id, db_engine=db_engine)
        logger.info(f'User names and IDs: {users_}')
        users_list = [str(user.id) for user in users_]
    else:
        users_list = []
    logger.debug(f'User IDs: {users_list}')
    return users_list


async def _set_scope_filters(
    export_all: bool, project_id: str, annotation_scheme: str, db_engine: DatabaseEngineAsync, scopes: str | None, logger: logging.Logger
) -> list[str]:
    if scopes is not None:
        scopes_list = [id.strip() for id in scopes.split(',')]
    elif export_all and scopes is None:
        scopes_ = await get_project_scopes(project_id=project_id, db_engine=db_engine)
        logger.info(f'Assignment scope names and IDs: {scopes_}')
        scopes_list = [str(scope.id) for scope in scopes_ if scope.scheme_id == annotation_scheme]
    else:
        scopes_list = []
    logger.debug(f'Assignment scope IDs: {scopes_list}')
    return scopes_list


async def _set_bot_scope_filters(
    export_all: bool, project_id: str, annotation_scheme: str, db_engine: DatabaseEngineAsync, bot_scopes: str | None, logger: logging.Logger
) -> list[str]:
    if bot_scopes is not None:
        bot_scopes_list = [id.strip() for id in bot_scopes.split(',')]
    elif export_all and bot_scopes is None:
        bot_scopes_ = await get_project_bot_scopes(project_id=project_id, db_engine=db_engine)
        logger.info(f'Bot annotation metadata names and IDs: {bot_scopes_}')
        bot_scopes_list = [str(scope.id) for scope in bot_scopes_ if scope.scheme_id == annotation_scheme]
    else:
        bot_scopes_list = []
    logger.debug(f'Bot annotation metadata IDs: {bot_scopes_list}')
    return bot_scopes_list


async def _set_label_filters(
    export_all: bool, project_id: str, db_engine: DatabaseEngineAsync, labels: str | None, logger: logging.Logger
) -> list[LabelOptions]:
    if labels is not None:
        labels_list = json.loads(labels)
        labels_list = [LabelOptions(**label) for label in labels_list]
    elif export_all and labels is None:
        labels_list = await get_project_labels(project_id=project_id, db_engine=db_engine)
        labels_list = list(labels_list.values())
    else:
        labels_list = []
    logger.info(f'Labels: {labels_list}')
    return labels_list


@app.command(
    'generate-config',
    help='Generate a config file that lists all IDs in given project. File is saved to config/export_options.env and read from this path when generating exports. Modify the config file to change the filters for the export.',
    epilog=ExportTypeEnum.help(),
)
def generate_config(
    project: Annotated[str, typer.Option(help='Project ID')],
    credentials_file: Annotated[Path, typer.Option(help='Path to credentials configuration .env')],
    annotation_scheme: Annotated[str | None, typer.Option(help='Annotation Scheme ID')] = None,
    loglevel: Annotated[str, typer.Option(help='Log level for importing (defaults to INFO)')] = 'INFO',
) -> None:
    logger, settings, db_engine = async_essentials(loglevel=loglevel, config=credentials_file, logger_name='export_config', run_log_init=True)

    async def _run() -> None:

        # require annotation_scheme but list possible ones to help
        if annotation_scheme is None:
            schemes = await get_project_schemes(project_id=project, db_engine=db_engine)
            schemes_msg = '\n'.join([f'name: {scheme.name}, id: {scheme.id}' for scheme in schemes])
            msg = (
                'Please select an annotation scheme to start exporting, re-run command with adding flag: \n'
                '--annotation-scheme-id <scheme-id> \n Available schemes are: \n' + schemes_msg
            )
            raise typer.BadParameter(msg)

        async with asyncio.TaskGroup() as tg:
            task_users = tg.create_task(_set_user_filters(export_all=True, project_id=project, db_engine=db_engine, users=None, logger=logger))
            task_scopes = tg.create_task(
                _set_scope_filters(export_all=True, project_id=project, annotation_scheme=annotation_scheme, db_engine=db_engine, scopes=None, logger=logger)
            )
            task_bot_scopes = tg.create_task(
                _set_bot_scope_filters(
                    export_all=True, project_id=project, annotation_scheme=annotation_scheme, db_engine=db_engine, bot_scopes=None, logger=logger
                )
            )
            task_labels = tg.create_task(_set_label_filters(export_all=True, project_id=project, db_engine=db_engine, labels=None, logger=logger))

        users = task_users.result()
        scopes = task_scopes.result()
        bot_scopes = task_bot_scopes.result()
        labels = task_labels.result()

        with open('config/export_options.env', 'w') as config:
            config.write(
                '## Options configuration for exports. Lists all IDs found in project. If you want to remove an ID from results, simply remove it. All values in this file can be overwritten with CLI flags.\n\n'
            )
            config.write('### Options\n')
            config.write(f'PROJECT = {project}\n')
            config.write(f'ANNOTATION_SCHEME = {annotation_scheme}\n')
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


load_dotenv(Path('config/export_options.env'))


@app.command(
    'run',
    help='Export items & annotations into file. Utilizes config/export_options.env if it exists. Precedence for options: CLI flag > config file > default',
    epilog=ExportTypeEnum.help(),
)
def exporter(
    format: ExportTypeEnum,
    project: Annotated[str, typer.Option(envvar='PROJECT', help='Project ID')],
    credentials_file: Annotated[Path, typer.Option(envvar='CREDENTIALS_FILE', help='Path to credentials configuration .env')],
    annotation_scheme: Annotated[str | None, typer.Option(envvar='ANNOTATION_SCHEME', help='Annotation Scheme ID')] = None,
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
    scopes: Annotated[str | None, typer.Option(envvar='SCOPES', help='Comma separated Assignment Scope IDs', rich_help_panel='Secondary Options')] = None,
    bot_scopes: Annotated[
        str | None, typer.Option(envvar='BOT_SCOPES', help='Comma separated Bot Annotation Metadata IDs', rich_help_panel='Secondary Options')
    ] = None,
    users: Annotated[str | None, typer.Option(envvar='USERS', help='Comma separated User IDs', rich_help_panel='Secondary Options')] = None,
    labels: Annotated[
        str | None,
        typer.Option(
            envvar='LABELS',
            help=r'JSON formatted labels similar to [{"key": "climPolRel", "options_bool": \[false, true]}]',
            rich_help_panel='Secondary Options',
        ),
    ] = None,
) -> None:

    logger, settings, db_engine = async_essentials(loglevel=loglevel, config=credentials_file, logger_name='export', run_log_init=True)

    # Precedence: CLI flag > env var > default

    async def _run() -> Path:

        # require annotation_scheme but list possible ones to help
        if annotation_scheme is None:
            schemes = await get_project_schemes(project_id=project, db_engine=db_engine)
            schemes_msg = '\n'.join([f'name: {scheme.name}, id: {scheme.id}' for scheme in schemes])
            msg = (
                'Please select an annotation scheme to start exporting, re-run command with adding flag: \n'
                '--annotation-scheme-id <scheme-id> \n Available schemes are: \n' + schemes_msg
            )
            raise typer.BadParameter(msg)

        async with asyncio.TaskGroup() as tg:
            task_users = tg.create_task(_set_user_filters(export_all, project, db_engine, users, logger))
            task_scopes = tg.create_task(_set_scope_filters(export_all, project, annotation_scheme, db_engine, scopes, logger))
            task_bot_scopes = tg.create_task(_set_bot_scope_filters(export_all, project, annotation_scheme, db_engine, bot_scopes, logger))
            task_labels = tg.create_task(_set_label_filters(export_all, project, db_engine, labels, logger))

        nql_filter = NQLFilterParser.validate_python({'filter': 'annotation', 'incl': True}) if has_annotation else None

        result: list[dict[str, Any]] = await prepare_export_table(
            bot_annotation_metadata_ids=task_bot_scopes.result(),
            assignment_scope_ids=task_scopes.result(),
            user_ids=task_users.result(),
            project_id=project,
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

        match format:
            case 'CSV':
                fp = write_csv(result)
            case 'EXCEL':
                fp = write_excel(result)
            case 'RIS':
                fp = write_ris(result, task_labels.result())
            case 'JSONL':
                fp = write_jsonl(result)
            case _:
                raise typer.BadParameter(f"Requested export format '{format}' is not one of the recognized formats: ['CSV', 'EXCEL', 'RIS', 'JSONL']")

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
