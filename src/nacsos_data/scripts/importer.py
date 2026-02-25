import uuid
import json
import logging
import asyncio
from enum import Enum
from pathlib import Path
from datetime import datetime
from typing import Annotated, Any

import typer
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from nacsos_data.db.engine import ensure_session_async
from nacsos_data.db.schemas import Import, Project, ItemType
from nacsos_data.db.schemas.imports import ImportRevision
from nacsos_data.util.academic.importer import (
    import_scopus_csv_file,
    import_openalex,
    import_wos_files,
    import_openalex_files,
    import_academic_db,
)
from nacsos_data.util.import_generic import import_generic

app = typer.Typer()


class ImportTypeEnum(str, Enum):
    SOLR = ('SOLR', 'Direct import from OpenAlex solr')
    OA = ('OA', 'Import from OpenAlex file (API or solr format)')
    WOS = ('WOS', 'Import from WoS txt files')
    SCOPUS = ('SCOPUS', 'Import from Scopus csv')
    ACADEMIC = ('ACADEMIC', 'Import from JSONl files containing `AcademicItemModel`s')
    GENERIC = ('GENERIC', 'Import generic items')
    LEXIS = ('LEXIS', 'Import from lexisnexis dump')

    def __new__(cls, value, description):
        obj = str.__new__(cls, value)
        obj._value_ = value
        obj.description = description
        return obj

    @classmethod
    def help(cls) -> str:
        return '\n\n'.join([f'  {getattr(ImportTypeEnum, entry).value:<10} -> {getattr(ImportTypeEnum, entry).description}' for entry in cls.__members__])


def _sources(path: Path, extension: str = 'csv') -> list[Path]:
    """Make sure source is list of files"""
    if not path.exists():
        raise FileNotFoundError(f'Does not exist: {path.resolve()}')

    if path.is_dir():
        sources = list(path.glob(f'*.{extension}'))
        if len(sources) == 0:
            raise FileNotFoundError(f'Empty folder: {path.resolve()}')
        return sources

    if path.is_file():
        return [path]

    raise RuntimeError('path is neither directory nor file')


async def _ensure_import(
    session: AsyncSession,
    logger: logging.Logger,
    import_id: str | None = None,
    project_id: str | None = None,
    name: str | None = None,
    description: str | None = None,
    user_id: str | None = None,
    num_items: int = 0,
    with_revision: bool = False,
) -> str:
    """Create or use existing import"""

    if import_id is not None:
        import_ = await session.scalar(sa.select(Import).where(Import.import_id == import_id, Import.project_id == project_id))
        if import_ is not None:
            return import_id
        raise RuntimeError(f'Import id {import_id} not found in project {project_id}')

    if name is not None and description is not None and user_id is not None:
        logger.info('Creating import and revision')
        import_id = uuid.uuid4()

        imp = Import(
            import_id=import_id,
            project_id=project_id,
            user_id=user_id,
            type='SCRIPT',
            name=name,
            description=description,
            time_created=datetime.now(),
        )
        session.add(imp)
        await session.flush()

        if with_revision:
            rev_id = uuid.uuid4()
            rev = ImportRevision(
                import_revision_id=rev_id,
                import_id=import_id,
                import_revision_counter=1,
                num_items=num_items,
                num_items_new=num_items,
                num_items_retrieved=num_items,
                num_items_removed=0,
                num_items_updated=0,
            )
            session.add(rev)
            await session.flush()
        return str(import_id)

    raise RuntimeError('Invalid options for import ensurer')


@ensure_session_async
async def _ensure_project_type(session: AsyncSession, expected_type: ItemType, project_id: str) -> bool:
    """Check that the project type matches the import kind"""
    project = await session.scalar(sa.select(Project).where(Project.project_id == project_id))
    if project is None:
        raise RuntimeError(f'No project with id {project_id}')
    if project.type != expected_type:
        raise RuntimeError(f'Project type {project.type} does not match expected type {expected_type}')
    return True


@app.command('import', help='Import data into the platform', epilog=ImportTypeEnum.help())
def importer(
    kind: ImportTypeEnum,
    source: Annotated[Path, typer.Option(help='Data source (single file, folder with files, or txt file containing solr query)')],
    project_id: Annotated[str, typer.Option(help='Project ID')],
    config_file: Annotated[Path, typer.Option(help='Path to config .env')],
    import_id: Annotated[str | None, typer.Option(help='ID of existing import (will ignore `name` and `description`)')] = None,
    name: Annotated[str | None, typer.Option(help='Name for import (requires `description`)')] = None,
    description: Annotated[str | None, typer.Option(help='Description for import (requires `name`)')] = None,
    params: Annotated[str | None, typer.Option(help='JSON formatted extra params')] = None,
    user_id: Annotated[str, typer.Option(help='User ID for import (defaults to Tim)')] = 'fd641232-bada-466e-9a3b-fb12038f5508',
    loglevel: Annotated[str, typer.Option(help='Log level for importing (defaults to INFO)')] = 'INFO',
):
    from nacsos_data.util import async_essentials

    logger, settings, db_engine = async_essentials(loglevel=loglevel, config=config_file, logger_name='import', run_log_init=True)

    async def _ensure_committed_import():
        logger.info('Preparing import')
        async with db_engine.session() as session:
            import_id_ = await _ensure_import(
                session=session,
                logger=logger,
                import_id=import_id,
                name=name,
                description=description,
                user_id=user_id,
                project_id=project_id,
                with_revision=False,
            )
            await session.commit()  # FIXME: ideally, we'd flush and pass the session on to nothing remains on fail
        return import_id_

    async def _run():
        logger.info(f'Unwrapping sources for: {source}')

        if kind == ImportTypeEnum.WOS:
            sources = _sources(source, extension='txt')
            logger.info(f'Importing WOS from {sources}')
            import_id_ = await _ensure_committed_import()
            await _ensure_project_type(db_engine=db_engine, project_id=project_id, expected_type=ItemType.academic)
            await import_wos_files(
                sources=sources,
                project_id=project_id,
                import_id=import_id_,
                db_config=config_file,
                logger=logger,
            )

        elif kind == ImportTypeEnum.SCOPUS:
            sources = _sources(source, extension='csv')
            logger.info(f'Importing Scopus from {sources}')
            import_id_ = await _ensure_committed_import()
            await _ensure_project_type(db_engine=db_engine, project_id=project_id, expected_type=ItemType.academic)
            await import_scopus_csv_file(
                sources=sources,
                project_id=project_id,
                import_id=import_id_,
                db_config=config_file,
                logger=logger,
            )

        elif kind == ImportTypeEnum.SOLR:
            logger.info('Importing OpenAlex directly from solr')
            import_id_ = await _ensure_committed_import()
            await _ensure_project_type(db_engine=db_engine, project_id=project_id, expected_type=ItemType.academic)
            with open(source, 'r') as f:
                query = f.read()
            params_: dict[str, Any] | None = json.loads(params) if params else None
            await import_openalex(
                query=query,
                nacsos_config=config_file,
                params=params_,
                project_id=project_id,
                import_id=import_id_,
                logger=logger,
            )

        elif kind == ImportTypeEnum.OA:
            logger.info('Proceeding with OpenAlex file import...')
            sources = _sources(source,extension='jsonl')
            import_id_ = await _ensure_committed_import()
            await _ensure_project_type(db_engine=db_engine, project_id=project_id, expected_type=ItemType.academic)
            await import_openalex_files(
                sources=sources,
                project_id=project_id,
                import_id=import_id_,
                db_config=config_file,
                logger=logger,
            )

        elif kind == ImportTypeEnum.ACADEMIC:
            logger.info('Proceeding with AcademicItem file import...')
            sources = _sources(source, extension='jsonl')
            import_id_ = await _ensure_committed_import()
            await _ensure_project_type(db_engine=db_engine, project_id=project_id, expected_type=ItemType.academic)
            await import_academic_db(
                sources=sources,
                project_id=project_id,
                import_id=import_id_,
                db_config=config_file,
                logger=logger,
            )

        elif kind == ImportTypeEnum.GENERIC:
            logger.info('Proceeding for Generic project import...')
            sources = _sources(source, extension='jsonl')
            await _ensure_project_type(db_engine=db_engine, project_id=project_id, expected_type=ItemType.generic)
            await import_generic(
                sources=sources,
                import_id=import_id,
                project_id=project_id,
                user_id=user_id,
                name=name,
                description=description,
                db_engine=db_engine,
                logger=logger,
            )

        elif kind == ImportTypeEnum.LEXIS:
            await _ensure_project_type(db_engine=db_engine, project_id=project_id, expected_type=ItemType.lexis)
            raise NotImplementedError

        else:
            raise AssertionError(f'Unknown kind: {kind}')

        logger.info('Done importing!')

    asyncio.run(_run())
