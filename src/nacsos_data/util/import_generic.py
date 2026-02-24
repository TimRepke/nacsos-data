import json
import logging
import uuid
from pathlib import Path
from sqlalchemy.dialects.postgresql import insert as insert_pg

from nacsos_data.models.imports import M2MImportItemType
from nacsos_data.db import DatabaseEngineAsync
from nacsos_data.db.schemas.items.generic import GenericItem
from nacsos_data.db.schemas.imports import Import, ImportRevision, m2m_import_item_table
from tqdm import tqdm


async def import_generic(
    sources: list[Path],  # Path to translated import file
    import_id: str,  # Project ID
    project_id: str,  # Project ID
    name: str,  # Name of this import
    description: str,  # Description of this import
    db_engine: DatabaseEngineAsync,
    logger: logging.Logger,
    user_id: str = 'fd641232-bada-466e-9a3b-fb12038f5508',  # User ID used for import (default: Tim)
):
    num_items = 0

    for source in tqdm(sources, total=f'Counting items from {len(sources)} source files'):
        with open(source, 'r') as f_in:
            for _line in f_in:
                num_items += 1

    async with db_engine.session() as session:
        if import_id is None:
            logger.info('Creating new import')
            import_id = uuid.uuid4()

            imp = Import(
                import_id=import_id,
                project_id=project_id,
                user_id=user_id,
                type='SCRIPT',
                name=name,
                description=description,
            )
            session.add(imp)
            await session.flush()
        else:
            logger.info('Using existing import by ID provided')

        logger.info('Creating new import revision')
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

        for source in sources:
            with open(source, 'r') as f_in:
                for line in tqdm(f_in):
                    obj = json.loads(line)
                    item = GenericItem(
                        **obj
                        | {
                            'item_id': uuid.uuid4(),
                            'project_id': project_id,
                        },
                    )
                    logger.debug(f'Importing item {item.item_id}')
                    session.add(item)
                    await session.flush()

                    stmt_m2m = insert_pg(m2m_import_item_table).values(
                        item_id=item.item_id,
                        import_id=import_id,
                        type=M2MImportItemType.explicit,
                        first_revision=1,
                        latest_revision=1,
                    )
                    await session.execute(stmt_m2m)
                    await session.flush()

        await session.commit()
