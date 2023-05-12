import uuid
import logging
from typing import Any, TYPE_CHECKING

from sqlalchemy import update, delete, select

from ..db import DatabaseEngineAsync
from ..db.schemas import \
    Assignment, \
    Annotation, \
    BotAnnotationMetaData, \
    BotAnnotation, \
    m2m_import_item_table
from ..models.bot_annotations import BotKind

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession  # noqa: F401

logger = logging.getLogger('nacsos_data.util.duplicate')


async def update_references(old_item_id: str | uuid.UUID,
                            new_item_id: str | uuid.UUID,
                            db_engine: DatabaseEngineAsync) -> None:
    """
    This function can be used in case where an item_id changes and
    all references in the database need to be updated accordingly.
    The most common scenario for this is for deduplication after detecting a pair of
    duplicates, merging it into one item and having to update references for
    assignments, annotations, imports, etc.

    Note: The AcademicItem (or TwitterItem,...) with `new_item_id` has to exist in the database.
    Note: This function does not delete the item for `old_item_id`

    :param old_item_id:
    :param new_item_id:
    :param db_engine:
    :return:
    """

    # No updates needed in
    #  - annotation_scheme
    #  - assignment_scope
    #  - auth_tokens
    #  - highlighters
    #  - import
    #  - project
    #  - project_permissions
    #  - user
    #
    # Not performing updates in
    #  - academic_item
    #  - twitter_item
    #  - generic_item
    #  - item
    #
    # Might need updates in `tasks`, but it's too painful for little gain.
    # May need to be done in the future, but not important for now.

    async with db_engine.session() as session:  # type: AsyncSession

        # Point Annotations to new Item
        n_annotations = await session.execute(
            update(Annotation)
            .where(Annotation.item_id == old_item_id)
            .values(item_id=new_item_id)
            .returning(Annotation.annotation_id)
        )

        # Point Assignments to new Item
        n_assignments = await session.execute(
            update(Assignment)
            .where(Assignment.item_id == old_item_id)
            .values(item_id=new_item_id)
        )

        # Point BotAnnotations to new Item
        n_bot_annotations = await session.execute(
            update(BotAnnotation)
            .where(BotAnnotation.item_id == old_item_id)
            .values(item_id=new_item_id)
        )

        # Rewire Import many-to-many relation
        # first, drop all references that we will create in a second anyway
        n_m2m_del = await session.execute(
            delete(m2m_import_item_table)
            .where(m2m_import_item_table.c.item_id == new_item_id)
        )
        # now, update all m2m relations for the old item
        n_m2m2_upd = await session.execute(
            update(m2m_import_item_table)
            .where(m2m_import_item_table.c.item_id == old_item_id)
            .values(item_id=new_item_id)
        )

        # We do store some background information in the meta-data for label resolutions, incl item_ids
        bot_annotation_scopes = (
            await session.execute(select(BotAnnotationMetaData)
                                  .where(BotAnnotationMetaData.kind == BotKind.RESOLVE))
        ).scalars().all()
        n_ba_scopes = 0
        for bot_anno_scope in bot_annotation_scopes:
            meta: dict[str, Any] = bot_anno_scope.meta  # type: ignore[assignment] # dict of type BotMetaResolve

            if meta is not None and str(old_item_id) in meta['collection']['annotations']:
                # update `item_ids` in `AnnotationCollection` entries if necessary
                for aci, collections in enumerate(meta['collection']['annotations'][str(old_item_id)]):
                    for ci, collection in enumerate(collections):
                        # collection[0] is always the path (e.g. recursive keys based on parent structure)
                        for ai, anno in enumerate(collection[1]):
                            if str(anno['item_id']) == str(old_item_id):
                                meta['collection']['annotations'][str(old_item_id)][aci][ci][1][ai]['item_id'] = str(new_item_id)

                # We already have something for the new `item_id`, so merge collections
                # Even though there might be "duplicate" labels now (same user,item pairs), we keep them all!
                if str(new_item_id) in meta['collection']['annotations']:
                    meta['collection']['annotations'][str(new_item_id)] += meta['collection']['annotations'][
                        str(old_item_id)]
                    del meta['collection']['annotations'][str(old_item_id)]

                # otherwise, just move and delete the old one
                else:
                    meta['collection']['annotations'][str(new_item_id)] = meta['collection']['annotations'][
                        str(old_item_id)]
                    del meta['collection']['annotations'][str(old_item_id)]

                n_ba_scopes += 1
                bot_anno_scope.meta = meta  # type: ignore[assignment]
                await session.commit()

        logger.debug(f'Updated references "{old_item_id}"->"{new_item_id}": '
                     f'{n_annotations.rowcount} annotations affected, '  # type: ignore[attr-defined]
                     f'{n_assignments.rowcount} assignments affected, '  # type: ignore[attr-defined]
                     f'{n_bot_annotations.rowcount} bot_annotations affected, '  # type: ignore[attr-defined]
                     f'{n_m2m_del.rowcount} import_m2m entries deleted, '  # type: ignore[attr-defined]
                     f'{n_m2m2_upd.rowcount} import_m2m entries updated, '  # type: ignore[attr-defined]
                     f'{n_ba_scopes} bot_annotation_metadata for RESOLVE updated')
