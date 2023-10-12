import random
from uuid import UUID, uuid4
from typing import TYPE_CHECKING

from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import ARRAY, UUID as pguuid

from nacsos_data.db import DatabaseEngineAsync
from nacsos_data.models.annotations import \
    AssignmentModel, \
    AssignmentStatus, \
    AssignmentScopeRandomWithExclusionConfig

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession  # noqa: F401


async def read_random_items(project_id: str | UUID,
                            num_items: int,
                            scopes: list[str] | list[UUID],
                            engine: DatabaseEngineAsync) -> list[str]:
    session: AsyncSession
    async with engine.session() as session:
        stmt = text("""
            WITH project_items AS (SELECT item.item_id
                                   FROM item
                                   WHERE item.project_id = :project_id),
                excluded_items AS (SELECT assignment.item_id
                                   FROM assignment
                                   WHERE assignment.assignment_scope_id = ANY(:scope_ids))
            SELECT pi.item_id
            FROM project_items pi
            LEFT OUTER JOIN excluded_items ei ON ei.item_id = pi.item_id
            WHERE ei.item_id is NULL
            ORDER BY random()
            LIMIT :num_items;
            """)
        stmt = stmt.bindparams(
            bindparam('scope_ids', type_=ARRAY(pguuid), value=scopes),
        )
        result = (await session.execute(stmt, {
            'project_id': project_id,
            'num_items': num_items,
            'scope_ids': scopes
        })).mappings().all()
        return [str(res['item_id']) for res in result]


async def random_assignments_with_exclusion(assignment_scope_id: str | UUID,
                                            annotation_scheme_id: str | UUID,
                                            project_id: str | UUID,
                                            config: AssignmentScopeRandomWithExclusionConfig,
                                            engine: DatabaseEngineAsync) -> list[AssignmentModel]:
    """
    :return: * None if config is invalid or cannot be satisfied,
             * Empty list if it led to no result,
             * List of AnnotationModel with "suggestions" how to make assignments
    """
    if config is None or config.config_type != 'random_exclusion':
        raise ValueError('Empty or mismatching config.')

    user_ids = config.users
    if user_ids is None or len(user_ids) < 1:
        raise ValueError('User pool empty')

    if len(user_ids) < config.min_assignments_per_item:
        raise ValueError(f'Invalid configuration: {len(user_ids)} users in pool '
                         f'but requested min {config.min_assignments_per_item} assignments per item.')

    random.seed(config.random_seed)

    # select random sample to receive annotations
    item_ids = await read_random_items(project_id=project_id,
                                       num_items=config.num_items,
                                       scopes=config.excluded_scopes,
                                       engine=engine)

    if len(item_ids) < config.num_items:
        raise ValueError(f'Not enough items found ({len(item_ids)} instead of {config.num_items})')

    assignments = []

    # select the items to be multi-coded (might not be the entire selection)
    multi_code_items = random.sample(item_ids, k=config.num_multi_coded_items)

    for item_id in multi_code_items:
        num_annotations = random.randint(config.min_assignments_per_item, config.max_assignments_per_item)
        random_users: list[str | UUID] = random.sample(user_ids, k=num_annotations)
        user_id: str | UUID
        for user_id in random_users:
            assignments.append(AssignmentModel(assignment_id=uuid4(),
                                               assignment_scope_id=assignment_scope_id,
                                               user_id=user_id,
                                               item_id=item_id,
                                               annotation_scheme_id=annotation_scheme_id,
                                               status=AssignmentStatus.OPEN))

    for it, item_id in enumerate(set(item_ids) - set(multi_code_items)):
        user_id = user_ids[it % len(user_ids)]
        assignments.append(AssignmentModel(assignment_id=uuid4(),
                                           assignment_scope_id=assignment_scope_id,
                                           user_id=user_id,
                                           item_id=item_id,
                                           annotation_scheme_id=annotation_scheme_id,
                                           status=AssignmentStatus.OPEN))
    return assignments
