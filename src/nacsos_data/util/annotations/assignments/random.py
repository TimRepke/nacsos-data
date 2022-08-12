import random
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from nacsos_data.db import DatabaseEngineAsync
from nacsos_data.models.annotations import \
    AssignmentModel, \
    AssignmentStatus, \
    AssignmentScopeRandomConfig


async def read_random_items(project_id: str | UUID, num_items: int, engine: DatabaseEngineAsync) -> list[str]:
    async with engine.session() as session:  # type: AsyncSession
        stmt = text("""
            SELECT m2m_project_item.item_id
            FROM m2m_project_item
            WHERE m2m_project_item.project_id = :project_id
            ORDER BY random()
            LIMIT :num_items;
            """)
        result = (await session.execute(stmt, {'project_id': project_id, 'num_items': num_items})).mappings().all()
        return [str(res['item_id']) for res in result]


async def random_assignments(assignment_scope_id: str | UUID,
                             annotation_scheme_id: str | UUID,
                             project_id: str | UUID,
                             config: AssignmentScopeRandomConfig,
                             engine: DatabaseEngineAsync) -> list[AssignmentModel]:
    """
    :return: * None if config is invalid or cannot be satisfied,
             * Empty list if it led to no result,
             * List of AnnotationModel with "suggestions" how to make assignments
    """
    if config is None or config.config_type != 'random':
        raise ValueError('Empty or mismatching config.')

    user_ids = config.users
    if user_ids is None or len(user_ids) < 1:
        raise ValueError('User pool empty')

    if len(user_ids) < config.min_assignments_per_item:
        raise ValueError(f'Invalid configuration: {len(user_ids)} users in pool '
                         f'but requested min {config.min_assignments_per_item} assignments per item.')

    random.seed(config.random_seed)

    # select random sample to receive annotations
    item_ids = await read_random_items(project_id=project_id, num_items=config.num_items, engine=engine)

    if len(item_ids) < config.num_items:
        raise ValueError(f'Not enough items found ({len(item_ids)} instead of {config.num_items})')

    assignments = []

    # select the items to be multi-coded (might not be the entire selection)
    multi_code_items = random.sample(item_ids, k=config.num_multi_coded_items)

    for item_id in multi_code_items:
        num_annotations = random.randint(config.min_assignments_per_item, config.max_assignments_per_item)
        random_users = random.sample(user_ids, k=num_annotations)  # type: list[str]
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
