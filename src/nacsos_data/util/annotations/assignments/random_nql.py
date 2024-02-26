import random
from uuid import UUID, uuid4
from typing import TYPE_CHECKING

from sqlalchemy import text, select

from nacsos_data.db import DatabaseEngineAsync
from nacsos_data.models.annotations import (
    AssignmentModel,
    AssignmentStatus,
    AssignmentScopeRandomWithNQLConfig
)
from nacsos_data.db.schemas.projects import Project
from nacsos_data.util.nql import query_to_sql

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession  # noqa: F401


async def random_assignments_with_nql(assignment_scope_id: str | UUID,
                                      annotation_scheme_id: str | UUID,
                                      project_id: str | UUID,
                                      config: AssignmentScopeRandomWithNQLConfig,
                                      engine: DatabaseEngineAsync) -> list[AssignmentModel]:
    """
    :return: * None if config is invalid or cannot be satisfied,
             * Empty list if it led to no result,
             * List of AnnotationModel with "suggestions" how to make assignments
    """
    if config is None or config.config_type != 'random_nql':
        raise ValueError('Empty or mismatching config.')

    user_ids = config.users
    if user_ids is None or len(user_ids) < 1:
        raise ValueError('User pool empty')

    if len(user_ids) < config.min_assignments_per_item:
        raise ValueError(f'Invalid configuration: {len(user_ids)} users in pool '
                         f'but requested min {config.min_assignments_per_item} assignments per item.')

    random.seed(config.random_seed)

    # select random sample to receive annotations
    session: AsyncSession
    async with engine.session() as session:
        project_type = await session.scalar(select(Project.type).where(Project.project_id == project_id))
        stmt_query = query_to_sql(config.query_parsed, project_id=str(project_id), project_type=project_type).cte('nql')
        stmt = select(stmt_query.c.item_id).order_by(text('random()')).limit(config.num_items)
        rslt = (await session.execute(stmt)).mappings().all()
        item_ids = [str(res['item_id']) for res in rslt]

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
