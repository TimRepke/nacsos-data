import logging
import uuid
import random
from dataclasses import dataclass

from sqlalchemy import select, text

from nacsos_data.db.schemas import Priority, AssignmentScope, Assignment
from nacsos_data.util.errors import NotFoundError
from nacsos_data.db.engine import DBSession, ensure_session_async
from nacsos_data.models.annotations import AssignmentModel, AssignmentScopeModel, AssignmentStatus
from nacsos_data.models.nql import NQLFilter
from nacsos_data.util.nql import NQLQuery

logger = logging.getLogger('nacsos_data.util.assignments')


@ensure_session_async
async def get_db_sample(session: DBSession, project_id: str, num_items: int, nql: NQLFilter | None = None) -> list[str]:
    stmt_query = (await NQLQuery.get_query(session=session, project_id=project_id, query=nql)).stmt.cte('nql')
    stmt = (select(stmt_query.c.item_id)
            .order_by(text('random()')).limit(num_items))
    rslt = (await session.execute(stmt)).mappings().all()
    return [str(res['item_id']) for res in rslt]


@ensure_session_async
async def get_priority_sample(session: DBSession, priority_id: str, num_items: int, offset: int = 0) -> list[str]:
    rslt = (await session.execute(select(Priority.prioritised_ids)
                                  .where(Priority.priority_id == priority_id))).scalar()

    if rslt is None or len(rslt) < offset + num_items:
        raise NotFoundError('No or not enough prioritised items found!')

    return [str(r) for r in rslt[offset:offset + num_items]]


@ensure_session_async
async def get_sample(session: DBSession, project_id: str, assignment_scope: AssignmentScopeModel) -> list[str]:
    config = assignment_scope.config
    if not config:
        raise ValueError('Missing config!')

    if config.config_type == 'RANDOM':
        return await get_db_sample(session=session,
                                   project_id=project_id,
                                   num_items=config.num_assigned_items,
                                   nql=config.nql_parsed)

    if config.config_type == 'PRIORITY':
        return await get_priority_sample(session=session,
                                         priority_id=config.priority_id,
                                         num_items=config.num_assigned_items,
                                         offset=config.prio_offset)

    raise NotImplementedError(f'Unknown config type {config.config_type}!')


@dataclass
class PoolUser:
    user_id: str
    budget: int


@dataclass
class PoolItem:
    item_id: str
    order: int


def distribute_assignments(users: dict[str, int],
                           overlaps: dict[int, int],
                           item_ids: list[str],
                           assignment_scope_id: str,
                           annotation_scheme_id: str,
                           random_seed: int = 1337) -> list[AssignmentModel]:
    """
    Given the user pool, overlap configuration, and a set of items to assign, create a plausible set of assignments.

    :param users: user pool (user_id -> number of assignments)
    :param overlaps: assignment overlap (users per item -> number of items with that many users per item)
    :param item_ids: pool of items to assign
    :param random_seed: seed for the random number generator
    :param assignment_scope_id:
    :param annotation_scheme_id:
    :return:
    """
    random.seed(random_seed)
    user_pool: list[PoolUser] = [PoolUser(user_id=user_id, budget=budget)
                                 for user_id, budget in users.items()]
    item_pool: list[PoolItem] = [PoolItem(item_id=item_id, order=i)
                                 for i, item_id in enumerate(item_ids)]

    logger.info(f'user_pool: {len(user_pool)} '
                f'/ item_pool: {len(item_pool)} '
                f'/ item_ids: {len(item_ids)} '
                f'/ overlaps: {len(overlaps)}')

    assignments = []

    for overlap, item_count in overlaps.items():
        logger.info(f'> overlap: {overlap} / item_count: {item_count} '
                    f'| user_pool: {len(user_pool)} / item_pool: {len(item_pool)}')
        logger.debug(f'{user_pool}')

        if len(item_pool) < item_count:
            raise AssertionError('Configuration impossible, item pool ran out early!')

        random.shuffle(item_pool)

        for i, item in enumerate(item_pool[:item_count]):
            logger.debug(f'{user_pool}')
            if len(user_pool) < overlap:
                raise AssertionError('Configuration impossible, user pool ran out early!')

            # random.shuffle(user_pool)
            for j in range(overlap):
                user_idx = (i + j) % len(user_pool)
                user_pool[user_idx].budget -= 1
                assignments.append(
                    AssignmentModel(assignment_id=uuid.uuid4(),
                                    assignment_scope_id=assignment_scope_id,
                                    annotation_scheme_id=annotation_scheme_id,
                                    order=item.order,
                                    item_id=item.item_id,
                                    user_id=user_pool[user_idx].user_id,
                                    status=AssignmentStatus.OPEN))

            # Clear users from pool where budget ran out
            user_pool = [user for user in user_pool if user.budget > 0]
            user_pool.sort(key=lambda x: x.budget, reverse=True)

        item_pool = item_pool[item_count:]
    return assignments


@ensure_session_async
async def create_assignments(session: DBSession, assignment_scope_id: str, project_id: str) -> list[AssignmentModel]:
    scope_ = await session.scalar(select(AssignmentScope)
                                  .where(AssignmentScope.assignment_scope_id == assignment_scope_id))
    if not scope_:
        raise NotFoundError(f'No assignment scope with ID={assignment_scope_id}')

    scope = AssignmentScopeModel(**scope_.__dict__)
    config = scope.config
    if not config or config.config_type == 'LEGACY':
        raise ValueError('No valid config!')

    item_ids = await get_sample(session=session,
                                project_id=project_id,
                                assignment_scope=scope)
    assignments = distribute_assignments(users=config.users,
                                         overlaps=config.overlaps,
                                         item_ids=item_ids,
                                         assignment_scope_id=assignment_scope_id,
                                         annotation_scheme_id=str(scope.annotation_scheme_id),
                                         random_seed=config.random_seed)

    session.add_all([Assignment(**assignment.model_dump()) for assignment in assignments])
    await session.commit()

    return assignments
