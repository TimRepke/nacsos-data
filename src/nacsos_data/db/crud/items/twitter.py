import logging
from uuid import UUID
from typing import TYPE_CHECKING

from sqlalchemy import select, insert
from sqlalchemy.exc import IntegrityError

from nacsos_data.db import DatabaseEngineAsync
from nacsos_data.db.schemas import TwitterItem
from nacsos_data.db.schemas.imports import m2m_import_item_table
from nacsos_data.db.crud.items import read_all_for_project, read_paged_for_project
from nacsos_data.models.imports import M2MImportItemType
from nacsos_data.models.items.twitter import TwitterItemModel

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession  # noqa: F401

logger = logging.getLogger('nacsos-data.crud.twitter')


async def import_tweet(tweet: TwitterItemModel,
                       engine: DatabaseEngineAsync,
                       project_id: str | UUID | None = None,
                       import_id: UUID | str | None = None,
                       import_type: M2MImportItemType | None = None) \
        -> TwitterItemModel:
    """
    Get or create Tweet (and optionally keep track of how it ended up in the database)
    1) Will try to find a TwitterItem in the database with tweet.twitter_id and
       project_id (or tweet.project_id if project_id is None).
       If not found, create!

    2) Add M2M import for this item (if import_id is not None and item_id<->import_id pair does not exist yet).

    :param tweet: The Tweet (Should contain all fields for which Twitter API gave us data)
    :param project_id: Project this Tweet is inserted to (or None if no project is linked -> AVOID this!)
    :param import_id: Import context (or None if not linked to specific import job -> AVOID this!)
    :param import_type: Type of relation to import (explicit or implicit, see schema for more information)
    :param engine:
    :return: TwitterItem(Model) that was affected by this operation
    """
    session: AsyncSession
    async with engine.session() as session:
        orm_tweet = TwitterItem(**tweet.model_dump())

        if project_id is not None:
            # FIXME: mypy  Union[str, UUID] vs Union[SQLCoreOperations[UUID], UUID]"
            orm_tweet.project_id = project_id  # type: ignore[assignment]
        try:
            session.add(orm_tweet)
            await session.flush()
        except IntegrityError:
            logger.debug(f'Tweet with twitter_id="{orm_tweet.twitter_id}" '
                         f'already exists in project "{orm_tweet.project_id}".')
            # First, rollback all previously attempted actions (bring transaction to initial state)
            await session.rollback()

            stmt = select(TwitterItem).where(TwitterItem.twitter_id == orm_tweet.twitter_id,
                                             TwitterItem.project_id == orm_tweet.project_id)
            # FIXME: mypy "Optional[TwitterItem]", variable has type "TwitterItem"
            orm_tweet = (await session.execute(stmt)).scalars().one_or_none()  # type: ignore[assignment]

        if orm_tweet is None:
            raise RuntimeError('Failed in unclear state, undetermined tweet!')

        if import_id is not None:
            stmt_m2m = insert(m2m_import_item_table) \
                .values(item_id=orm_tweet.item_id, import_id=import_id, type=import_type)
            try:
                await session.execute(stmt_m2m)
            except IntegrityError:
                logger.debug(f'M2M_i2i already exists, ignoring {import_id} <-> {orm_tweet.item_id}')
                await session.rollback()

        await session.commit()

        return TwitterItemModel.model_validate(orm_tweet.__dict__)


async def import_tweets(tweets: list[TwitterItemModel], engine: DatabaseEngineAsync,
                        project_id: str | UUID | None = None, import_id: str | UUID | None = None) \
        -> list[str | UUID]:
    # FIXME: mypy List comprehension has incompatible type List[Union[str, UUID, None]]; expected List[Union[str, UUID]]
    return [(await import_tweet(tweet, engine=engine,  # type: ignore[misc]
                                project_id=project_id, import_id=import_id)).item_id
            for tweet in tweets]


async def read_all_twitter_items_for_project(project_id: str | UUID, engine: DatabaseEngineAsync) \
        -> list[TwitterItemModel]:
    tweets: list[TwitterItemModel] = await read_all_for_project(project_id=project_id,
                                                                Schema=TwitterItem, Model=TwitterItemModel,
                                                                engine=engine)
    return tweets


async def read_all_twitter_items_for_project_paged(project_id: str | UUID, page: int, page_size: int,
                                                   engine: DatabaseEngineAsync) -> list[TwitterItemModel]:
    tweets: list[TwitterItemModel] = await read_paged_for_project(project_id=project_id,
                                                                  page=page, page_size=page_size,
                                                                  Schema=TwitterItem, Model=TwitterItemModel,
                                                                  engine=engine)
    return tweets


async def read_twitter_item_by_item_id(item_id: str | UUID, engine: DatabaseEngineAsync) -> TwitterItemModel | None:
    session: AsyncSession
    async with engine.session() as session:
        result = await session.get(TwitterItem, item_id)
        if result is not None:
            return TwitterItemModel.model_validate(result.__dict__)
    return None


async def read_twitter_item_by_twitter_id(twitter_id: str, project_id: str,
                                          engine: DatabaseEngineAsync) -> TwitterItemModel | None:
    session: AsyncSession
    async with engine.session() as session:
        stmt = select(TwitterItem).where(TwitterItem.twitter_id == twitter_id,
                                         TwitterItem.project_id == project_id)
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return TwitterItemModel.model_validate(result.__dict__)
    return None


async def read_twitter_items_by_author_id(twitter_author_id: str, project_id: str,
                                          engine: DatabaseEngineAsync) -> list[TwitterItemModel]:
    session: AsyncSession
    async with engine.session() as session:
        stmt = select(TwitterItem).where(TwitterItem.twitter_author_id == twitter_author_id,
                                         TwitterItem.project_id == project_id)
        result = (await session.execute(stmt)).scalars().all()
        return [TwitterItemModel(**res.__dict__) for res in result]
