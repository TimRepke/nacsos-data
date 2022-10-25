import uuid
import logging
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID

from nacsos_data.db import DatabaseEngineAsync
from nacsos_data.db.schemas.items import Item, M2MImportItem
from nacsos_data.db.schemas import TwitterItem, M2MProjectItem
from nacsos_data.models.items.twitter import TwitterItemModel

from . import _read_all_for_project, _read_paged_for_project

logger = logging.getLogger('nacsos-data.crud.twitter')


async def read_all_twitter_items_for_project(project_id: str | UUID, engine: DatabaseEngineAsync) \
        -> list[TwitterItemModel]:
    return await _read_all_for_project(project_id=project_id, Schema=TwitterItem, Model=TwitterItemModel, engine=engine)


async def read_all_twitter_items_for_project_paged(project_id: str | UUID, page: int, page_size: int,
                                                   engine: DatabaseEngineAsync) -> list[TwitterItemModel]:
    return await _read_paged_for_project(project_id=project_id, page=page, page_size=page_size,
                                         Schema=TwitterItem, Model=TwitterItemModel, engine=engine)


async def read_twitter_item_by_item_id(item_id: str | UUID, engine: DatabaseEngineAsync) -> TwitterItemModel | None:
    stmt = select(TwitterItem).filter_by(item_id=item_id)
    async with engine.session() as session:  # type: AsyncSession
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return TwitterItemModel(**result.__dict__)
    return None


async def read_twitter_item_by_twitter_id(twitter_id: str,
                                          engine: DatabaseEngineAsync) -> TwitterItemModel | None:
    stmt = select(TwitterItem).filter_by(twitter_id=twitter_id)
    async with engine.session() as session:  # type: AsyncSession
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return TwitterItemModel(**result.__dict__)
    return None


async def read_twitter_items_by_author_id(twitter_author_id: str, engine: DatabaseEngineAsync) \
        -> list[TwitterItemModel]:
    stmt = select(TwitterItem).filter_by(twitter_author_id=twitter_author_id)
    async with engine.session() as session:  # type: AsyncSession
        result = await session.execute(stmt)
        result_list = result.scalars().all()
        return [TwitterItemModel(**res.__dict__) for res in result_list]


async def create_twitter_item(tweet: TwitterItemModel, engine: DatabaseEngineAsync,
                              project_id: str | UUID | None = None, import_id: str | UUID | None = None) \
        -> str | UUID | None:
    """
    Insert a Tweet into the database.

    :param tweet: The Tweet (Should contain all fields for which Twitter API gave us data)
    :param project_id: Project this Tweet is inserted to (or None if no project is linked -> AVOID this!)
    :param import_id: Import context (or None if not linked to specific import job -> AVOID this!)
    :param engine:
    :return:
    """

    async with engine.session() as session:
        orm_tweet = TwitterItem(**tweet.dict())
        if orm_tweet.item_id is None:
            orm_tweet.item_id = uuid.uuid4()
        item_id = str(orm_tweet.item_id)
        orm_item = Item(item_id=orm_tweet.item_id, text=tweet.status)

        try:
            session.add(orm_tweet)
            session.add(orm_item)
            await session.commit()
        except IntegrityError:
            logger.debug(f'Did not create new item tweet_id: {orm_tweet.twitter_id} -> exists in item_id: {item_id}.')
            # First, rollback all previously attempted actions (bring transaction to initial state)
            await session.rollback()
            item_id = (
                await session.execute(select(TwitterItem.item_id)
                                      .where(TwitterItem.twitter_id == orm_tweet.twitter_id))
            ).one()[0]

        if project_id is not None:
            orm_m2m_p2i = M2MProjectItem(item_id=item_id, project_id=project_id)
            try:
                session.add(orm_m2m_p2i)
                await session.commit()
            except IntegrityError:
                logger.debug(f'M2M_p2i already exists, ignoring {project_id} <-> {item_id}')
                await session.rollback()

        if import_id is not None:
            orm_m2m_i2i = M2MImportItem(item_id=item_id, import_id=import_id)
            try:
                session.add(orm_m2m_i2i)
                await session.commit()
            except IntegrityError:
                logger.debug(f'M2M_i2i already exists, ignoring {import_id} <-> {item_id}')
                await session.rollback()

        return item_id


async def create_twitter_items(tweets: list[TwitterItemModel], engine: DatabaseEngineAsync,
                               project_id: str | UUID | None = None, import_id: str | UUID | None = None) \
        -> list[str | UUID | None]:
    return [await create_twitter_item(tweet, project_id=project_id, import_id=import_id, engine=engine)
            for tweet in tweets]
