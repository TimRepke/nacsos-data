import uuid
from typing import Optional
from sqlalchemy import select, delete, insert
from uuid import UUID

from nacsos_data.db import DatabaseEngineAsync
from nacsos_data.db.crud.items import create_item
from nacsos_data.db.schemas.items import Item
from nacsos_data.db.schemas import TwitterItem, M2MProjectItem
from nacsos_data.models.items import ItemModel
from nacsos_data.models.items.twitter import TwitterItemModel


# TODO paged output with cursor of some sort
#      ideally make it a decorator so it's reusable everywhere
#      https://www.postgresql.org/docs/current/sql-declare.html
async def read_all_tweets_for_project(project_id: str | UUID, engine: DatabaseEngineAsync) -> list[TwitterItemModel]:
    async with engine.session() as session:
        stmt = select(TwitterItem) \
            .join(M2MProjectItem, M2MProjectItem.item_id == TwitterItem.item_id) \
            .where(M2MProjectItem.project_id == project_id)
        result = await session.execute(stmt)
        result_list = result.scalars().all()
        return [TwitterItemModel(**res.__dict__) for res in result_list]


async def read_tweet_by_item_id(item_id: str | UUID, engine: DatabaseEngineAsync) -> TwitterItemModel:
    stmt = select(TwitterItem).filter_by(item_id=item_id)
    async with engine.session() as session:
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return TwitterItemModel(**result.__dict__)


async def read_tweet_by_twitter_id(twitter_id: str | UUID, engine: DatabaseEngineAsync) -> TwitterItemModel:
    stmt = select(TwitterItem).filter_by(twitter_id=twitter_id)
    async with engine.session() as session:
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return TwitterItemModel(**result.__dict__)


async def read_tweets_by_author_id(twitter_author_id: str | UUID, engine: DatabaseEngineAsync) \
        -> list[TwitterItemModel]:
    stmt = select(TwitterItem).filter_by(twitter_author_id=twitter_author_id)
    async with engine.session() as session:
        result = await session.execute(stmt)
        result_list = result.scalars().all()
        return [TwitterItemModel(**res.__dict__) for res in result_list]


async def create_tweet(tweet: TwitterItemModel, project_id: str | UUID | None, engine: DatabaseEngineAsync) \
        -> str | UUID:
    # TODO return item_id
    try:
        async with engine.session() as session:
            # TODO check, that this will break if an already existing tweets is being inserted
            #      and fail gracefully
            orm_tweet = TwitterItem(**tweet.dict())
            orm_tweet.item_id = uuid.uuid4()
            orm_item = Item(item_id=orm_tweet.item_id, text=tweet.status)

            session.add(orm_tweet)
            session.add(orm_item)

            if project_id is not None:
                orm_m2m = M2MProjectItem(item_id=orm_item.item_id, project_id=project_id)
                session.add(orm_m2m)

            await session.commit()

            return orm_tweet.item_id
    except Exception as e:
        # TODO clean exception handling
        print(e)


async def create_tweets(tweets: list[TwitterItemModel], project_id: str | UUID | None, engine: DatabaseEngineAsync) \
        -> list[str | UUID]:
    # TODO return item_ids
    # TODO make this in an actual batched mode
    for tweet in tweets:
        return [await create_tweet(tweet, project_id=project_id, engine=engine)]
