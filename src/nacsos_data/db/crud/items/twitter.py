import uuid
from sqlalchemy import select
from uuid import UUID

from nacsos_data.db import DatabaseEngineAsync
from nacsos_data.db.schemas.items import Item
from nacsos_data.db.schemas import TwitterItem, M2MProjectItem
from nacsos_data.models.items.twitter import TwitterItemModel

from . import _read_all_for_project, _read_paged_for_project


async def read_all_twitter_items_for_project(project_id: str | UUID, engine: DatabaseEngineAsync) \
        -> list[TwitterItemModel]:
    return await _read_all_for_project(project_id=project_id, Schema=TwitterItem, Model=TwitterItemModel, engine=engine)


async def read_all_twitter_items_for_project_paged(project_id: str | UUID, page: int, page_size: int,
                                                   engine: DatabaseEngineAsync) -> list[TwitterItemModel]:
    return await _read_paged_for_project(project_id=project_id, page=page, page_size=page_size,
                                         Schema=TwitterItem, Model=TwitterItemModel, engine=engine)


async def read_twitter_item_by_item_id(item_id: str | UUID, engine: DatabaseEngineAsync) -> TwitterItemModel:
    stmt = select(TwitterItem).filter_by(item_id=item_id)
    async with engine.session() as session:
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return TwitterItemModel(**result.__dict__)


async def read_twitter_item_by_twitter_id(twitter_id: str | UUID, engine: DatabaseEngineAsync) -> TwitterItemModel:
    stmt = select(TwitterItem).filter_by(twitter_id=twitter_id)
    async with engine.session() as session:
        result = (await session.execute(stmt)).scalars().one_or_none()
        if result is not None:
            return TwitterItemModel(**result.__dict__)


async def read_twitter_items_by_author_id(twitter_author_id: str | UUID, engine: DatabaseEngineAsync) \
        -> list[TwitterItemModel]:
    stmt = select(TwitterItem).filter_by(twitter_author_id=twitter_author_id)
    async with engine.session() as session:
        result = await session.execute(stmt)
        result_list = result.scalars().all()
        return [TwitterItemModel(**res.__dict__) for res in result_list]


async def create_twitter_item(tweet: TwitterItemModel, project_id: str | UUID | None, engine: DatabaseEngineAsync) \
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


async def create_twitter_items(tweets: list[TwitterItemModel], project_id: str | UUID | None,
                               engine: DatabaseEngineAsync) -> list[str | UUID]:
    # TODO return item_ids
    # TODO make this in an actual batched mode
    for tweet in tweets:
        return [await create_twitter_item(tweet, project_id=project_id, engine=engine)]
