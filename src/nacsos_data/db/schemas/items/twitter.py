from sqlalchemy import String, Column, Integer, DateTime, Float, ForeignKey, BigInteger
from sqlalchemy.dialects.postgresql import UUID, JSONB
import uuid

from ..items import Item
from ...base_class import Base


class TwitterItem(Base):
    """
    TODO: description

    For more in-depth documentation, please refer to:
    https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet
    """
    __tablename__ = 'twitter_item'

    # Unique identifier for this TwitterItem, corresponds to Item
    item_id = Column(UUID(as_uuid=True), ForeignKey(Item.item_id), default=uuid.uuid4,
                     nullable=False, index=True, primary_key=True, unique=True)  # type: Column[uuid.UUID | str]
    # Unique identifier on Twitter
    twitter_id = Column(BigInteger, nullable=False, unique=True, index=True)
    # Unique user identifier on Twitter
    twitter_author_id = Column(BigInteger, nullable=True, index=True)

    # text of the tweet (in Twitter lingo, it's the "status")
    status = Column(String, nullable=False)

    # date and time this tweet was posted Format: ISO 8601 (e.g. "2019-06-04T23:12:08.000Z")
    created_at = Column(DateTime, nullable=False)

    # language of this tweet (as provided by Twitter)
    language = Column(String, nullable=True)

    # The Tweet ID of the original Tweet of the conversation (which includes direct replies, replies of replies).
    conversation_id = Column(BigInteger, nullable=True, index=True)
    # A list of Tweets this Tweet refers to. For example, if the parent Tweet is a Retweet, a Retweet with comment
    # (also known as Quoted Tweet) or a Reply, it will include the related Tweet referenced to by its parent.
    referenced_tweets = Column(JSONB, nullable=True)

    # Specifies the type of attachments (if any) present in this Tweet.
    # attachments: Optional[any] # TODO should we store that?

    # from geo.coordinates.coordinates:
    # A pair of decimal values representing the precise location of the user (latitude, longitude).
    # This value be null unless the user explicitly shared their precise location.
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)

    # from entities.hashtags (Contains details about text recognized as a Hashtag.)
    hashtags = Column(JSONB, nullable=True)
    # from entities.mentions (Contains details about text recognized as a user mention.)
    mentions = Column(JSONB, nullable=True)
    # from entities.urls (Contains details about text recognized as a URL.)
    urls = Column(JSONB, nullable=True)
    # from entities.cashtag (Contains details about text recognized as a Cashtag.)
    # [Cashtags are stock price symbols]
    cashtags = Column(JSONB, nullable=True)
    # from context_annotations (Contains context annotations for the Tweet.)
    # Entity recognition/extraction, topical analysis
    annotations = Column(JSONB, nullable=True)

    # Public engagement metrics for the Tweet at the time of the request.
    # taken from public_metrics.???
    retweet_count = Column(Integer, nullable=False)
    reply_count = Column(Integer, nullable=False)
    like_count = Column(Integer, nullable=False)
    quote_count = Column(Integer, nullable=False)

    # Additional information about the user (retrieved via expansions=author_id)
    user = Column(JSONB, nullable=True)
