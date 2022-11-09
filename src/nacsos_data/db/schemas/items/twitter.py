from sqlalchemy import String, Integer, DateTime, Float, ForeignKey, UniqueConstraint, Column
from sqlalchemy.dialects.postgresql import UUID, JSONB
import uuid

from sqlalchemy.orm import mapped_column, column_property

from ..projects import Project
from .base import Item
from . import ItemType


class TwitterItem(Item):
    """
    TODO: description

    For more in-depth documentation, please refer to:
    https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet
    """
    __tablename__ = 'twitter_item'
    __table_args__ = (
        UniqueConstraint('twitter_id', 'project_id'),
    )

    # Unique identifier for this TwitterItem, corresponds to Item
    item_id = mapped_column(UUID(as_uuid=True),
                            ForeignKey(Item.item_id),
                            default=uuid.uuid4, nullable=False, index=True, primary_key=True, unique=True)

    # mirror of `Item.project_id` so we can introduce the UniqueConstraint
    # https://docs.sqlalchemy.org/en/20/faq/ormconfiguration.html#i-m-getting-a-warning-or-error-about-implicitly-combining-column-x-under-attribute-y
    project_id = column_property(Column(UUID(as_uuid=True),
                                        ForeignKey(Project.project_id, ondelete='cascade'),
                                        index=True, nullable=False), Item.project_id)

    # Unique identifier on Twitter
    twitter_id = mapped_column(String, nullable=False, unique=False, index=True)
    # Unique user identifier on Twitter
    twitter_author_id = mapped_column(String, nullable=True, index=True)

    # date and time this tweet was posted Format: ISO 8601 (e.g. "2019-06-04T23:12:08.000Z")
    created_at = mapped_column(DateTime, nullable=False)

    # language of this tweet (as provided by Twitter)
    language = mapped_column(String, nullable=True)

    # The Tweet ID of the original Tweet of the conversation (which includes direct replies, replies of replies).
    conversation_id = mapped_column(String, nullable=True, index=True)
    # A list of Tweets this Tweet refers to. For example, if the parent Tweet is a Retweet, a Retweet with comment
    # (also known as Quoted Tweet) or a Reply, it will include the related Tweet referenced to by its parent.
    referenced_tweets = mapped_column(JSONB, nullable=True)

    # Specifies the type of attachments (if any) present in this Tweet.
    # attachments: Optional[any] # TODO should we store that?

    # from geo.coordinates.coordinates:
    # A pair of decimal values representing the precise location of the user (latitude, longitude).
    # This value be null unless the user explicitly shared their precise location.
    latitude = mapped_column(Float, nullable=True)
    longitude = mapped_column(Float, nullable=True)

    # from entities.hashtags (Contains details about text recognized as a Hashtag.)
    hashtags = mapped_column(JSONB, nullable=True)
    # from entities.mentions (Contains details about text recognized as a user mention.)
    mentions = mapped_column(JSONB, nullable=True)
    # from entities.urls (Contains details about text recognized as a URL.)
    urls = mapped_column(JSONB, nullable=True)
    # from entities.cashtag (Contains details about text recognized as a Cashtag.)
    # [Cashtags are stock price symbols]
    cashtags = mapped_column(JSONB, nullable=True)
    # from context_annotations (Contains context annotations for the Tweet.)
    # Entity recognition/extraction, topical analysis
    annotations = mapped_column(JSONB, nullable=True)

    # Public engagement metrics for the Tweet at the time of the request.
    # taken from public_metrics.???
    retweet_count = mapped_column(Integer, nullable=False)
    reply_count = mapped_column(Integer, nullable=False)
    like_count = mapped_column(Integer, nullable=False)
    quote_count = mapped_column(Integer, nullable=False)

    # Additional information about the user (retrieved via expansions=author_id)
    user = mapped_column(JSONB, nullable=True)

    __mapper_args__ = {
        'polymorphic_identity': ItemType.twitter,
    }
