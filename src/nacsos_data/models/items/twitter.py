from typing import Literal
from datetime import datetime
from uuid import UUID

from .. import SBaseModel


class ReferencedTweet(SBaseModel):
    id: int
    type: Literal['retweeted', 'quoted', 'replied_to']


class Cashtag(SBaseModel):
    # The start position (zero-based) of the recognized Cashtag within the Tweet. All start indices are inclusive.
    start: int
    # The end position (zero-based) of the recognized Cashtag within the Tweet. This end index is exclusive.
    end: int
    # The text of the Cashtag.
    tag: str


class Hashtag(SBaseModel):
    # The start position (zero-based) of the recognized Hashtag within the Tweet. All start indices are inclusive.
    start: int
    # The end position (zero-based) of the recognized Hashtag within the Tweet. This end index is exclusive.
    end: int
    # The text of the Hashtag.
    tag: str


class Mention(SBaseModel):
    # The start position (zero-based) of the recognized user mention within the Tweet. All start indices are inclusive.
    start: int
    # The end position (zero-based) of the recognized user mention within the Tweet. This end index is exclusive.
    end: int
    # The part of text recognized as a user mention.
    username: str


class URL(SBaseModel):
    # The start position (zero-based) of the recognized URL within the Tweet. All start indices are inclusive.
    start: int
    # The end position (zero-based) of the recognized URL within the Tweet. This end index is exclusive.
    end: int
    # from entities.urls.url
    # The URL in the format tweeted by the user.
    url: list[str] | None
    # from entities.urls.expanded_url
    # The fully resolved URL(s).
    url_expanded: list[str] | None
    # TODO: check how url_unwound differs ("The full destination URL.")


class TwitterItemModel(SBaseModel):
    """
    Corresponds to db.models.items.TwitterItem

    For more in-depth documentation, please refer to:
    https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet
    """
    # Unique identifier for this TwitterItem, corresponds to Item
    item_id: str | UUID | None
    # Unique identifier on Twitter
    twitter_id: int | None
    # Unique user identifier on Twitter
    twitter_author_id: int | None

    # date and time this tweet was posted Format: ISO 8601 (e.g. "2019-06-04T23:12:08.000Z")
    created_at: datetime

    # language of this tweet (as provided by Twitter)
    language: str | None

    # The Tweet ID of the original Tweet of the conversation (which includes direct replies, replies of replies).
    conversation_id: int | None
    # A list of Tweets this Tweet refers to. For example, if the parent Tweet is a Retweet, a Retweet with comment
    # (also known as Quoted Tweet) or a Reply, it will include the related Tweet referenced to by its parent.
    referenced_tweets: list[ReferencedTweet] | None

    # Specifies the type of attachments (if any) present in this Tweet.
    # attachments: Optional[any] # TODO should we store that?

    # from geo.coordinates.coordinates:
    # A pair of decimal values representing the precise location of the user (latitude, longitude).
    # This value be null unless the user explicitly shared their precise location.
    latitude: float | None
    longitude: float | None

    # from entities.hashtags (Contains details about text recognized as a Hashtag.)
    hashtags: list[Hashtag] | None
    # from entities.mentions (Contains details about text recognized as a user mention.)
    mentions: list[Mention] | None
    # from entities.urls (Contains details about text recognized as a URL.)
    urls: list[URL] | None
    # from entities.cashtag (Contains details about text recognized as a Cashtag.)
    # [Cashtags are stock price symbols]
    cashtags: list[Cashtag] | None

    # Public engagement metrics for the Tweet at the time of the request.
    # taken from public_metrics.???
    retweet_count: int
    reply_count: int
    like_count: int
    quote_count: int


class TwitterMetaObject(SBaseModel):
    """
    This object contains information about the number of users returned in the current request and pagination details.
    """
    # The number of Tweet results returned in the response.
    count: int
    # The Tweet ID of the most recent Tweet returned in the response.
    newest_id: int
    # The Tweet ID of the oldest Tweet returned in the response.
    oldest_id: int
    # A value that encodes the next 'page' of results that can be requested, via the next_token request parameter.
    next_token: str
