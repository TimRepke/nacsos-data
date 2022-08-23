from typing import Literal
from datetime import datetime
from uuid import UUID
from pydantic import BaseModel


class ReferencedTweet(BaseModel):
    id: int
    type: Literal['retweeted', 'quoted', 'replied_to']


class Cashtag(BaseModel):
    # The start position (zero-based) of the recognized Cashtag within the Tweet. All start indices are inclusive.
    start: int
    # The end position (zero-based) of the recognized Cashtag within the Tweet. This end index is exclusive.
    end: int
    # The text of the Cashtag.
    tag: str


class Hashtag(BaseModel):
    # The start position (zero-based) of the recognized Hashtag within the Tweet. All start indices are inclusive.
    start: int
    # The end position (zero-based) of the recognized Hashtag within the Tweet. This end index is exclusive.
    end: int
    # The text of the Hashtag.
    tag: str


class Mention(BaseModel):
    # The start position (zero-based) of the recognized user mention within the Tweet. All start indices are inclusive.
    start: int
    # The end position (zero-based) of the recognized user mention within the Tweet. This end index is exclusive.
    end: int
    # The part of text recognized as a user mention.
    username: str
    # The twitter id for that user
    user_id: int


class URL(BaseModel):
    # The start position (zero-based) of the recognized URL within the Tweet. All start indices are inclusive.
    start: int
    # The end position (zero-based) of the recognized URL within the Tweet. This end index is exclusive.
    end: int
    # from entities.urls.url
    # The URL in the format tweeted by the user.
    url: str
    # from entities.urls.expanded_url
    # The fully resolved URL(s).
    url_expanded: str
    # TODO: check how url_unwound differs ("The full destination URL.")


class ContextAnnotation(BaseModel):
    """
    Flattened and reduced version of the context_annotation object
    https://developer.twitter.com/en/docs/twitter-api/annotations/overview

    NOTE: Under the assumption that we could always recover the `description`
          of the domain and entity, this information is not stored to save space.
    """
    # ID of the top level context (aka domain)
    domain_id: int
    # Name of the top level context
    domain_name: str
    # ID of the second-level context (e.g. topic, named entity, ...)
    entity_id: int
    # Name of the second-level context
    entity_name: str


class TwitterUserModel(BaseModel):
    """
    Flattened and reduced representation of a Twitter User Object
    https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/user

    NOTE:
        - In the context of a `TwitterItemModel`, the `id` is not set as it can be inferred from `twitter_author_id`
        - `name` is None if `name` == `username` to save space
    """
    # The unique identifier of this user.
    id: int | None = None
    # The UTC datetime that the user account was created on Twitter.
    created_at: datetime
    # The name of the user, as they’ve defined it on their profile.
    # Not necessarily a person’s name. Typically capped at 50 characters, but subject to change.
    name: str | None = None
    # The Twitter screen name, handle, or alias that this user identifies themselves with.
    # Usernames are unique but subject to change. Typically a maximum of 15 characters long,
    # but some historical accounts may exist with longer names.
    username: str
    # Indicates whether or not this Twitter user has a verified account.
    # A verified account lets people know that an account of public interest is authentic.
    verified: bool
    # The text of this user's profile description (also known as bio), if the user provided one.
    description: str | None = None
    # The location specified in the user's profile, if the user provided one.
    # As this is a freeform value, it may not indicate a valid location, but it may be
    # fuzzily evaluated when performing searches with location queries.
    location: str | None = None

    # Attributes from `public_metrics` (Contains details about activity for this user)
    followers_count: int | None = None
    following_count: int | None = None
    tweet_count: int | None = None
    listed_count: int | None = None


class TwitterItemModel(BaseModel):
    """
    Corresponds to db.models.items.TwitterItem

    For more in-depth documentation, please refer to:
    https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet
    """
    # Unique identifier for this TwitterItem, corresponds to Item
    item_id: str | UUID | None = None
    # Unique identifier on Twitter
    twitter_id: int | None = None
    # Unique user identifier on Twitter
    twitter_author_id: int | None = None

    # text of the tweet (in Twitter lingo, it's the "status")
    status: str

    # date and time this tweet was posted Format: ISO 8601 (e.g. "2019-06-04T23:12:08.000Z")
    created_at: datetime

    # language of this tweet (as provided by Twitter)
    language: str | None = None

    # The Tweet ID of the original Tweet of the conversation (which includes direct replies, replies of replies).
    conversation_id: int | None = None
    # A list of Tweets this Tweet refers to. For example, if the parent Tweet is a Retweet, a Retweet with comment
    # (also known as Quoted Tweet) or a Reply, it will include the related Tweet referenced to by its parent.
    referenced_tweets: list[ReferencedTweet] | None = None

    # Specifies the type of attachments (if any) present in this Tweet.
    # attachments: Optional[any] # TODO should we store that?

    # from geo.coordinates.coordinates:
    # A pair of decimal values representing the precise location of the user (latitude, longitude).
    # This value be null unless the user explicitly shared their precise location.
    latitude: float | None = None
    longitude: float | None = None

    # from entities.hashtags (Contains details about text recognized as a Hashtag.)
    hashtags: list[Hashtag] | None = None
    # from entities.mentions (Contains details about text recognized as a user mention.)
    mentions: list[Mention] | None = None
    # from entities.urls (Contains details about text recognized as a URL.)
    urls: list[URL] | None = None
    # from entities.cashtag (Contains details about text recognized as a Cashtag.)
    # [Cashtags are stock price symbols]
    cashtags: list[Cashtag] | None = None
    # from context_annotations (Contains context annotations for the Tweet.)
    # Entity recognition/extraction, topical analysis
    annotations: list[ContextAnnotation] | None = None

    # Public engagement metrics for the Tweet at the time of the request.
    # taken from public_metrics.???
    retweet_count: int
    reply_count: int
    like_count: int
    quote_count: int

    user: TwitterUserModel | None = None


class TwitterMetaObject(BaseModel):
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
