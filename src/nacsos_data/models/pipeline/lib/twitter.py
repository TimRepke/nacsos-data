from datetime import datetime
from typing import Literal, Any

from .abc import TaskParams


class ImportConfigTwitter(TaskParams):
    func_name: Literal['nacsos_lib.twitter.twitter_api.search_twitter']  # type: ignore[misc]

    # One query for matching Tweets. You can learn how to build this query by reading our build a query guide.
    # You can use all available operators and can make queries up to 1,024 characters long.
    # https://developer.twitter.com/en/docs/twitter-api/tweets/counts/integrate/build-a-query
    query: str

    # The maximum number of search results to be returned by a request. A number between 10 and the system limit
    # (currently 500). By default, a request response will return 10 results.
    max_results: int | None = None

    # This parameter is used to get the next 'page' of results. The value used with the parameter is
    # pulled directly from the response provided by the API, and should not be modified. You can learn more
    # by visiting our page on pagination.
    next_token: str | None = None

    # Returns results with a Tweet ID greater than (for example, more recent than) the specified ID.
    # The ID specified is exclusive and responses will not include it. If included with the same request as
    # a start_time parameter, only since_id will be used.
    since_id: str | None = None

    # Returns results with a Tweet ID less than (that is, older than) the specified ID. Used with since_id.
    # The ID specified is exclusive and responses will not include it.
    until_id: str | None = None

    # This parameter is used to specify the order in which you want the Tweets returned.
    # By default, a request will return the most recent Tweets first (sorted by recency).
    sort_order: Literal['recency', 'relevancy'] = 'recency'

    # YYYY-MM-DDTHH:mm:ssZ (ISO 8601/RFC 3339). The oldest UTC timestamp from which the Tweets will be provided.
    # Timestamp is in second granularity and is inclusive (for example, 12:00:01 includes the first second of the
    # minute). By default, a request will return Tweets from up to 30 days ago if you do not include this parameter.
    start_time: str | datetime | None = None

    # YYYY-MM-DDTHH:mm:ssZ (ISO 8601/RFC 3339). Used with start_time. The newest, most recent UTC timestamp
    # to which the Tweets will be provided. Timestamp is in second granularity and is exclusive
    # (for example, 12:00:01 excludes the first second of the minute). If used without start_time,
    # Tweets from 30 days before end_time will be returned by default. If not specified, end_time will
    # default to [now - 30 seconds].
    end_time: str | datetime | None = None

    bearer_token: str
    results_per_response: int = 500
    max_requests: int = -1
    max_tweets: int = -1

    @property
    def payload(self) -> dict[str, Any]:
        return {
            'query': self.query,
            'bearer_token': self.bearer_token,
            'next_token': self.next_token,
            'since_id': self.since_id,
            'until_id': self.until_id,
            'sort_order': self.sort_order,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'results_per_response': self.results_per_response,
            'max_requests': self.max_requests,
            'max_tweets': self.max_tweets
        }
