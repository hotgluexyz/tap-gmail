"""Gmail tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_gmail.streams import GmailStream, MessageListStream, MessagesStream, MessageAttachmentsStream

STREAM_TYPES = [MessageListStream, MessagesStream,MessageAttachmentsStream]


class TapGmail(Tap):
    """Gmail tap class."""

    name = "tap-gmail"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType,
            description="Your google client_id",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            description="Your google client_secret",
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            description="Your google refresh token",
        ),
        th.Property(
            "messages.q",
            th.StringType,
            description="Only return messages matching the specified query. Supports the same query format as the Gmail search box. For example, \"from:someuser@example.com rfc822msgid:<somemsgid@example.com> is:unread\". Parameter cannot be used when accessing the api using the gmail.metadata scope. https://developers.google.com/gmail/api/reference/rest/v1/users.messages/list#query-parameters",
        ),
        th.Property("user_id", th.StringType, description="Your Gmail User ID"),
        th.Property(
            "messages.include_spam_trash",
            th.BooleanType,
            description="Include messages from SPAM and TRASH in the results.",
            default=False,
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

if __name__ == '__main__':
    TapGmail.cli()    
