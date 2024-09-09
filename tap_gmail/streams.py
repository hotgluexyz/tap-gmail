"""Stream type classes for tap-gmail."""

from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from requests import Response

from tap_gmail.client import GmailStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
import base64  # noqa: E402


class MessageListStream(GmailStream):
    """Define custom stream."""

    name = "message_list"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "message_list.json"
    records_jsonpath = "$.messages[*]"
    next_page_token_jsonpath = "$.nextPageToken"

    @property
    def path(self):
        """Set the path for the stream."""
        return "/gmail/v1/users/" + self.config["user_id"] + "/messages"

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"message_id": record["id"]}

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["includeSpamTrash"]=self.config["messages.include_spam_trash"]
        params["q"]=self.config.get("messages.q")
        return params


class MessagesStream(GmailStream):

    name = "messages"
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "messages.json"
    parent_stream_type = MessageListStream
    ignore_parent_replication_keys = True
    state_partitioning_keys = []
    
    def find_attachment_ids(self,payload):
        attachments = []

        def traverse_parts(parts):
            for part in parts:
                if 'parts' in part:
                    traverse_parts(part['parts'])
                elif part['mimeType'].startswith('image/') or 'attachmentId' in part['body']:
                    attachments.append({
                        'partId': part['partId'],
                        'mimeType': part['mimeType'],
                        'filename': part['filename'],
                        'attachmentId': part['body'].get('attachmentId')
                    })
        if "parts" in payload:
            traverse_parts(payload['parts'])
        return attachments
    @property
    def path(self):
        """Set the path for the stream."""
        return "/gmail/v1/users/" + self.config["user_id"] + "/messages/{message_id}"
    def get_child_context(self, record, context) -> Dict:
        attachment_ids = self.find_attachment_ids(record['payload'])
        return {"message_id": record["id"],"attachment_ids": attachment_ids}
class MessageAttachmentsStream(GmailStream):

    name = "message_attachments"
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "message_attachments.json"
    parent_stream_type = MessagesStream
    ignore_parent_replication_keys = True
    state_partitioning_keys = []
    attachment_id = None
    file_name = None

    def save_attachment_to_file(self,decoded_data, file_path):
        # Write the decoded data to a file
        if self.hg_sync_output_folder:
            file_path = self.hg_sync_output_folder + "/" + file_path
        with open(file_path, 'wb') as file:
            file.write(decoded_data)
    
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """
        Override the get_records function so we could yield all of sellingProgram type report for each time period
        """
        for attachment in context.get("attachment_ids",[]):
            #An email could have multiple attachments loop through and download attachment(s)
            self.attachment_id = attachment.get("attachmentId")
            self.file_name = attachment.get("filename")
            yield from super().get_records(context)        
    @property
    def path(self):
        """Set the path for the stream."""
        return "/gmail/v1/users/" + self.config["user_id"] + "/messages/{message_id}/attachments/"+self.attachment_id

    def post_process(self, row, context = None):
        #download the file
        if 'data' in row:
            #Decode the base64 data
            decoded_data = base64.urlsafe_b64decode(row['data'])
            file_name = f"{context.get('message_id')} - {self.file_name}"
            self.save_attachment_to_file(decoded_data, file_name)
            #Avoid populating the data field to keep the singer output clean
            del row['data']
            #Reference fields so we could match on them
            row['attachmentId'] = self.attachment_id
            row['filename'] = file_name
            row['message_id'] = context.get('message_id')
        return row
