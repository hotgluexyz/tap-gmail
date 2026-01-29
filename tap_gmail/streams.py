"""Stream type classes for tap-gmail."""

from typing import Any, Dict, Iterable, Optional

from tap_gmail.client import GmailStream

from hotglue_singer_sdk import typing as th

import base64  # noqa: E402


class MessageListStream(GmailStream):
    """Define custom stream."""

    name = "message_list"
    primary_keys = ["id"]
    replication_key = "hg_synced_at"
    records_jsonpath = "$.messages[*]"
    next_page_token_jsonpath = "$.nextPageToken"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("threadId", th.StringType),
        th.Property("hg_synced_at", th.DateTimeType),
    ).to_dict()

    @property
    def path(self):
        """Set the path for the stream."""
        return "/messages"

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"message_id": record["id"]}

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["includeSpamTrash"]=self.config["messages.include_spam_trash"]
        start_date = self.get_starting_time(context)
        if start_date:
            params["q"]="after:"+str(int(start_date.timestamp()))
        return params

    def post_process(self, row, context = None):
        row["hg_synced_at"] = self.sync_start_time
        return row


class MessagesStream(GmailStream):

    name = "messages"
    replication_key = None
    parent_stream_type = MessageListStream
    ignore_parent_replication_keys = True
    state_partitioning_keys = []
    parallelization_limit = 25

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("threadId", th.StringType),
        th.Property("labelIds", th.ArrayType(th.StringType)),
        th.Property("snippet", th.StringType),
        th.Property("historyId", th.StringType),
        th.Property("internalDate", th.StringType),
        th.Property("payload", th.CustomType({"type": ["object", "string"]})),
        th.Property("sizeEstimate", th.IntegerType),
        th.Property("raw", th.StringType),
    ).to_dict()
    
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
        return "/messages/{message_id}"

    def get_child_context(self, record, context) -> Dict:
        attachment_ids = self.find_attachment_ids(record['payload'])
        return {"message_id": record["id"],"attachment_ids": attachment_ids}

    def post_process(self, row, context = None):
        #download the file
        if row.get("payload", {}).get("body", {}).get("data"):
            #Decode the base64 data
            decoded_data = base64.urlsafe_b64decode(row['payload']['body']['data'])
            row['payload']['body']['data'] = decoded_data
        return row

    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)
        params["format"]="full"
        return params


class MessageAttachmentsStream(GmailStream):

    name = "message_attachments"
    replication_key = None
    parent_stream_type = MessagesStream
    ignore_parent_replication_keys = True
    state_partitioning_keys = []
    attachment_id = None
    file_name = None

    schema = th.PropertiesList(
        th.Property("attachmentId", th.StringType),
        th.Property("size", th.NumberType),
        th.Property("data", th.StringType),
        th.Property("filename", th.StringType),
        th.Property("message_id", th.StringType),
    ).to_dict()

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
        return "/messages/{message_id}/attachments/"+self.attachment_id

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
