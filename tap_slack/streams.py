from slack import WebClient

import singer
import os
import time

from singer import metadata

LOGGER = singer.get_logger()


class SlackStream():

    def __init__(self, webclient, config=None, catalog_stream=None, state=None):
        self.webclient = webclient
        self.config = config
        self.catalog_stream = catalog_stream
        self.state = state

    def get_abs_path(self, path):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

    def load_schema(self):
        schema_path = self.get_abs_path('schemas')
        return singer.utils.load_json('{}/{}.json'.format(schema_path, self.name))

    def write_schema(self):
        schema = self.load_schema()
        return singer.write_schema(stream_name=self.name, schema=schema, key_properties=self.key_properties)

    def write_state(self):
        return singer.write_state(self.state)

    def _all_channels(self):
        types = "public_channel"
        enable_private_channels = self.config.get("private_channels", True)
        if enable_private_channels:
            types = "public_channel,private_channel"

        for page in self.webclient.conversations_list(limit=100, exclude_archived='false', types=types):
            channels = page.get('channels')
            for channel in channels:
                yield channel

    def _specified_channels(self):
        for channel_id in self.config.get("channels"):
            page = self.webclient.conversations_info(channel=channel_id, include_num_members=0)
            yield page.get('channel')

    def channels(self):
        if "channels" in self.config:
            yield from self._specified_channels()
        else:
            yield from self._all_channels()

class ConversationsStream(SlackStream):
    name = 'conversations'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    forced_replication_method = 'FULL_TABLE'
    valid_replication_keys = []

    def sync(self, mdata):

        schema = self.load_schema()

        with singer.metrics.job_timer(job_type='list_conversations') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:
                for channel in self.channels():
                    with singer.Transformer(integer_datetime_fmt="unix-seconds-integer-datetime-parsing") as transformer:
                        transformed_record = transformer.transform(data=channel, schema=schema, metadata=metadata.to_map(mdata))
                        singer.write_record(stream_name=self.name, time_extracted=singer.utils.now(), record=transformed_record)
                        counter.increment()


class ConversationMembersStream(SlackStream):
    name = 'conversation_members'
    key_properties = ['channel_id','user_id']
    replication_method = 'FULL_TABLE'
    forced_replication_method = 'FULL_TABLE'
    valid_replication_keys = []

    def sync(self, mdata):

        schema = self.load_schema()

        with singer.metrics.job_timer(job_type='list_conversation_members') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:
                for channel in self.channels():
                    channel_id = channel.get('id')
                    for page in self.webclient.conversations_members(channel=channel_id):
                        members = page.get('members')
                        for member in members:
                            data = {}
                            data['channel_id'] = channel_id
                            data['user_id'] = member
                            with singer.Transformer() as transformer:
                                transformed_record = transformer.transform(data=data, schema=schema, metadata=metadata.to_map(mdata))
                                singer.write_record(stream_name=self.name, time_extracted=singer.utils.now(), record=transformed_record)
                                counter.increment()


class ConversationHistoryStream(SlackStream):
    name = 'conversation_history'
    key_properties = ['channel_id','ts']
    replication_method = 'FULL_TABLE'
    forced_replication_method = 'FULL_TABLE'
    valid_replication_keys = []

    def sync(self, mdata):

        schema = self.load_schema()

        with singer.metrics.job_timer(job_type='list_conversation_history') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:
                for channel in self.channels():
                    channel_id = channel.get('id')
                    for page in self.webclient.conversations_history(channel=channel_id):
                        messages = page.get('messages')
                        for message in messages:
                            data = {}
                            data['channel_id'] = channel_id
                            data = {**data, **message}
                            with singer.Transformer(integer_datetime_fmt="unix-seconds-integer-datetime-parsing") as transformer:
                                transformed_record = transformer.transform(data=data, schema=schema, metadata=metadata.to_map(mdata))
                                singer.write_record(stream_name=self.name, time_extracted=singer.utils.now(), record=transformed_record)
                                counter.increment()
                        #TODO: handle rate limiting better than this.
                        time.sleep(1)


class UsersStream(SlackStream):
    name = 'users'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_key = 'updated'
    valid_replication_keys = ['updated_at']

    def sync(self, mdata):

        schema = self.load_schema()
        bookmark = singer.get_bookmark(state=self.state, tap_stream_id=self.name, key=self.replication_key)
        if bookmark is None:
            bookmark = self.config.get('start_date')
        new_bookmark = bookmark

        with singer.metrics.job_timer(job_type='list_users') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:
                for page in self.webclient.users_list(limit=100):
                    users = page.get('members')
                    for user in users:
                        with singer.Transformer(integer_datetime_fmt="unix-seconds-integer-datetime-parsing") as transformer:
                            transformed_record = transformer.transform(data=user, schema=schema, metadata=metadata.to_map(mdata))
                            new_bookmark = max(new_bookmark, transformed_record.get('updated'))
                            if (self.replication_method == 'INCREMENTAL' and transformed_record.get('updated') > bookmark) or self.replication_method == 'FULL_TABLE':
                                singer.write_record(stream_name=self.name, time_extracted=singer.utils.now(), record=transformed_record)
                                counter.increment()

        self.state = singer.write_bookmark(state=self.state, tap_stream_id=self.name, key=self.replication_key, val=new_bookmark)



AVAILABLE_STREAMS = {
    "conversations": ConversationsStream,
    "users": UsersStream,
    "conversation_members": ConversationMembersStream,
    "conversation_history": ConversationHistoryStream
}


