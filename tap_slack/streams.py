import os
import time
from datetime import timedelta, datetime

import pytz
import singer
from singer import metadata, utils
from singer.utils import strptime_to_utc
from slack.errors import SlackApiError

from tap_slack.transform import transform_json

LOGGER = singer.get_logger()
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'
utc = pytz.UTC


class SlackStream:

    def __init__(self, webclient, config=None, catalog=None, state=None):
        self.webclient = webclient
        self.config = config
        self.catalog = catalog
        self.state = state
        if config:
            self.date_window_size = int(config.get('date_window_size', '7'))
        else:
            self.date_window_size = 7

    @staticmethod
    def get_abs_path(path):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

    def load_schema(self):
        schema_path = self.get_abs_path('schemas')
        # pylint: disable=no-member
        return singer.utils.load_json('{}/{}.json'.format(schema_path, self.name))

    def write_schema(self):
        schema = self.load_schema()
        # pylint: disable=no-member
        return singer.write_schema(stream_name=self.name, schema=schema,
                                   key_properties=self.key_properties)

    def write_state(self):
        return singer.write_state(self.state)

    def update_bookmarks(self, stream, value):
        if 'bookmarks' not in self.state:
            self.state['bookmarks'] = {}
        self.state['bookmarks'][stream] = value
        LOGGER.info('Stream: {} - Write state, bookmark value: {}'.format(stream, value))
        self.write_state()

    def get_bookmark(self, stream, default):
        # default only populated on initial sync
        if (self.state is None) or ('bookmarks' not in self.state):
            return default
        return self.state.get('bookmarks', {}).get(stream, default)

    def get_absolute_date_range(self, start_date):
        """
        Based on parameters in tap configuration, returns the absolute date range for the sync,
        including the lookback window if applicable.
        :param start_date: The start date in the config, or the last synced date from the bookmark
        :return: the start date and the end date that make up the date range
        """
        lookback_window = self.config.get('lookback_window', '14')
        start_dttm = strptime_to_utc(start_date)
        attribution_window = int(lookback_window)
        now_dttm = utils.now()
        delta_days = (now_dttm - start_dttm).days
        if delta_days < attribution_window:
            start = now_dttm - timedelta(days=attribution_window)
        else:
            start = start_dttm

        return start, now_dttm

    def _all_channels(self):
        types = "public_channel"
        enable_private_channels = self.config.get("private_channels", "false")
        exclude_archived = self.config.get("exclude_archived", "false")
        if enable_private_channels == "true":
            types = "public_channel,private_channel"

        try:
            conversations_list = self.webclient.conversations_list(
                limit=100,
                exclude_archived=exclude_archived,
                types=types)
        except SlackApiError as err:
            if err.response.data["error"] == "ratelimited":
                # If we've hit rate limits there will be a header indicating how
                # long to wait to make another request
                delay = int(err.response.headers['Retry-After'])
                time.sleep(delay)
                conversations_list = self.webclient.conversations_list(
                    limit=100,
                    exclude_archived=exclude_archived,
                    types=types)
            else:
                raise err
        for page in conversations_list:
            channels = page.get('channels')
            for channel in channels:
                yield channel

    def _specified_channels(self):
        for channel_id in self.config.get("channels"):
            try:
                page = self.webclient.conversations_info(channel=channel_id, include_num_members=0)
            except SlackApiError as err:
                if err.response.data["error"] == "ratelimited":
                    # If we've hit rate limits there will be a header indicating how
                    # long to wait to make another request
                    delay = int(err.response.headers['Retry-After'])
                    time.sleep(delay)
                    page = self.webclient.conversations_info(channel=channel_id,
                                                             include_num_members=0)
                else:
                    raise err
            yield page.get('channel')

    def channels(self):
        if "channels" in self.config:
            yield from self._specified_channels()
        else:
            yield from self._all_channels()


# ConversationsStream = Slack Channels
class ConversationsStream(SlackStream):
    name = 'channels'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    forced_replication_method = 'FULL_TABLE'
    valid_replication_keys = []
    date_fields = ['created']

    def sync(self, mdata):
        schema = self.load_schema()

        # pylint: disable=unused-variable
        with singer.metrics.job_timer(job_type='list_conversations') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:
                channels = self.channels()
                transformed_channels = transform_json(stream=self.name, data=channels,
                                                      date_fields=self.date_fields)
                for channel in transformed_channels:
                    with singer.Transformer(
                            integer_datetime_fmt="unix-seconds-integer-datetime-parsing") \
                            as transformer:
                        transformed_record = transformer.transform(data=channel, schema=schema,
                                                                   metadata=metadata.to_map(mdata))
                        singer.write_record(stream_name=self.name,
                                            time_extracted=singer.utils.now(),
                                            record=transformed_record)
                        counter.increment()


# ConversationsMembersStream = Slack Channel Members (Users)
class ConversationMembersStream(SlackStream):
    name = 'channel_members'
    key_properties = ['channel_id', 'user_id']
    replication_method = 'FULL_TABLE'
    forced_replication_method = 'FULL_TABLE'
    valid_replication_keys = []
    date_fields = []

    def sync(self, mdata):

        schema = self.load_schema()

        # pylint: disable=unused-variable
        with singer.metrics.job_timer(job_type='list_conversation_members') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:
                for channel in self.channels():
                    channel_id = channel.get('id')

                    try:
                        members_cursor = self.webclient.conversations_members(channel=channel_id)
                    except SlackApiError as err:
                        # Ignore fetch_members_error for archived channels
                        error_response = err.response.data["error"]
                        if error_response == 'fetch_members_failed':
                            LOGGER.warning('Failed to fetch members for channel: {}'
                                           .format(channel_id))
                            members_cursor = []
                        elif error_response == 'ratelimited':
                            # If we've hit rate limits there will be a header indicating how
                            # long to wait to make another request
                            delay = int(err.response.headers['Retry-After'])
                            time.sleep(delay)
                            members_cursor = self.webclient.conversations_members(
                                channel=channel_id)
                        else:
                            raise err

                    for page in members_cursor:
                        members = page.get('members')
                        for member in members:
                            data = {'channel_id': channel_id, 'user_id': member}
                            with singer.Transformer() as transformer:
                                transformed_record = transformer.transform(data=data, schema=schema,
                                                                           metadata=metadata.to_map(
                                                                               mdata))
                                singer.write_record(stream_name=self.name,
                                                    time_extracted=singer.utils.now(),
                                                    record=transformed_record)
                                counter.increment()


# ConversationsHistoryStream = Slack Messages (not including reply threads)
class ConversationHistoryStream(SlackStream):
    name = 'messages'
    key_properties = ['channel_id', 'ts']
    replication_method = 'INCREMENTAL'
    forced_replication_method = 'INCREMENTAL'
    valid_replication_keys = ['channel_id', 'ts']
    date_fields = ['ts']

    # pylint: disable=arguments-differ
    def update_bookmarks(self, channel_id, value):
        """
        For the messages stream, bookmarks are written per-channel.
        :param channel_id: The channel to bookmark
        :param value: The earliest message date in the window.
        :return: None
        """
        if 'bookmarks' not in self.state:
            self.state['bookmarks'] = {}
        if self.name not in self.state['bookmarks']:
            self.state['bookmarks'][self.name] = {}
        self.state['bookmarks'][self.name][channel_id] = value
        LOGGER.info('Stream: {}, channel id: {} - Write state, bookmark value: {}'
                    .format(self.name, channel_id, value))
        self.write_state()

    # pylint: disable=arguments-differ
    def get_bookmark(self, channel_id, default):
        """
        Gets the channel's bookmark value, if present, otherwise a default value passed in.
        :param channel_id: The channel to retrieve the bookmark for.
        :param default: The default value to return if no bookmark
        :return: The bookmark or default value passed in
        """
        # default only populated on initial sync
        if (self.state is None) or ('bookmarks' not in self.state):
            return default
        return self.state.get('bookmarks', {}).get(self.name, {channel_id: default}) \
            .get(channel_id, default)

    # pylint: disable=too-many-branches,too-many-statements
    def sync(self, mdata):

        schema = self.load_schema()
        threads_stream = None
        threads_mdata = None

        # If threads are also being synced we'll need to do that for each message
        for catalog_entry in self.catalog.get_selected_streams(self.state):
            if catalog_entry.stream == 'threads':
                threads_mdata = catalog_entry.metadata
                threads_stream = ThreadsStream(webclient=self.webclient, config=self.config,
                                               catalog=self.catalog, state=self.state)

        # pylint: disable=unused-variable
        with singer.metrics.job_timer(job_type='list_conversation_history') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:
                for channel in self.channels():
                    channel_id = channel.get('id')

                    bookmark_date = self.get_bookmark(channel_id, self.config.get('start_date'))
                    start, end = self.get_absolute_date_range(bookmark_date)

                    # Window the requests based on the tap configuration
                    date_window_start = start
                    date_window_end = start + timedelta(days=int(self.date_window_size))
                    min_bookmark = start
                    max_bookmark = start

                    while date_window_start < date_window_end:
                        try:
                            messages = self.webclient \
                                .conversations_history(channel=channel_id,
                                                       oldest=int(date_window_start.timestamp()),
                                                       latest=int(date_window_end.timestamp()))
                        except SlackApiError as err:
                            if err.response.data["error"] == "ratelimited":
                                # If we've hit rate limits there will be a header indicating how
                                # long to wait to make another request
                                delay = int(err.response.headers['Retry-After'])
                                time.sleep(delay)
                                try:
                                    messages = self.webclient \
                                        .conversations_history(channel=channel_id,
                                                               oldest=int(
                                                                   date_window_start.timestamp()),
                                                               latest=int(
                                                                   date_window_end.timestamp()))
                                except SlackApiError as err_two:
                                    if err_two.response.data["error"] == "not_in_channel":
                                        # The tap config might dictate that archived channels should
                                        # be processed, but if the slackbot was not made a member of
                                        # those channels prior to archiving attempting to get the
                                        # messages will throw an error
                                        LOGGER.warning(
                                            'Attempted to get messages for channel: {} that '
                                            'slackbot is not in'.format(
                                                channel_id
                                            ))
                                        messages = None
                                        date_window_start = date_window_end
                            elif err.response.data["error"] == "not_in_channel":
                                # The tap config might dictate that archived channels should be
                                # processed, but if the slackbot was not made a member of those
                                # channels prior to archiving attempting to get the messages will
                                # throw an error
                                LOGGER.warning(
                                    'Attempted to get messages for channel: {} that slackbot is '
                                    'not in'.format(
                                        channel_id))
                                messages = None
                                date_window_start = date_window_end
                            else:
                                raise err
                        if messages:
                            for page in messages:
                                messages = page.get('messages')
                                transformed_messages = transform_json(stream=self.name,
                                                                      data=messages,
                                                                      date_fields=self.date_fields,
                                                                      channel_id=channel_id)
                                for message in transformed_messages:
                                    data = {'channel_id': channel_id}
                                    data = {**data, **message}

                                    if threads_stream:
                                        # If threads is selected we need to sync all the
                                        # threaded replies to this message
                                        threads_stream.write_schema()
                                        threads_stream.sync(mdata=threads_mdata,
                                                            channel_id=channel_id,
                                                            ts=data.get('thread_ts'))
                                        threads_stream.write_state()
                                    with singer.Transformer(
                                            integer_datetime_fmt=
                                            "unix-seconds-integer-datetime-parsing"
                                    ) as transformer:
                                        transformed_record = transformer.transform(
                                            data=data,
                                            schema=schema,
                                            metadata=metadata.to_map(mdata)
                                        )
                                        record_timestamp = \
                                            transformed_record.get('thread_ts', '').partition('.')[
                                                0]
                                        record_timestamp_int = int(record_timestamp)
                                        if record_timestamp_int >= start.timestamp():
                                            singer.write_record(stream_name=self.name,
                                                                time_extracted=singer.utils.now(),
                                                                record=transformed_record)
                                            counter.increment()

                                            if datetime.utcfromtimestamp(
                                                    record_timestamp_int).replace(
                                                tzinfo=utc) > max_bookmark.replace(tzinfo=utc):
                                                # Records are sorted by most recent first, so this
                                                # should only fire once every sync, per channel
                                                max_bookmark = datetime.fromtimestamp(
                                                    record_timestamp_int)
                                            elif datetime.utcfromtimestamp(
                                                    record_timestamp_int).replace(
                                                tzinfo=utc) < min_bookmark:
                                                # The min bookmark tracks how far back we've synced
                                                # during the sync, since the records are ordered
                                                # newest -> oldest
                                                min_bookmark = datetime.fromtimestamp(
                                                    record_timestamp_int)
                                self.update_bookmarks(channel_id,
                                                      min_bookmark.strftime(DATETIME_FORMAT))
                            # Update the date window
                            date_window_start = date_window_end
                            date_window_end = date_window_start + timedelta(
                                days=self.date_window_size)
                            if date_window_end > end:
                                date_window_end = end


# UsersStream = Slack Users
class UsersStream(SlackStream):
    name = 'users'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_key = 'updated'
    valid_replication_keys = ['updated_at']
    date_fields = ['updated']

    def sync(self, mdata):

        schema = self.load_schema()
        bookmark = singer.get_bookmark(state=self.state, tap_stream_id=self.name,
                                       key=self.replication_key)
        if bookmark is None:
            bookmark = self.config.get('start_date')
        new_bookmark = bookmark

        # pylint: disable=unused-variable
        with singer.metrics.job_timer(job_type='list_users') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:
                try:
                    users_list = self.webclient.users_list(limit=100)
                except SlackApiError as err:
                    # If we've hit rate limits there will be a header indicating how
                    # long to wait to make another request
                    if err.response.data["error"] == "ratelimited":
                        delay = int(err.response.headers['Retry-After'])
                        time.sleep(delay)
                        users_list = self.webclient.users_list(limit=100)
                    else:
                        raise err

                for page in users_list:
                    users = page.get('members')
                    transformed_users = transform_json(stream=self.name, data=users,
                                                       date_fields=self.date_fields)
                    for user in transformed_users:
                        with singer.Transformer(
                                integer_datetime_fmt="unix-seconds-integer-datetime-parsing") \
                                as transformer:
                            transformed_record = transformer.transform(data=user, schema=schema,
                                                                       metadata=metadata.to_map(
                                                                           mdata))
                            new_bookmark = max(new_bookmark, transformed_record.get('updated'))
                            if (self.replication_method == 'INCREMENTAL'
                                and transformed_record.get('updated') > bookmark) \
                                    or self.replication_method == 'FULL_TABLE':
                                singer.write_record(stream_name=self.name,
                                                    time_extracted=singer.utils.now(),
                                                    record=transformed_record)
                                counter.increment()

        self.state = singer.write_bookmark(state=self.state, tap_stream_id=self.name,
                                           key=self.replication_key, val=new_bookmark)


# ThreadsStream = Slack Message Threads (Replies to Slack message)
class ThreadsStream(SlackStream):
    name = 'threads'
    key_properties = ['channel_id', 'ts', 'thread_ts']
    replication_method = 'FULL_TABLE'
    replication_key = 'updated'
    valid_replication_keys = ['updated_at']
    date_fields = ['ts', 'last_read']

    def sync(self, mdata, channel_id, ts):
        schema = self.load_schema()
        start, end = self.get_absolute_date_range(self.config.get('start_date'))

        # pylint: disable=unused-variable
        with singer.metrics.job_timer(job_type='list_threads') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:
                try:
                    replies = self.webclient.conversations_replies(channel=channel_id, ts=ts,
                                                                   inclusive="true",
                                                                   oldest=int(start.timestamp()),
                                                                   latest=int(end.timestamp()))
                except SlackApiError as err:
                    if err.response.data["error"] == "ratelimited":
                        # If we've hit rate limits there will be a header indicating how
                        # long to wait to make another request
                        delay = int(err.response.headers['Retry-After'])
                        time.sleep(delay)
                        replies = self.webclient.conversations_replies(channel=channel_id, ts=ts,
                                                                       inclusive="true",
                                                                       oldest=int(
                                                                           start.timestamp()),
                                                                       latest=int(end.timestamp()))
                    else:
                        raise err

                for page in replies:
                    transformed_threads = transform_json(stream=self.name,
                                                         data=page.get('messages', []),
                                                         date_fields=self.date_fields,
                                                         channel_id=channel_id)
                    for message in transformed_threads:
                        with singer.Transformer(
                                integer_datetime_fmt="unix-seconds-integer-datetime-parsing") \
                                as transformer:
                            transformed_record = transformer.transform(data=message, schema=schema,
                                                                       metadata=metadata.to_map(
                                                                           mdata))
                            singer.write_record(stream_name=self.name,
                                                time_extracted=singer.utils.now(),
                                                record=transformed_record)
                            counter.increment()


# UserGroupsStream = Slack User Groups
class UserGroupsStream(SlackStream):
    name = 'user_groups'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    valid_replication_keys = []

    def sync(self, mdata):
        schema = self.load_schema()

        # pylint: disable=unused-variable
        with singer.metrics.job_timer(job_type='list_user_groups') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:
                try:
                    usergroups_list = self.webclient.usergroups_list(include_count="true",
                                                                     include_disabled="true",
                                                                     include_user="true")
                except SlackApiError as err:
                    if err.response.data["error"] == "ratelimited":
                        # If we've hit rate limits there will be a header indicating how
                        # long to wait to make another request
                        delay = int(err.response.headers['Retry-After'])
                        time.sleep(delay)
                        usergroups_list = self.webclient.usergroups_list(include_count="true",
                                                                         include_disabled="true",
                                                                         include_user="true")
                    else:
                        raise err

                for page in usergroups_list:
                    for usergroup in page.get('usergroups'):
                        with singer.Transformer(
                                integer_datetime_fmt="unix-seconds-integer-datetime-parsing") \
                                as transformer:
                            transformed_record = transformer.transform(data=usergroup,
                                                                       schema=schema,
                                                                       metadata=metadata.to_map(
                                                                           mdata))
                            singer.write_record(stream_name=self.name,
                                                time_extracted=singer.utils.now(),
                                                record=transformed_record)
                            counter.increment()


# TeamsStream = Slack Teams
class TeamsStream(SlackStream):
    name = 'teams'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    replication_key = 'updated'
    valid_replication_keys = ['updated_at']
    date_fields = []

    def sync(self, mdata):
        schema = self.load_schema()

        # pylint: disable=unused-variable
        with singer.metrics.job_timer(job_type='team_info') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:
                try:
                    team_info = self.webclient.team_info()
                except SlackApiError as err:
                    if err.response.data["error"] == "ratelimited":
                        delay = int(err.response.headers['Retry-After'])
                        time.sleep(delay)
                        team_info = self.webclient.team_info()
                    else:
                        raise err

                for page in team_info:
                    team = page.get('team')
                    with singer.Transformer(
                            integer_datetime_fmt="unix-seconds-integer-datetime-parsing") \
                            as transformer:
                        transformed_record = transformer.transform(data=team,
                                                                   schema=schema,
                                                                   metadata=metadata.to_map(
                                                                       mdata))
                        singer.write_record(stream_name=self.name,
                                            time_extracted=singer.utils.now(),
                                            record=transformed_record)
                        counter.increment()


# FilesStream = Files uploaded/shared to Slack and hosted by Slack
class FilesStream(SlackStream):
    name = 'files'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_key = 'updated'
    valid_replication_keys = ['updated_at']
    date_fields = []

    def sync(self, mdata):
        schema = self.load_schema()

        # pylint: disable=unused-variable
        with singer.metrics.job_timer(job_type='list_files') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:

                bookmark_date = self.get_bookmark(self.name, self.config.get('start_date'))
                start, end = self.get_absolute_date_range(bookmark_date)

                # Window the requests based on the tap configuration
                date_window_start = start
                date_window_end = start + timedelta(days=int(self.date_window_size))
                min_bookmark = start
                max_bookmark = start

                while date_window_start < date_window_end:
                    try:
                        files_list = self.webclient \
                            .files_list(from_ts=int(date_window_start.timestamp()),
                                        to_ts=int(date_window_end.timestamp()))
                    except SlackApiError as err:
                        if err.response.data["error"] == "ratelimited":
                            # If we've hit rate limits there will be a header indicating how
                            # long to wait to make another request
                            delay = int(err.response.headers['Retry-After'])
                            time.sleep(delay)
                            files_list = self.webclient \
                                .files_list(from_ts=int(date_window_start.timestamp()),
                                            to_ts=int(date_window_end.timestamp()))
                        else:
                            raise err

                    for page in files_list:
                        files = page.get('files')
                        transformed_files = transform_json(stream=self.name,
                                                           data=files,
                                                           date_fields=self.date_fields)
                        for file in transformed_files:
                            with singer.Transformer(
                                    integer_datetime_fmt="unix-seconds-integer-datetime-parsing"
                            ) as transformer:
                                transformed_record = transformer.transform(
                                    data=file,
                                    schema=schema,
                                    metadata=metadata.to_map(mdata)
                                )
                                record_timestamp = \
                                    file.get('timestamp', '')
                                record_timestamp_int = int(record_timestamp)

                                if record_timestamp_int >= start.timestamp():
                                    singer.write_record(stream_name=self.name,
                                                        time_extracted=singer.utils.now(),
                                                        record=transformed_record)
                                    counter.increment()

                                    if datetime.utcfromtimestamp(
                                            record_timestamp_int).replace(
                                        tzinfo=utc) > max_bookmark.replace(tzinfo=utc):
                                        # Records are sorted by most recent first, so this
                                        # should only fire once every sync, per channel
                                        max_bookmark = datetime.fromtimestamp(
                                            record_timestamp_int)
                                    elif datetime.utcfromtimestamp(
                                            record_timestamp_int).replace(
                                        tzinfo=utc) < min_bookmark:
                                        # The min bookmark tracks how far back we've synced
                                        # during the sync, since the records are ordered
                                        # newest -> oldest
                                        min_bookmark = datetime.fromtimestamp(
                                            record_timestamp_int)
                        self.update_bookmarks(self.name, min_bookmark.strftime(DATETIME_FORMAT))
                    # Update the date window
                    date_window_start = date_window_end
                    date_window_end = date_window_start + timedelta(
                        days=self.date_window_size)
                    if date_window_end > end:
                        date_window_end = end


# RemoteFilesStream = Files shared to Slack but not hosted by Slack
class RemoteFilesStream(SlackStream):
    name = 'remote_files'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_key = 'updated'
    valid_replication_keys = ['updated_at']
    date_fields = []

    def sync(self, mdata):
        schema = self.load_schema()

        # pylint: disable=unused-variable
        with singer.metrics.job_timer(job_type='list_files') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:

                bookmark_date = self.get_bookmark(self.name, self.config.get('start_date'))
                start, end = self.get_absolute_date_range(bookmark_date)

                # Window the requests based on the tap configuration
                date_window_start = start
                date_window_end = start + timedelta(days=int(self.date_window_size))
                min_bookmark = start
                max_bookmark = start

                while date_window_start < date_window_end:
                    try:
                        remote_files_list = self.webclient \
                            .files_remote_list(from_ts=int(date_window_start.timestamp()),
                                               to_ts=int(date_window_end.timestamp()))
                    except SlackApiError as err:
                        if err.response.data["error"] == "ratelimited":
                            # If we've hit rate limits there will be a header indicating how
                            # long to wait to make another request
                            delay = int(err.response.headers['Retry-After'])
                            time.sleep(delay)
                            remote_files_list = self.webclient \
                                .files_remote_list(from_ts=int(date_window_start.timestamp()),
                                                   to_ts=int(date_window_end.timestamp()))
                        else:
                            raise err

                    for page in remote_files_list:
                        remote_files = page.get('files')
                        transformed_files = transform_json(stream=self.name,
                                                           data=remote_files,
                                                           date_fields=self.date_fields)
                        for file in transformed_files:
                            with singer.Transformer(
                                    integer_datetime_fmt="unix-seconds-integer-datetime-parsing"
                            ) as transformer:
                                transformed_record = transformer.transform(
                                    data=file,
                                    schema=schema,
                                    metadata=metadata.to_map(mdata)
                                )
                                record_timestamp = \
                                    file.get('timestamp', '')
                                record_timestamp_int = int(record_timestamp)

                                if record_timestamp_int >= start.timestamp():
                                    singer.write_record(stream_name=self.name,
                                                        time_extracted=singer.utils.now(),
                                                        record=transformed_record)
                                    counter.increment()

                                    if datetime.utcfromtimestamp(
                                            record_timestamp_int).replace(
                                        tzinfo=utc) > max_bookmark.replace(tzinfo=utc):
                                        # Records are sorted by most recent first, so this
                                        # should only fire once every sync, per channel
                                        max_bookmark = datetime.fromtimestamp(
                                            record_timestamp_int)
                                    elif datetime.utcfromtimestamp(
                                            record_timestamp_int).replace(
                                        tzinfo=utc) < min_bookmark:
                                        # The min bookmark tracks how far back we've synced
                                        # during the sync, since the records are ordered
                                        # newest -> oldest
                                        min_bookmark = datetime.fromtimestamp(
                                            record_timestamp_int)
                        self.update_bookmarks(self.name, min_bookmark.strftime(DATETIME_FORMAT))
                    # Update the date window
                    date_window_start = date_window_end
                    date_window_end = date_window_start + timedelta(
                        days=self.date_window_size)
                    if date_window_end > end:
                        date_window_end = end


AVAILABLE_STREAMS = {
    "channels": ConversationsStream,
    "users": UsersStream,
    "channel_members": ConversationMembersStream,
    "messages": ConversationHistoryStream,
    "threads": ThreadsStream,
    "user_groups": UserGroupsStream,
    "teams": TeamsStream,
    "files": FilesStream,
    "remote_files": RemoteFilesStream,
}
