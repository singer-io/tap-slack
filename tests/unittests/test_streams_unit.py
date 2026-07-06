from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from tap_slack.streams import (
    ConversationHistoryStream,
    ConversationMembersStream,
    ConversationsStream,
    FilesStream,
    RemoteFilesStream,
    TeamsStream,
    ThreadsStream,
    UserGroupsStream,
    UsersStream,
)


@contextmanager
def dummy_job_timer(**kwargs):
    yield None


class DummyCounter:
    def __init__(self):
        self.value = 0

    def increment(self):
        self.value += 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyTransformer:
    def __init__(self, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def transform(self, data, schema, metadata):
        return data


@pytest.fixture
def patched_stream_env():
    with patch("tap_slack.streams.singer.metrics.job_timer", side_effect=dummy_job_timer), patch(
        "tap_slack.streams.singer.metrics.record_counter", side_effect=lambda **_: DummyCounter()
    ), patch("tap_slack.streams.singer.Transformer", DummyTransformer), patch(
        "tap_slack.streams.metadata.to_map", side_effect=lambda m: m
    ), patch("tap_slack.streams.singer.write_record"), patch(
        "tap_slack.streams.singer.write_state"
    ), patch(
        "tap_slack.streams.singer.utils.now", return_value=datetime(2025, 1, 2, tzinfo=timezone.utc)
    ):
        yield


def make_client():
    client = MagicMock()
    client.config = {}
    client.webclient = MagicMock()
    return client


def test_slackstream_helpers_and_channel_selection(patched_stream_env):
    client = make_client()
    stream = ConversationsStream(client=client, config={"private_channels": "true"}, state={})

    assert stream.load_schema()["type"] == ["null", "object"]
    assert stream.get_bookmark("x", "default") == "default"

    stream.state = {}
    stream.update_bookmarks("x", "2025-01-01T00:00:00")
    assert stream.state["bookmarks"]["x"] == "2025-01-01T00:00:00"

    start, end = stream.get_absolute_date_range("2025-01-01T00:00:00Z")
    assert start.tzinfo is not None
    assert end.tzinfo is not None

    client.get_all_channels.return_value = [{"channels": [{"id": "C1"}, {"id": "C2"}]}]
    assert list(stream.channels()) == [{"id": "C1"}, {"id": "C2"}]

    stream.config = {"channels": ["C10", "C20"]}
    client.get_channel.side_effect = [{"id": "C10"}, {"id": "C20"}]
    assert list(stream.channels()) == [{"id": "C10"}, {"id": "C20"}]


def test_conversations_and_members_sync_write_records(patched_stream_env):
    client = make_client()

    conv = ConversationsStream(client=client, config={}, state={})
    conv.load_schema = MagicMock(return_value={"type": "object"})
    client.get_all_channels.return_value = [{"channels": [{"id": "C1"}, {"id": "C2"}]}]
    conv.sync(mdata=[])
    assert client.get_all_channels.called

    members = ConversationMembersStream(client=client, config={}, state={})
    members.load_schema = MagicMock(return_value={"type": "object"})
    client.get_all_channels.return_value = [{"channels": [{"id": "C1"}]}]
    client.get_channel_members.return_value = [{"members": ["U1", "U2"]}]
    members.sync(mdata=[])
    assert client.get_channel_members.called


def test_users_sync_incremental_bookmark_paths(patched_stream_env):
    client = make_client()
    stream = UsersStream(
        client=client,
        config={"start_date": "2025-01-01T00:00:00Z"},
        state={"bookmarks": {}},
    )
    stream.load_schema = MagicMock(return_value={"type": "object"})

    client.get_users.return_value = [
        {
            "members": [
                {"id": "U1", "updated": "2025-01-01T00:00:00"},
                {"id": "U2", "updated": "2025-01-03T00:00:00"},
            ]
        }
    ]

    stream.sync(mdata=[])
    assert client.get_users.called

    with patch("tap_slack.streams.singer.get_bookmark", return_value="2025-01-04T00:00:00"):
        stream.sync(mdata=[])


def test_threads_sync_records(patched_stream_env):
    client = make_client()
    stream = ThreadsStream(
        client=client,
        config={"start_date": "2025-01-01T00:00:00Z"},
        state={"bookmarks": {}},
    )
    stream.load_schema = MagicMock(return_value={"type": "object"})

    with patch.object(
        stream,
        "get_absolute_date_range",
        return_value=(
            datetime(2025, 1, 1, tzinfo=timezone.utc),
            datetime(2025, 1, 2, tzinfo=timezone.utc),
        ),
    ):
        client.get_thread.return_value = [
            {
                "messages": [
                    {"ts": "1735689600.001", "last_read": "1735689600.001", "text": "r1"}
                ]
            }
        ]
        stream.sync(mdata=[], channel_id="C1", ts="1735689600.001")


def test_usergroups_and_teams_sync(patched_stream_env):
    client = make_client()

    ug = UserGroupsStream(client=client, config={}, state={})
    ug.load_schema = MagicMock(return_value={"type": "object"})
    client.get_user_groups.return_value = [{"usergroups": [{"id": "G1"}, {"id": "G2"}]}]
    ug.sync(mdata=[])

    teams = TeamsStream(client=client, config={}, state={})
    teams.load_schema = MagicMock(return_value={"type": "object"})
    client.get_teams.return_value = [{"team": {"id": "T1"}}]
    teams.sync(mdata=[])


def test_files_and_remote_files_sync_and_bookmarks(patched_stream_env):
    client = make_client()

    files = FilesStream(
        client=client,
        config={"start_date": "2025-01-01T00:00:00Z", "date_window_size": "1"},
        state={"bookmarks": {}},
    )
    files.load_schema = MagicMock(return_value={"type": "object"})

    remote = RemoteFilesStream(
        client=client,
        config={"start_date": "2025-01-01T00:00:00Z", "date_window_size": "1"},
        state={"bookmarks": {}},
    )
    remote.load_schema = MagicMock(return_value={"type": "object"})

    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = datetime(2025, 1, 2, tzinfo=timezone.utc)

    client.get_files.return_value = [{"files": [{"id": "F1", "timestamp": int(start.timestamp()) + 10}]}]
    client.get_remote_files.return_value = [
        {"files": [{"id": "RF1", "timestamp": int(start.timestamp()) + 20}]}
    ]

    with patch.object(files, "get_absolute_date_range", return_value=(start, end)):
        files.sync(mdata=[])

    with patch.object(remote, "get_absolute_date_range", return_value=(start, end)):
        remote.sync(mdata=[])


def test_messages_sync_updates_channel_bookmarks(patched_stream_env):
    client = make_client()
    state = {"bookmarks": {}}

    class DummyEntry:
        def __init__(self, stream):
            self.stream = stream
            self.metadata = []

    class DummyCatalog:
        def get_selected_streams(self, state):
            return [DummyEntry("messages")]

    stream = ConversationHistoryStream(
        client=client,
        config={"start_date": "2025-01-01T00:00:00Z", "date_window_size": "1"},
        catalog=DummyCatalog(),
        state=state,
    )
    stream.load_schema = MagicMock(return_value={"type": "object"})

    client.get_all_channels.return_value = [{"channels": [{"id": "C1"}]}]
    client.get_messages.return_value = [
        {
            "messages": [
                {"ts": "1735689600.100", "text": "m1", "thread_ts": "1735689600.100"},
                {"ts": "1735689600.200", "text": "m2", "thread_ts": "1735689600.200"},
            ]
        }
    ]

    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = datetime(2025, 1, 2, tzinfo=timezone.utc)

    with patch.object(stream, "get_absolute_date_range", return_value=(start, end)):
        stream.sync(mdata=[])

    assert "messages" in stream.state["bookmarks"]
    assert "C1" in stream.state["bookmarks"]["messages"]


def test_messages_sync_handles_none_messages_page(patched_stream_env):
    client = make_client()
    state = {"bookmarks": {}}

    class DummyEntry:
        def __init__(self, stream):
            self.stream = stream
            self.metadata = []

    class DummyCatalog:
        def get_selected_streams(self, state):
            return [DummyEntry("messages")]

    stream = ConversationHistoryStream(
        client=client,
        config={"start_date": "2025-01-01T00:00:00Z", "date_window_size": "1"},
        catalog=DummyCatalog(),
        state=state,
    )
    stream.load_schema = MagicMock(return_value={"type": "object"})

    client.get_all_channels.return_value = [{"channels": [{"id": "C1"}]}]
    client.get_messages.return_value = None

    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = datetime(2025, 1, 2, tzinfo=timezone.utc)

    with patch.object(stream, "get_absolute_date_range", return_value=(start, end)):
        stream.sync(mdata=[])
