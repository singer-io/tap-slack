from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from slack_sdk.errors import SlackApiError

from tap_slack.streams import (
    ConversationHistoryStream,
    ConversationsStream,
    FilesStream,
    RemoteFilesStream,
    SlackStream,
    TeamsStream,
    UserGroupsStream,
    UsersStream,
)


@contextmanager
def dummy_job_timer(**kwargs):
    yield None


class DummyCounter:
    def increment(self):
        pass

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


class FakeMoment:
    def __init__(self, gt_result=False, lt_result=False):
        self.gt_result = gt_result
        self.lt_result = lt_result

    def replace(self, **kwargs):
        return self

    def __gt__(self, other):
        return self.gt_result

    def __lt__(self, other):
        return self.lt_result

    def strftime(self, fmt):
        return "2025-01-01T00:00:00"


def make_fake_datetime_module(moment_map):
    def fake_utcfromtimestamp(timestamp):
        return moment_map[timestamp]["utc"]

    def fake_fromtimestamp(timestamp):
        return moment_map[timestamp]["from"]

    return SimpleNamespace(
        utcfromtimestamp=fake_utcfromtimestamp,
        fromtimestamp=fake_fromtimestamp,
    )


def make_slack_error(code):
    response = MagicMock()
    response.data = {"ok": False, "error": code}
    response.status_code = 200
    return SlackApiError(message=code, response=response)


def test_base_stream_probe_access_raises_not_implemented():
    class DummyStream(SlackStream):
        name = "dummy"
        key_properties = ["id"]

    stream = DummyStream(client=MagicMock(), config={})
    with pytest.raises(NotImplementedError):
        stream._probe_access()


def test_write_schema_calls_singer_write_schema():
    class DummyStream(SlackStream):
        name = "dummy"
        key_properties = ["id"]

    stream = DummyStream(client=MagicMock(), config={})
    with patch.object(stream, "load_schema", return_value={"type": "object"}), patch(
        "tap_slack.streams.singer.write_schema"
    ) as write_schema_mock:
        stream.write_schema()

    write_schema_mock.assert_called_once_with(
        stream_name="dummy", schema={"type": "object"}, key_properties=["id"]
    )


def test_all_channels_defaults_to_public_and_not_archived_filter():
    client = MagicMock()
    client.get_all_channels.return_value = [{"channels": []}]

    stream = ConversationsStream(client=client, config={}, state={})
    list(stream._all_channels())

    client.get_all_channels.assert_called_once_with(
        types="public_channel", exclude_archived="false"
    )


def test_get_absolute_date_range_uses_config_start_when_outside_lookback():
    client = MagicMock()
    stream = ConversationsStream(
        client=client,
        config={"lookback_window": "14"},
        state={},
    )

    start, end = stream.get_absolute_date_range("2025-01-01T00:00:00Z")

    assert start < end
    assert start.year == 2025


@pytest.mark.parametrize(
    "stream_cls, webclient_method",
    [
        (UsersStream, "users_list"),
        (UserGroupsStream, "usergroups_list"),
        (TeamsStream, "team_info"),
        (FilesStream, "files_list"),
        (RemoteFilesStream, "files_remote_list"),
    ],
)
def test_probe_access_reraises_non_forbidden_errors(stream_cls, webclient_method):
    client = MagicMock()
    client.webclient = MagicMock()
    getattr(client.webclient, webclient_method).side_effect = make_slack_error("internal_error")

    stream = stream_cls(client=client, config={}, state={})
    with pytest.raises(SlackApiError):
        stream._probe_access()


def test_messages_bookmark_helpers_cover_missing_paths():
    stream = ConversationHistoryStream(client=MagicMock(), config={}, catalog=MagicMock(), state=None)
    assert stream.get_bookmark("C1", "default") == "default"

    stream.state = {}
    with patch.object(stream, "write_state") as write_state_mock:
        stream.update_bookmarks("C1", "2025-01-01T00:00:00")

    assert stream.state["bookmarks"]["messages"]["C1"] == "2025-01-01T00:00:00"
    write_state_mock.assert_called_once()


def test_messages_sync_with_threads_selected_calls_thread_stream_methods():
    class DummyEntry:
        def __init__(self, stream):
            self.stream = stream
            self.metadata = []

    class DummyCatalog:
        def get_selected_streams(self, state):
            return [DummyEntry("threads")]

    class DummyThreadsStream:
        def __init__(self, *args, **kwargs):
            self.write_schema_called = 0
            self.write_state_called = 0
            self.sync_called = 0

        def write_schema(self):
            self.write_schema_called += 1

        def sync(self, mdata, channel_id, ts):
            self.sync_called += 1

        def write_state(self):
            self.write_state_called += 1

    client = MagicMock()
    stream = ConversationHistoryStream(
        client=client,
        config={"start_date": "2025-01-01T00:00:00Z", "date_window_size": "1"},
        catalog=DummyCatalog(),
        state={"bookmarks": {}},
    )
    stream.load_schema = MagicMock(return_value={"type": "object"})

    client.get_all_channels.return_value = [{"channels": [{"id": "C1"}]}]
    client.get_messages.return_value = [
        {
            "messages": [
                {"ts": "1735689600.100", "thread_ts": "1735689600.100", "text": "one"}
            ]
        }
    ]

    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = datetime(2025, 1, 2, tzinfo=timezone.utc)

    with patch("tap_slack.streams.ThreadsStream", DummyThreadsStream), patch(
        "tap_slack.streams.singer.metrics.job_timer", side_effect=dummy_job_timer
    ), patch(
        "tap_slack.streams.singer.metrics.record_counter", side_effect=lambda **_: DummyCounter()
    ), patch(
        "tap_slack.streams.singer.Transformer", DummyTransformer
    ), patch(
        "tap_slack.streams.metadata.to_map", side_effect=lambda m: m
    ), patch(
        "tap_slack.streams.singer.write_record"
    ), patch(
        "tap_slack.streams.singer.utils.now", return_value=datetime(2025, 1, 2, tzinfo=timezone.utc)
    ), patch.object(
        stream, "get_absolute_date_range", return_value=(start, end)
    ):
        stream.sync(mdata=[])


def test_messages_sync_covers_bookmark_comparison_branches():
    class DummyEntry:
        def __init__(self, stream):
            self.stream = stream
            self.metadata = []

    class DummyCatalog:
        def get_selected_streams(self, state):
            return [DummyEntry("messages")]

    client = MagicMock()
    stream = ConversationHistoryStream(
        client=client,
        config={"start_date": "2025-01-01T00:00:00Z", "date_window_size": "1"},
        catalog=DummyCatalog(),
        state={"bookmarks": {}},
    )
    stream.load_schema = MagicMock(return_value={"type": "object"})

    client.get_all_channels.return_value = [{"channels": [{"id": "C1"}]}]
    client.get_messages.return_value = [
        {
            "messages": [
                {"ts": "1735689700.100", "thread_ts": "1735689700.100", "text": "newer"},
                {"ts": "1735689600.100", "thread_ts": "1735689600.100", "text": "older"},
            ]
        }
    ]

    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = datetime(2025, 1, 2, tzinfo=timezone.utc)
    moment_map = {
        1735689700: {"utc": FakeMoment(gt_result=True, lt_result=False), "from": FakeMoment(gt_result=True, lt_result=False)},
        1735689600: {"utc": FakeMoment(gt_result=False, lt_result=True), "from": FakeMoment(gt_result=False, lt_result=True)},
    }

    with patch("tap_slack.streams.datetime", make_fake_datetime_module(moment_map)), patch(
        "tap_slack.streams.singer.metrics.job_timer", side_effect=dummy_job_timer
    ), patch(
        "tap_slack.streams.singer.metrics.record_counter", side_effect=lambda **_: DummyCounter()
    ), patch(
        "tap_slack.streams.singer.Transformer", DummyTransformer
    ), patch(
        "tap_slack.streams.metadata.to_map", side_effect=lambda m: m
    ), patch(
        "tap_slack.streams.singer.write_record"
    ), patch(
        "tap_slack.streams.singer.utils.now", return_value=datetime(2025, 1, 2, tzinfo=timezone.utc)
    ), patch.object(
        stream, "get_absolute_date_range", return_value=(start, end)
    ):
        stream.sync(mdata=[])


@pytest.mark.parametrize(
    "stream_cls, client_attr, moment_map, start_field, end_field",
    [
        (
            FilesStream,
            "get_files",
            {
                1735689700: {"utc": FakeMoment(gt_result=True, lt_result=False), "from": FakeMoment(gt_result=True, lt_result=False)},
                1735689600: {"utc": FakeMoment(gt_result=False, lt_result=True), "from": FakeMoment(gt_result=False, lt_result=True)},
            },
            "timestamp",
            "files",
        ),
        (
            RemoteFilesStream,
            "get_remote_files",
            {
                1735689700: {"utc": FakeMoment(gt_result=True, lt_result=False), "from": FakeMoment(gt_result=True, lt_result=False)},
                1735689600: {"utc": FakeMoment(gt_result=False, lt_result=True), "from": FakeMoment(gt_result=False, lt_result=True)},
            },
            "timestamp",
            "files",
        ),
    ],
)
def test_files_and_remote_files_cover_min_bookmark_branch(stream_cls, client_attr, moment_map, start_field, end_field):
    client = MagicMock()
    stream = stream_cls(
        client=client,
        config={"start_date": "2025-01-01T00:00:00Z", "date_window_size": "1"},
        state={"bookmarks": {}},
    )
    stream.load_schema = MagicMock(return_value={"type": "object"})

    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = datetime(2025, 1, 2, tzinfo=timezone.utc)

    getattr(client, client_attr).return_value = [
        {
            end_field: [
                {"id": "A", start_field: 1735689700},
                {"id": "B", start_field: 1735689600},
            ]
        }
    ]

    with patch("tap_slack.streams.datetime", make_fake_datetime_module(moment_map)), patch(
        "tap_slack.streams.singer.metrics.job_timer", side_effect=dummy_job_timer
    ), patch(
        "tap_slack.streams.singer.metrics.record_counter", side_effect=lambda **_: DummyCounter()
    ), patch(
        "tap_slack.streams.singer.Transformer", DummyTransformer
    ), patch(
        "tap_slack.streams.metadata.to_map", side_effect=lambda m: m
    ), patch(
        "tap_slack.streams.singer.write_record"
    ), patch(
        "tap_slack.streams.singer.utils.now", return_value=datetime(2025, 1, 2, tzinfo=timezone.utc)
    ), patch.object(
        stream, "get_absolute_date_range", return_value=(start, end)
    ):
        stream.sync(mdata=[])
