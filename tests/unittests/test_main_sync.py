import runpy
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from tap_slack import auto_join, main, sync


def test_auto_join_with_explicit_channel_list_success():
    client = MagicMock()
    client.join_channel.return_value = {"ok": True}
    auto_join(client, {"channels": ["C1", "C2"]})
    assert client.join_channel.call_count == 2


def test_auto_join_with_explicit_channel_list_failure_raises():
    client = MagicMock()
    client.join_channel.return_value = {"ok": False, "error": "not_in_channel"}
    with pytest.raises(Exception):
        auto_join(client, {"channels": ["C1"]})


def test_auto_join_without_channel_list_uses_public_channels():
    client = MagicMock()
    client.get_all_channels.return_value = {
        "channels": [{"id": "C1", "name": "general"}, {"id": "C2", "name": "random"}]
    }
    client.join_channel.return_value = {"ok": True}

    auto_join(client, {})

    assert client.get_all_channels.called
    assert client.join_channel.call_count == 2


def test_auto_join_without_channel_list_failure_raises_with_name():
    client = MagicMock()
    client.get_all_channels.return_value = {
        "channels": [{"id": "C1", "name": "general"}]
    }
    client.join_channel.return_value = {"ok": False, "error": "channel_not_found"}

    with pytest.raises(Exception):
        auto_join(client, {})


def test_sync_includes_messages_when_threads_selected():
    class FakeStream:
        def __init__(self, client, config=None, catalog=None, state=None, write_to_singer=True):
            self.write_to_singer = write_to_singer

        def write_schema(self):
            pass

        def sync(self, mdata):
            pass

        def write_state(self):
            pass

    class Entry:
        def __init__(self, stream):
            self.stream = stream
            self.metadata = []

    class FakeCatalog:
        def get_selected_streams(self, state):
            return [Entry("threads")]

        def get_stream(self, name):
            return Entry(name)

    created = []

    class CapturingFake(FakeStream):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            created.append(self)

    with patch.dict(
        "tap_slack.AVAILABLE_STREAMS",
        {
            "messages": CapturingFake,
            "threads": CapturingFake,
        },
        clear=False,
    ):
        sync(client=MagicMock(), config={}, catalog=FakeCatalog(), state={})

    assert len(created) == 1
    assert created[0].write_to_singer is False


def test_sync_uses_write_to_singer_true_when_messages_selected():
    class FakeStream:
        def __init__(self, client, config=None, catalog=None, state=None, write_to_singer=True):
            self.write_to_singer = write_to_singer

        def write_schema(self):
            pass

        def sync(self, mdata):
            pass

        def write_state(self):
            pass

    class Entry:
        def __init__(self, stream):
            self.stream = stream
            self.metadata = []

    class FakeCatalog:
        def get_selected_streams(self, state):
            return [Entry("messages")]

        def get_stream(self, name):
            return Entry(name)

    created = []

    class CapturingFake(FakeStream):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            created.append(self)

    with patch.dict("tap_slack.AVAILABLE_STREAMS", {"messages": CapturingFake}, clear=False):
        sync(client=MagicMock(), config={}, catalog=FakeCatalog(), state={})

    assert len(created) == 1
    assert created[0].write_to_singer is True


def test_sync_sets_false_when_no_messages_or_threads_selected():
    class FakeStream:
        def __init__(self, client, config=None, catalog=None, state=None, write_to_singer=True):
            self.write_to_singer = write_to_singer

        def write_schema(self):
            pass

        def sync(self, mdata):
            pass

        def write_state(self):
            pass

    class Entry:
        def __init__(self, stream):
            self.stream = stream
            self.metadata = []

    class FakeCatalog:
        def get_selected_streams(self, state):
            return [Entry("channels")]

        def get_stream(self, name):
            return Entry(name)

    created = []

    class CapturingFake(FakeStream):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            created.append(self)

    with patch.dict("tap_slack.AVAILABLE_STREAMS", {"channels": CapturingFake}, clear=False):
        sync(client=MagicMock(), config={}, catalog=FakeCatalog(), state={})

    assert len(created) == 1
    assert created[0].write_to_singer is True


def test_main_discover_and_sync_paths():
    parse_discover = SimpleNamespace(discover=True, config={"token": "x"}, catalog=None, state={})
    parse_sync = SimpleNamespace(
        discover=False,
        config={"token": "x", "join_public_channels": "true"},
        catalog=object(),
        state={},
    )

    with patch("tap_slack.singer.utils.parse_args", return_value=parse_discover), patch(
        "tap_slack.WebClient"
    ) as webclient_cls, patch("tap_slack.SlackClient") as slack_client_cls, patch(
        "tap_slack.discover"
    ) as discover_mock:
        main()
        webclient_cls.assert_called_once_with(token="x")
        slack_client_cls.assert_called_once()
        discover_mock.assert_called_once()

    with patch("tap_slack.singer.utils.parse_args", return_value=parse_sync), patch(
        "tap_slack.WebClient"
    ), patch("tap_slack.SlackClient"), patch("tap_slack.sync") as sync_mock, patch(
        "tap_slack.auto_join"
    ) as auto_join_mock:
        main()
        auto_join_mock.assert_called_once()
        sync_mock.assert_called_once()


def test_module_main_guard_executes_main():
    parse_args = SimpleNamespace(discover=False, config={"token": "x"}, catalog=None, state={})

    with patch("singer.utils.parse_args", return_value=parse_args), patch(
        "slack_sdk.WebClient"
    ), patch("tap_slack.client.SlackClient"):
        runpy.run_module("tap_slack.__init__", run_name="__main__")
