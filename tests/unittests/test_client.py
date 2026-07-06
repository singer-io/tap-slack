from unittest.mock import MagicMock, patch

import pytest
from slack_sdk.errors import SlackApiError

from tap_slack.client import SlackClient


def make_slack_error(code, retry_after="0"):
    response = MagicMock()
    response.data = {"ok": False, "error": code}
    response.headers = {"Retry-After": retry_after}
    response.status_code = 200
    return SlackApiError(message=code, response=response)


def test_client_wait_rate_limited_sleeps():
    err = make_slack_error("ratelimited", retry_after="2")
    with patch("tap_slack.client.time.sleep") as sleep_mock:
        SlackClient.wait(err)
    sleep_mock.assert_called_once_with(2)


def test_client_wait_non_ratelimited_raises():
    err = make_slack_error("invalid_auth")
    with pytest.raises(SlackApiError):
        SlackClient.wait(err)


def test_client_wait_with_non_slack_error_noop():
    SlackClient.wait(ValueError("x"))


def test_client_channel_and_collection_calls_passthrough():
    webclient = MagicMock()
    webclient.conversations_info.return_value = {"channel": {"id": "C1"}}
    webclient.conversations_list.return_value = [{"channels": [{"id": "C1"}]}]
    webclient.users_list.return_value = [{"members": []}]
    webclient.usergroups_list.return_value = [{"usergroups": []}]
    webclient.team_info.return_value = [{"team": {"id": "T1"}}]
    webclient.files_list.return_value = [{"files": []}]
    webclient.files_remote_list.return_value = [{"files": []}]
    webclient.conversations_replies.return_value = [{"messages": []}]
    webclient.conversations_join.return_value = {"ok": True}

    client = SlackClient(webclient, config={})

    assert list(client.get_channel(include_num_members=1, channel="C1")) == [{"id": "C1"}]
    assert client.get_all_channels(types="public_channel", exclude_archived="true") == [{"channels": [{"id": "C1"}]}]
    assert client.get_users(limit=100) == [{"members": []}]
    assert client.get_user_groups("true", "true", "true") == [{"usergroups": []}]
    assert client.get_teams() == [{"team": {"id": "T1"}}]
    assert client.get_files(1, 2) == [{"files": []}]
    assert client.get_remote_files(1, 2) == [{"files": []}]
    assert client.get_thread("C1", "1", "true", 1, 2) == [{"messages": []}]
    assert client.join_channel("C1") == {"ok": True}


def test_client_get_channel_members_error_paths():
    webclient = MagicMock()
    client = SlackClient(webclient, config={})

    webclient.conversations_members.side_effect = make_slack_error("fetch_members_failed")
    with patch("tap_slack.client.LOGGER") as logger_mock:
        assert client.get_channel_members("C1") == []
    logger_mock.warning.assert_called_once()

    webclient.conversations_members.side_effect = make_slack_error("invalid_auth")
    with pytest.raises(SlackApiError):
        client.get_channel_members("C2")


def test_client_get_messages_error_paths():
    webclient = MagicMock()
    client = SlackClient(webclient, config={})

    webclient.conversations_history.side_effect = make_slack_error("not_in_channel")
    with patch("tap_slack.client.LOGGER") as logger_mock:
        assert client.get_messages("C1", 1, 2) is None
    logger_mock.warning.assert_called_once()

    webclient.conversations_history.side_effect = make_slack_error("invalid_auth")
    with pytest.raises(SlackApiError):
        client.get_messages("C2", 1, 2)
