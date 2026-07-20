"""
Unit tests for stream exclusion during discovery.
Tests that unauthorized streams (403 Forbidden) are excluded from the catalog.
"""

import unittest
from unittest.mock import MagicMock, patch, PropertyMock

from slack_sdk.errors import SlackApiError

from tap_slack import _apply_access_checks, _prune_inaccessible_children, discover
from tap_slack.exceptions import SlackForbiddenError
from tap_slack.streams import (
    AVAILABLE_STREAMS,
    ConversationsStream,
    ConversationMembersStream,
    ConversationHistoryStream,
    FilesStream,
    RemoteFilesStream,
    TeamsStream,
    ThreadsStream,
    UserGroupsStream,
    UsersStream,
)


def _mock_client():
    """Create a mock SlackClient."""
    client = MagicMock()
    client.config = {}
    client.webclient = MagicMock()
    return client


def _make_slack_api_error(error_code):
    """Create a mock SlackApiError with the given error code."""
    response = MagicMock()
    response.data = {"ok": False, "error": error_code}
    response.status_code = 200
    err = SlackApiError(message=f"The request to the Slack API failed. (error: {error_code})",
                        response=response)
    return err


class TestCheckAccess(unittest.TestCase):
    """Test the check_access() method on individual streams."""

    def test_child_stream_always_returns_true(self):
        """Child streams should always return True regardless of permissions."""
        client = _mock_client()
        child_streams = [ConversationMembersStream, ConversationHistoryStream, ThreadsStream]
        for stream_cls in child_streams:
            stream = stream_cls(client)
            self.assertTrue(stream.check_access(),
                            f"{stream.name} (child) should always return True")

    def test_parent_stream_accessible(self):
        """Parent stream should return True when API call succeeds."""
        client = _mock_client()
        stream = ConversationsStream(client)
        # Mock webclient method to succeed
        client.webclient.conversations_list.return_value = {"ok": True, "channels": []}
        self.assertTrue(stream.check_access())

    def test_parent_stream_forbidden_missing_scope(self):
        """Parent stream should return False when API returns missing_scope."""
        client = _mock_client()
        stream = UsersStream(client)
        client.webclient.users_list.side_effect = _make_slack_api_error("missing_scope")
        self.assertFalse(stream.check_access())

    def test_parent_stream_forbidden_logs_warning(self):
        """check_access() should log a warning with the stream name and error when forbidden."""
        client = _mock_client()
        stream = UsersStream(client)
        client.webclient.users_list.side_effect = _make_slack_api_error("missing_scope")

        with patch('tap_slack.streams.LOGGER') as mock_logger:
            result = stream.check_access()

        self.assertFalse(result)
        mock_logger.warning.assert_called_once_with(
            "Unauthorized Stream: %s, excluding from catalog. Error: '%s'",
            "users",
            mock_logger.warning.call_args[0][2],  # the SlackForbiddenError instance
        )
        # Verify the error message content
        logged_exc = mock_logger.warning.call_args[0][2]
        self.assertIn("missing_scope", str(logged_exc))

    def test_parent_stream_forbidden_not_allowed_token_type(self):
        """Parent stream should return False when API returns not_allowed_token_type."""
        client = _mock_client()
        stream = TeamsStream(client)
        client.webclient.team_info.side_effect = _make_slack_api_error("not_allowed_token_type")
        self.assertFalse(stream.check_access())

    def test_parent_stream_forbidden_access_denied(self):
        """Parent stream should return False when API returns access_denied."""
        client = _mock_client()
        stream = FilesStream(client)
        client.webclient.files_list.side_effect = _make_slack_api_error("access_denied")
        self.assertFalse(stream.check_access())

    def test_parent_stream_other_error_raises(self):
        """Parent stream should raise non-permission SlackApiError."""
        client = _mock_client()
        stream = ConversationsStream(client)
        client.webclient.conversations_list.side_effect = _make_slack_api_error("internal_error")
        with self.assertRaises(SlackApiError):
            stream.check_access()


class TestApplyAccessChecks(unittest.TestCase):
    """Test the _apply_access_checks() function."""

    def test_all_streams_accessible(self):
        """When all streams are accessible, none are removed."""
        client = _mock_client()
        # All probe calls succeed
        client.webclient.conversations_list.return_value = {"ok": True}
        client.webclient.users_list.return_value = {"ok": True}
        client.webclient.usergroups_list.return_value = {"ok": True}
        client.webclient.team_info.return_value = {"ok": True}
        client.webclient.files_list.return_value = {"ok": True}
        client.webclient.files_remote_list.return_value = {"ok": True}

        streams = [stream_cls(client) for _, stream_cls in AVAILABLE_STREAMS.items()]
        original_count = len(streams)
        _apply_access_checks(client, streams)
        self.assertEqual(len(streams), original_count)

    def test_partial_access_excludes_forbidden_streams(self):
        """When some streams are forbidden, they are excluded."""
        client = _mock_client()
        # conversations succeeds, users fails
        client.webclient.conversations_list.return_value = {"ok": True}
        client.webclient.users_list.side_effect = _make_slack_api_error("missing_scope")
        client.webclient.usergroups_list.return_value = {"ok": True}
        client.webclient.team_info.return_value = {"ok": True}
        client.webclient.files_list.return_value = {"ok": True}
        client.webclient.files_remote_list.return_value = {"ok": True}

        streams = [stream_cls(client) for _, stream_cls in AVAILABLE_STREAMS.items()]
        _apply_access_checks(client, streams)

        stream_names = [s.name for s in streams]
        self.assertNotIn('users', stream_names)
        # Other streams should still be present
        self.assertIn('channels', stream_names)
        self.assertIn('teams', stream_names)

    def test_child_excluded_when_parent_forbidden(self):
        """When parent stream (channels) is forbidden, its children are excluded."""
        client = _mock_client()
        # channels fails, others succeed
        client.webclient.conversations_list.side_effect = _make_slack_api_error("missing_scope")
        client.webclient.users_list.return_value = {"ok": True}
        client.webclient.usergroups_list.return_value = {"ok": True}
        client.webclient.team_info.return_value = {"ok": True}
        client.webclient.files_list.return_value = {"ok": True}
        client.webclient.files_remote_list.return_value = {"ok": True}

        streams = [stream_cls(client) for _, stream_cls in AVAILABLE_STREAMS.items()]
        _apply_access_checks(client, streams)

        stream_names = [s.name for s in streams]
        # Parent excluded
        self.assertNotIn('channels', stream_names)
        # Children excluded
        self.assertNotIn('channel_members', stream_names)
        self.assertNotIn('messages', stream_names)
        self.assertNotIn('threads', stream_names)
        # Non-children still present
        self.assertIn('users', stream_names)
        self.assertIn('teams', stream_names)

    def test_all_forbidden_raises_error(self):
        """When all parent streams are forbidden, raise SlackForbiddenError."""
        client = _mock_client()
        # All probe calls fail
        client.webclient.conversations_list.side_effect = _make_slack_api_error("missing_scope")
        client.webclient.users_list.side_effect = _make_slack_api_error("missing_scope")
        client.webclient.usergroups_list.side_effect = _make_slack_api_error("missing_scope")
        client.webclient.team_info.side_effect = _make_slack_api_error("missing_scope")
        client.webclient.files_list.side_effect = _make_slack_api_error("missing_scope")
        client.webclient.files_remote_list.side_effect = _make_slack_api_error("missing_scope")

        streams = [stream_cls(client) for _, stream_cls in AVAILABLE_STREAMS.items()]
        with self.assertRaises(SlackForbiddenError) as ctx:
            _apply_access_checks(client, streams)
        self.assertIn("No streams are accessible", str(ctx.exception))
        self.assertIn("read permission for at least one stream", str(ctx.exception))

    def test_all_forbidden_raises_error_message(self):
        """SlackForbiddenError message should contain the expected text."""
        client = _mock_client()
        client.webclient.conversations_list.side_effect = _make_slack_api_error("missing_scope")
        client.webclient.users_list.side_effect = _make_slack_api_error("missing_scope")
        client.webclient.usergroups_list.side_effect = _make_slack_api_error("missing_scope")
        client.webclient.team_info.side_effect = _make_slack_api_error("missing_scope")
        client.webclient.files_list.side_effect = _make_slack_api_error("missing_scope")
        client.webclient.files_remote_list.side_effect = _make_slack_api_error("missing_scope")

        streams = [stream_cls(client) for _, stream_cls in AVAILABLE_STREAMS.items()]
        with self.assertRaises(SlackForbiddenError) as ctx:
            _apply_access_checks(client, streams)
        self.assertEqual(
            str(ctx.exception),
            "No streams are accessible. Ensure the credentials have read permission for at least one stream."
        )

    @patch('tap_slack.LOGGER')
    def test_partial_access_logs_excluded_streams_warning(self, mock_logger):
        """_apply_access_checks should log a warning listing excluded stream names."""
        client = _mock_client()
        client.webclient.conversations_list.return_value = {"ok": True}
        client.webclient.users_list.side_effect = _make_slack_api_error("missing_scope")
        client.webclient.usergroups_list.side_effect = _make_slack_api_error("missing_scope")
        client.webclient.team_info.return_value = {"ok": True}
        client.webclient.files_list.return_value = {"ok": True}
        client.webclient.files_remote_list.return_value = {"ok": True}

        streams = [stream_cls(client) for _, stream_cls in AVAILABLE_STREAMS.items()]
        _apply_access_checks(client, streams)

        # Find the specific warning call about excluded streams
        exclusion_calls = [
            call for call in mock_logger.warning.call_args_list
            if "Unauthorized streams excluded from catalog" in str(call)
        ]
        self.assertEqual(len(exclusion_calls), 1)
        warning_msg = exclusion_calls[0][0][0]
        excluded_names = exclusion_calls[0][0][1]
        self.assertEqual(
            warning_msg,
            "Unauthorized streams excluded from catalog: %s"
        )
        self.assertIn("users", excluded_names)
        self.assertIn("user_groups", excluded_names)

    @patch('tap_slack.LOGGER')
    def test_parent_forbidden_warning_includes_pruned_children(self, mock_logger):
        """When a parent stream is inaccessible, warning should include pruned child stream names."""
        client = _mock_client()
        client.webclient.conversations_list.side_effect = _make_slack_api_error("missing_scope")
        client.webclient.users_list.return_value = {"ok": True}
        client.webclient.usergroups_list.return_value = {"ok": True}
        client.webclient.team_info.return_value = {"ok": True}
        client.webclient.files_list.return_value = {"ok": True}
        client.webclient.files_remote_list.return_value = {"ok": True}

        streams = [stream_cls(client) for _, stream_cls in AVAILABLE_STREAMS.items()]
        _apply_access_checks(client, streams)

        exclusion_calls = [
            call for call in mock_logger.warning.call_args_list
            if "Unauthorized streams excluded from catalog" in str(call)
        ]
        self.assertEqual(len(exclusion_calls), 1)
        excluded_names = exclusion_calls[0][0][1]
        self.assertIn("channels", excluded_names)
        self.assertIn("channel_members", excluded_names)
        self.assertIn("messages", excluded_names)
        self.assertIn("threads", excluded_names)


class TestPruneInaccessibleChildren(unittest.TestCase):
    """Test the _prune_inaccessible_children() function."""

    def test_children_removed_when_parent_missing(self):
        """Child streams are removed when their parent is not in the list."""
        client = _mock_client()
        # Only include users (no channels parent)
        streams = [
            UsersStream(client),
            ConversationMembersStream(client),  # parent='channels'
            ThreadsStream(client),  # parent='channels'
        ]
        _prune_inaccessible_children(streams)
        stream_names = [s.name for s in streams]
        self.assertIn('users', stream_names)
        self.assertNotIn('channel_members', stream_names)
        self.assertNotIn('threads', stream_names)

    def test_children_kept_when_parent_present(self):
        """Child streams are kept when their parent is in the list."""
        client = _mock_client()
        streams = [
            ConversationsStream(client),
            ConversationMembersStream(client),
            ConversationHistoryStream(client),
            ThreadsStream(client),
        ]
        _prune_inaccessible_children(streams)
        stream_names = [s.name for s in streams]
        self.assertEqual(len(stream_names), 4)


class TestDiscoverWithAccessChecks(unittest.TestCase):
    """Test the full discover() function with access checking."""

    @patch('tap_slack.json.dump')
    def test_discover_excludes_forbidden_streams(self, mock_json_dump):
        """discover() should produce a catalog without forbidden streams."""
        client = _mock_client()
        # user_groups forbidden
        client.webclient.conversations_list.return_value = {"ok": True}
        client.webclient.users_list.return_value = {"ok": True}
        client.webclient.usergroups_list.side_effect = _make_slack_api_error("missing_scope")
        client.webclient.team_info.return_value = {"ok": True}
        client.webclient.files_list.return_value = {"ok": True}
        client.webclient.files_remote_list.return_value = {"ok": True}

        discover(client)

        # Verify json.dump was called with the catalog
        self.assertTrue(mock_json_dump.called)
        catalog = mock_json_dump.call_args[0][0]
        stream_names = [s['stream'] for s in catalog['streams']]
        self.assertNotIn('user_groups', stream_names)
        self.assertIn('channels', stream_names)
        self.assertIn('users', stream_names)


if __name__ == '__main__':
    unittest.main()
