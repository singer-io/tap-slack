def config():
    return {
        "test_name": "test_sync",
        "tap_name": "tap-slack",
        "type": "platform.slack",
        "properties": {
            "start_date": "TAP_SLACK_START_DATE",
            "lookback_window": "TAP_SLACK_LOOKBACK_WINDOW",
            "join_public_channels": "TAP_SLACK_JOIN_PUBLIC_CHANNELS",
            "private_channels": "TAP_SLACK_PRIVATE_CHANNELS",
            "exclude_archived": "TAP_SLACK_EXCLUDE_ARCHIVED",
        },
        "credentials": {
            "token": "TAP_SLACK_AUTH_TOKEN"
        },
        "bookmark": {
            "bookmark_key": "users",
            "bookmark_timestamp": "2020-06-00T14:30:31+0000"
        },
        "streams": {
            "channels": {"id"},
            "channel_members": {"channel_id", "user_id"},
            "messages": {"channel_id", "ts"},
            "users": {"id"},
            "threads": {"channel_id", "ts", "thread_ts"},
            "user_groups": {"id"},
            "teams": {"id"},
            "files": {"id"},
            "remote_files": {"id"}
        },
        "exclude_streams": []
    }
