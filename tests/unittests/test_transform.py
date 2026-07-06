from tap_slack.transform import decimal_timestamp_to_utc_timestamp, transform_json


def test_decimal_timestamp_to_utc_timestamp():
    assert decimal_timestamp_to_utc_timestamp("1712345678.123456") == "1712345678"


def test_transform_json_messages_and_channels_and_threads():
    messages = [
        {
            "ts": "1712345678.123456",
            "files": [{"id": "F1"}, {"name": "missing-id"}],
            "text": "hello",
        }
    ]
    out_messages = transform_json("messages", messages, ["ts"], channel_id="C1")
    assert out_messages[0]["file_ids"] == ["F1"]
    assert out_messages[0]["channel_id"] == "C1"
    assert out_messages[0]["ts"] == "1712345678"
    assert out_messages[0]["thread_ts"] == "1712345678.123456"

    channels = [{"id": "C1", "parent_conversation": "abc", "channel_id": "old"}]
    out_channels = transform_json("channels", channels, [])
    assert "parent_conversation" not in out_channels[0]
    assert "channel_id" not in out_channels[0]

    threads = [{"ts": "1712345678.999", "last_read": "1712345000.000"}]
    out_threads = transform_json("threads", threads, ["ts", "last_read"], channel_id="C2")
    assert out_threads[0]["channel_id"] == "C2"
    assert out_threads[0]["thread_ts"] == "1712345678.999"
    assert out_threads[0]["ts"] == "1712345678"
    assert out_threads[0]["last_read"] == "1712345000"


def test_transform_json_with_empty_input():
    assert transform_json("users", [], ["updated"]) == []
    assert transform_json("users", None, ["updated"]) is None
