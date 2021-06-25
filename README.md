# ![slack icon](etc/slack_icon.png) tap-slack
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Python 3.7](https://upload.wikimedia.org/wikipedia/commons/f/fc/Blue_Python_3.7_Shield_Badge.svg)](https://www.python.org/downloads/release/python-370/)

Singer.io tap for extracting data from the Slack Web API

## Installation

It is highly recommended installing `tap-slack` in it's own isolated virtual environment. For example:

```bash
python3 -m venv ~/.venvs/tap-slack
source ~/.venvs/tap-slack/bin/activate
pip3 install tap-slack
deactivate
```

## Setup

The tap requires a [Slack API token](https://github.com/slackapi/python-slackclient/blob/master/documentation_v2/auth.md#tokens--authentication) to interact with your Slack workspace. You can obtain a token for a single workspace by creating a new [Slack App](https://api.slack.com/apps?new_app=1) in your workspace and assigning it the relevant [scopes](https://api.slack.com/docs/oauth-scopes). As of right now, the minimum required scopes for this App are:
 - `channels:history`
 - `channels:join`
 - `channels:read`
 - `files:read`
 - `groups:read`
 - `links:read`
 - `reactions:read`
 - `remote_files:read`
 - `remote_files:write`
 - `team:read`
 - `usergroups:read`
 - `users.profile:read`
 - `users:read`
 - `users:read.email`

Create a config file containing the API token and a start date, e.g.:

```json
{
  "token":"xxxx",
  "start_date":"2020-05-01T00:00:00"
}
```

### Private channels

Optionally, you can also specify whether you want to sync private channels or not by adding the following to the config:

```json
    "private_channels":"false"
```

By default, private channels will be synced.

### Joining Public Channels

By adding the following to your config file you can have the tap auto-join all public channels in your ogranziation.
```json
"join_public_channels":"true"
```
If you do not elect to have the tap join all public channels you must invite the bot to all channels you wish to sync.

### Specify channels to sync

By default, the tap will sync all channels it has been invited to. However, you can limit the tap to sync only the channels you specify by adding their IDs to the config file, e.g.:

```json
"channels":[
    "abc123",
    "def345"
  ]
```

Note this needs to be channel ID, not the name, as [recommended by the Slack API](https://api.slack.com/types/conversation#other_attributes). To get the ID for a channel, either use the Slack API or [find it in the URL](https://www.wikihow.com/Find-a-Channel-ID-on-Slack-on-PC-or-Mac).

### Archived Channels

You can control whether or not the tap will sync archived channels by including the following in the tap config:
```json
  "exclude_archived": "false"
```
It's important to note that a bot *CANNOT* join an archived channel, so unless the bot was added to the channel prior to it being archived it will not be able to sync the data from that channel.

### Date Windowing

Due to the potentially high volume of data when syncing certain streams (messages, files, threads)
this tap implements date windowing based on a configuration parameter.

including 
```json
"date_window_size": "5"
```

Will cause the tap to sync 5 days of data per request, for applicable streams. The default value if 
one is not defined is to window requests for 7 days at a time.

## Usage

It is recommended to follow Singer [best practices](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-and-developing-singer-taps-and-targets) when running taps either [on their own](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap) or [with a Singer target](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target).

In practice, it will look something like the following:

```bash
~/.venvs/tap-slack/bin/tap-slack --config slack.config.json --catalog catalog.json | ~/.venvs/target-stitch/bin/target-stitch --config stitch.config.json
```

## Replication

The Slack Conversations API does not natively store last updated timestamp information about a Conversation. In addition, Conversation records are mutable. Thus, `tap-slack` requires a `FULL_TABLE` replication strategy to ensure the most up-to-date data in replicated when replicating the following Streams:
 - `Channels` (Conversations)
 - `Channel Members` (Conversation Members)

The `Users` stream _does_ store information about when a User record was last updated, so `tap-slack` uses that timestamp as a bookmark value and prefers using an `INCREMENTAL` replication strategy.

## Table Schemas

### Channels (Conversations)

 - Table Name: `channels`
 - Description:
 - Primary Key Column: `id`
 - Replication Strategy: `FULL_TABLE`
 - API Documentation: [Link](https://api.slack.com/methods/conversations.list)

### Channel Members (Conversation Members)

 - Table Name: `channel_members`
 - Description:
 - Primary Key Columns: `channel_id`, `user_id`
 - Replication Strategy: `FULL_TABLE`
 - API Documentation: [Link](https://api.slack.com/methods/conversations.members)

### Messages (Conversation History)

 - Table Name: `messages`
 - Description:
 - Primary Key Columns: `channel_id`, `ts`
 - Replication Strategy: `INCREMENTAL`
 - API Documentation: [Link](https://api.slack.com/methods/conversations.history)

### Users

 - Table Name: `users`
 - Description:
 - Primary Key Column: `id`
 - Replication Strategy: `INCREMENTAL`
 - API Documentation: [Link](https://api.slack.com/methods/users.list)
 
### Threads (Conversation Replies)

 - Table Name: `threads`
 - Description:
 - Primary Key Columns: `channel_id`, `ts`, `thread_ts`
 - Replication Strategy: `FULL_TABLE` for each parent `message`
 - API Documentation: [Link](https://api.slack.com/methods/conversations.replies)
 
### User Groups 

 - Table Name: `user_groups`
 - Description:
 - Primary Key Column: `id`
 - Replication Strategy: `FULL_TABLE`
 - API Documentation: [Link](https://api.slack.com/methods/usergroups.list)
 
### Files 

 - Table Name: `files`
 - Description:
 - Primary Key Column: `id`
 - Replication Strategy: `INCREMENTAL` query filtered using date windows and lookback window
 - API Documentation: [Link](https://api.slack.com/methods/files.list)
 
### Remote Files 

 - Table Name: `remote_files`
 - Description:
 - Primary Key Column: `id`
 - Replication Strategy: `INCREMENTAL` query filtered using date windows and lookback window
 - API Documentation: [Link](https://api.slack.com/methods/files.remote.list)
 
## Testing the Tap
    
While developing the Slack tap, the following utilities were run in accordance with Singer.io best practices:
Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md#code-quality):
```bash
> pylint tap_slack -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
```
Pylint test resulted in the following score:
```bash
Your code has been rated at 9.72/10 

```

To [check the tap](https://github.com/singer-io/singer-tools#singer-check-tap) and verify working:
```bash
> tap-slack --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
> tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
```
Check tap resulted in the following:
 ```bash
Checking stdin for valid Singer-formatted data
The output is valid.
It contained 3657 messages for 9 streams.

    581 schema messages
   2393 record messages
    683 state messages

Details by stream:
+-----------------+---------+---------+
| stream          | records | schemas |
+-----------------+---------+---------+
| threads         | 633     | 573     |
| user_groups     | 1       | 1       |
| channel_members | 1049    | 1       |
| users           | 22      | 1       |
| channels        | 0       | 1       |
| remote_files    | 3       | 1       |
| messages        | 573     | 1       |
| teams           | 1       | 1       |
| files           | 111     | 1       |
+-----------------+---------+---------+
```
----
Copyright &copy; 2019 Stitch
