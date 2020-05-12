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
 - `channels:read`
 - `channels:history`
 - `users:read`

Create a config file containing the API token and a start date, e.g.:

```json
{
  "token":"xxxx",
  "start_date":"2020-05-01T00:00:00"
}
```

Optionally, you can also specify whether you want to sync private channels or not by adding the following to the config:

```json
    "private_channels":false
```

By default, private channels will be synced, and therefore you will also need these additional scopes:
- `groups:read`
- `groups:history`

## Usage

It is recommended to follow Singer [best practices](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-and-developing-singer-taps-and-targets) when running taps either [on their own](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap) or [with a Singer target](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target).

In practice, it will look something like the following:

```bash
~/.venvs/tap-slack/bin/tap-slack --config slack.config.json --catalog catalog.json | ~/.venvs/target-stitch/bin/target-stitch --config stitch.config.json
```

## Replication

The Slack Conversations API does not natively store last updated timestamp information about a Conversation. In addition, Conversation records are mutable. Thus, `tap-slack` requires a `FULL_TABLE` replication strategy to ensure the most up-to-date data in replicated when replicating the following Streams:
 - `Conversations`
 - `ConversationMembersStream`
 - `ConversationHistoryStream`

The `Users` stream _does_ store information about when a User record was last updated, so `tap-slack` uses that timestamp as a bookmark value and prefers using an `INCREMENTAL` replication strategy.

## Table Schemas

### Conversations

 - Table Name: `conversations`
 - Description:
 - Primary Key Column: `id`
 - Replication Strategy: `FULL_TABLE`
 - API Documentation: [Link](https://api.slack.com/methods/conversations.list)

### Conversation Members

 - Table Name: `conversation_members`
 - Description:
 - Primary Key Column: N/A
 - Replication Strategy: `FULL_TABLE`
 - API Documentation: [Link](https://api.slack.com/methods/conversations.members)

### Conversation History

 - Table Name: `conversation_history`
 - Description:
 - Primary Key Column: N/A
 - Replication Strategy: `FULL_TABLE`
 - API Documentation: [Link](https://api.slack.com/methods/conversations.history)

### Users

 - Table Name: `users`
 - Description:
 - Primary Key Column: `id`
 - Replication Strategy: `INCREMENTAL`
 - API Documentation: [Link](https://api.slack.com/methods/users.list)

Copyright &copy; 2019 Stitch
