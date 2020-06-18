import sys
import json
import singer
from slack import WebClient
from tap_slack.streams import AVAILABLE_STREAMS
from tap_slack.catalog import generate_catalog

LOGGER = singer.get_logger()


def auto_join(webclient):
    response = webclient.conversations_list(types="public_channel", exclude_archived="true")
    conversations = response.get("channels", [])

    for conversation in conversations:
        conversation_id = conversation.get("id", None)
        conversation_name = conversation.get("name", None)
        join_response = webclient.conversations_join(channel=conversation_id)
        if not join_response.get("ok", False):
            error = join_response.get("error", "Unspecified Error")
            LOGGER.error('Error joining {}, Reason: {}'.format(conversation_name, error))
            raise Exception('{}: {}'.format(conversation_name, error))


def discover(webclient):
    LOGGER.info('Starting Discovery..')
    streams = [stream_class(webclient) for _, stream_class in AVAILABLE_STREAMS.items()]
    catalog = generate_catalog(streams)
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished Discovery..")


def sync(webclient, config, catalog, state):
    LOGGER.info('Starting Sync..')
    for catalog_entry in catalog.get_selected_streams(state):
        if "threads" not in catalog_entry.stream:
            stream = AVAILABLE_STREAMS[catalog_entry.stream](webclient=webclient, config=config,
                                                             catalog=catalog,
                                                             state=state)
            LOGGER.info('Syncing stream: %s', catalog_entry.stream)
            stream.write_schema()
            stream.sync(catalog_entry.metadata)
            stream.write_state()

    LOGGER.info('Finished Sync..')


def main():
    args = singer.utils.parse_args(required_config_keys=['token', 'start_date'])

    webclient = WebClient(token=args.config.get("token"))

    if args.discover:
        discover(webclient=webclient)
    elif args.catalog:
        if args.config.get("join_public_channels", "false") == "true":
            auto_join(webclient=webclient)
        sync(webclient=webclient, config=args.config, catalog=args.catalog, state=args.state)


if __name__ == '__main__':
    main()
