import sys
import json
import singer
from slack import WebClient
from .streams import AVAILABLE_STREAMS
from .catalog import generate_catalog

LOGGER = singer.get_logger()

def discover(webclient):
    LOGGER.info('Starting Discovery..')
    streams = [stream_class(webclient) for _,stream_class in AVAILABLE_STREAMS.items()]
    catalog = generate_catalog(streams)
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished Discovery..")


def sync(webclient, config, catalog, state):

    LOGGER.info('Starting Sync..')
    for catalog_entry in catalog.get_selected_streams(state):
        stream = AVAILABLE_STREAMS[catalog_entry.stream](webclient=webclient, config=config, catalog_stream=catalog_entry.stream, state=state)
        LOGGER.info('Syncing Stream %s', catalog_entry.stream)
        stream.write_schema()
        stream.sync()
        stream.write_state()

    LOGGER.info('Finished Sync..')


def main():
    args = singer.utils.parse_args(required_config_keys=['token', 'start_date'])

    webclient = WebClient(token=args.config.get("token"))

    if args.discover:
        discover(webclient=webclient)
    elif args.catalog:
        sync(webclient=webclient, config=args.config, catalog=args.catalog, state=args.state)

if __name__ == '__main__':
    main()
