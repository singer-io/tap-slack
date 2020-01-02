import sys
import json
import singer
from slack import WebClient
from .streams import AVAILABLE_STREAMS
from .catalog import generate_catalog

LOGGER = singer.get_logger()

def discover(webclient):
    LOGGER.info('Starting Discovery..')
    streams = [stream(webclient) for stream in AVAILABLE_STREAMS]
    catalog = generate_catalog(streams)
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished Discovery..")


def sync(webclient, config, catalog, state):

    LOGGER.info('Starting Sync..')
    streams_to_sync = []
    for catalog_stream in catalog.streams:
        for available_stream in AVAILABLE_STREAMS:
            if available_stream.name == catalog_stream.stream:
                to_sync = available_stream(webclient=webclient, config=config, catalog_stream=catalog_stream, state=state)
                streams_to_sync.append(to_sync)

    for stream in streams_to_sync:
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
