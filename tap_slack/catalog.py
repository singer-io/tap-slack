import singer

def generate_catalog(streams):

    catalog = {}
    catalog['streams'] = []
    for stream in streams:
        schema = stream.load_schema()
        catalog_entry = {
            'stream': stream.name,
            'tap_stream_id': stream.name,
            'schema': schema,
            'metadata': singer.metadata.get_standard_metadata(schema=schema,
                                                              key_properties=stream.key_properties,
                                                              valid_replication_keys=stream.valid_replication_keys,
                                                              replication_method=stream.replication_method)
        }
        # TODO: valid_replication_keys do not get marked with automatic metadata which could break bookmarking
        catalog['streams'].append(catalog_entry)

    return catalog
