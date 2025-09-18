import singer

def generate_catalog(streams):

    catalog = {}
    catalog['streams'] = []
    for stream in streams:
        schema = stream.load_schema()

        mdata = singer.metadata.get_standard_metadata(
            schema=schema,
            key_properties=stream.key_properties,
            valid_replication_keys=stream.valid_replication_keys,
            replication_method=stream.replication_method
        )
        mdata = singer.metadata.to_map(mdata)

        parent_tap_stream_id = getattr(stream, "parent", None)
        if parent_tap_stream_id:
            mdata = singer.metadata.write(mdata, (), 'parent-tap-stream-id', parent_tap_stream_id)

        mdata = singer.metadata.to_list(mdata)        
        catalog_entry = {
            'stream': stream.name,
            'tap_stream_id': stream.name,
            'schema': schema,
            'metadata': mdata
        }
        catalog['streams'].append(catalog_entry)

    return catalog
