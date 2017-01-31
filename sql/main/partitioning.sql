CREATE OR REPLACE FUNCTION _iobeamdb_catalog.get_partition_for_key(
    key TEXT,
    mod_factor INT
)
    RETURNS SMALLINT LANGUAGE SQL IMMUTABLE STRICT AS $$
SELECT ((_iobeamdb_internal.murmur3_hash_string(key, 1 :: INT4) & x'7fffffff' :: INTEGER) % mod_factor) :: SMALLINT;
$$;
