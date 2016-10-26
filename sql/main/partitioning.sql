CREATE OR REPLACE FUNCTION get_partition_for_key(key TEXT, num_nodes SMALLINT)
    RETURNS SMALLINT LANGUAGE SQL IMMUTABLE STRICT AS $$
SELECT ((public.hash_string(key, 'murmur3' :: TEXT, 1 :: INT4) & x'7fffffff' :: INTEGER) % num_nodes)::SMALLINT;
$$;



