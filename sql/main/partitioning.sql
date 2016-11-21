CREATE OR REPLACE FUNCTION get_partition_for_key(key TEXT, mod_factor INT)
    RETURNS SMALLINT LANGUAGE SQL IMMUTABLE STRICT AS $$
SELECT ((public.hash_string(key, 'murmur3' :: TEXT, 1 :: INT4) & x'7fffffff' :: INTEGER) % mod_factor) :: SMALLINT;
$$;



