-- The naming scheme for the composite bloom filter metadata columns has changed in 2.27.
-- See the commit message for more details and the bug report here:
--
-- See bug report here: https://github.com/timescale/timescaledb/issues/9578
--

DO $$
DECLARE
    rename_data RECORD;
BEGIN
    FOR rename_data IN
        --
        -- Make sure the old meta name actually exists for the compressed chunk
        -- relation, so the renaming that follows only impact real columns not
        -- some hallucinated ones.
        --
        SELECT
            att.attrelid::regclass,
            e.old_meta_name,
            e.new_meta_name,
            count(*) OVER (PARTITION BY att.attrelid, e.old_meta_name) AS n
        FROM
            pg_attribute att,
            (
            --
            -- Calculate the old and new metadata column names of the composite bloom filters.
            -- Note that the new scheme always use a hash string to distinguish between the
            -- composite columns, but the old one only used the hash if the concatenated column
            -- names were too long.
            --
            SELECT
                compress_relid,
                CASE
                    WHEN length(joined_cols_underscores) > 39
                        THEN '_ts_meta_v2_bloomh_' || hash_underscores || '_' || joined_cols_underscores
                ELSE '_ts_meta_v2_bloomh_' || joined_cols_underscores
                END as old_meta_name,
                '_ts_meta_v2_bloomh_' || hash_zeroes || '_' || joined_cols_underscores as new_meta_name
            FROM
                (
                --
                -- Calculate the first 4 characters of the md5 hashes of both the
                -- zero and underscore concatenated column names of the composite
                -- bloom filters.
                --
                SELECT
                    compress_relid,
                    substr(md5(joined_cols_zeroes),1,4) as hash_zeroes,
                    substr(md5(joined_cols_underscores),1,4) as hash_underscores,
                    joined_cols_underscores
                FROM (
                    --
                    -- Select the compression settings objects that are actually a
                    -- a 'bloom' filter, out of the already selected 'column' arrays
                    -- and return the compressed chunk relation along with the column
                    -- names concatenated with underscores as well as zeroes.
                    --
                    SELECT
                        compress_relid,
                        (SELECT string_agg(value::bytea, '\x00'::bytea) FROM jsonb_array_elements_text(cols::jsonb)) as joined_cols_zeroes,
                        array_to_string(array(select jsonb_array_elements_text(cols::jsonb)), '_') as joined_cols_underscores
                    FROM (
                        --
                        -- Select the settings where the column field is an array
                        -- which is a must for the composite bloom filters
                        --
                        SELECT
                            *,
                            ae->>'column' cols
                        FROM (
                            --
                            -- Capture the compression settings for the compressed
                            -- tables, and separate the individual settings along
                            -- with their types
                            --
                            SELECT
                                compress_relid::text,
                                jsonb_array_elements(index) ae,
                                jsonb_array_elements(index)->>'type' ty
                            FROM _timescaledb_catalog.compression_settings
                            WHERE compress_relid IS NOT NULL
                        ) a
                    WHERE jsonb_typeof(ae->'column') = 'array'
                    ) b
                 WHERE ty = 'bloom'
                ) c
            ) d
        ) e
        WHERE att.attrelid = e.compress_relid::regclass AND att.attname = e.new_meta_name
    LOOP
        IF rename_data.n > 1 THEN
            RAISE EXCEPTION 'Downgrade is not possible because ambiguous composite bloom filters found. The ambiguous composite bloom filter column % for relation % need to be removed with the ALTER command.',
                rename_data.new_meta_name,
                rename_data.attrelid;
        END IF;

        RAISE NOTICE 'RENAMING: %.% to %',
            rename_data.attrelid,
            rename_data.new_meta_name,
            rename_data.old_meta_name;

        EXECUTE format(
            'ALTER TABLE %s RENAME COLUMN %I TO %I',
            rename_data.attrelid,
            rename_data.new_meta_name,
            rename_data.old_meta_name
        );
    END LOOP;
END;
$$;
