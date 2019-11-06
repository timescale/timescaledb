-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\ir post.v3.sql

--PG11 added an optimization where columns that were added by
--an ALTER TABLE that had a DEFAULT value did not cause a table re-write.
--Instead, those columns are filled with the default value on read.
--But, this mechanism does not apply to catalog tables and does
--not work with our catalog scanning code.
--Thus make sure all catalog tables do not have this enabled (a.atthasmissing == false)
CREATE OR REPLACE FUNCTION timescaledb_catalog_has_no_missing_columns()
    RETURNS VOID LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    cnt   INTEGER;
    rel_names TEXT;
BEGIN

    SELECT count(*), string_agg(c.relname,' ;')
    FROM pg_namespace n
    INNER JOIN pg_class c ON (c.relnamespace = n.oid)
    INNER JOIN pg_attribute a ON attrelid=c.oid
    WHERE   a.attnum >= 0 AND
            (n.nspname='_timescaledb_catalog' OR n.nspname = '_timescaledb_config' OR
            n.nspname='_timescaledb_internal')
        AND a.atthasmissing
    INTO STRICT cnt, rel_names;

    IF cnt != 0 THEN
        RAISE EXCEPTION 'Some catalog tables were altered without a table re-write: %', rel_names;
    END IF;
END;
$BODY$;

SELECT timescaledb_catalog_has_no_missing_columns();