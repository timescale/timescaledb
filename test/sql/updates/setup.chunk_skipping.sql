-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.


CREATE TABLE chunkskip (
      time                  TIMESTAMPTZ         NOT NULL,
      updated_at            TIMESTAMPTZ         NOT NULL,
      location              INT,
      temp                  FLOAT
    );

SELECT setseed(0.1);
SELECT table_name FROM create_hypertable( 'chunkskip', 'time');

INSERT INTO chunkskip
SELECT g, g+interval '1 hour', ceil(random()*20), random()*30
FROM generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day') g;

ALTER TABLE chunkskip SET (timescaledb.compress, timescaledb.compress_segmentby='location');
SELECT count(compress_chunk(ch, true)) FROM show_chunks('chunkskip') ch;

DO $$
DECLARE
  version text[] := (SELECT regexp_split_to_array(extversion,'(\.|-)') FROM pg_extension WHERE extname = 'timescaledb');
BEGIN

  -- Enable chunk skipping. Doing this after compression to have a
  -- "special" chunk_id entry of 0. We check that this is changed to
  -- NULL during upgrade.
  IF version[1]::int >= 2 AND version[2]::int >= 17 AND (version[2]::int > 17 OR version[3]::int > 0) THEN
     SET timescaledb.enable_chunk_skipping = true;
  END IF;
END
$$;

SELECT enable_chunk_skipping('chunkskip', 'updated_at');
