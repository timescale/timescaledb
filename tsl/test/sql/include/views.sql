-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- This file contains views for testing and are not part of the public
-- API.

CREATE SCHEMA IF NOT EXISTS test_info;
GRANT USAGE ON SCHEMA test_info TO PUBLIC;

CREATE VIEW test_info.chunks_base AS
SELECT format('%1$I.%2$I', ch.schema_name, ch.table_name)::regclass AS chunk_oid
     , format('%1$I.%2$I', ht.schema_name, ht.table_name)::regclass AS hypertable_oid
     , sl.range_start
     , sl.range_end
     , di.column_type
  FROM _timescaledb_catalog.chunk ch
  JOIN _timescaledb_catalog.hypertable ht ON ch.hypertable_id = ht.id
  JOIN _timescaledb_catalog.chunk_constraint cc ON chunk_id = ch.id
  JOIN _timescaledb_catalog.dimension_slice sl ON cc.dimension_slice_id = sl.id
  JOIN _timescaledb_catalog.dimension di ON sl.dimension_id = di.id;

-- View to provide some information about chunks
-- chunk_id     Chunk ID
-- hypertable   Hypertable that the chunk belongs to
-- time_range   Time constraint range for chunk
CREATE VIEW test_info.chunks_tstz AS
  SELECT chunk_oid, hypertable_oid,
         tstzrange(CASE
		   WHEN range_start = -9223372036854775808 THEN NULL
		   ELSE TIMESTAMPTZ 'epoch' + range_start * INTERVAL '1 microsecond'
		   END,
		   CASE WHEN range_end = 9223372036854775807 THEN NULL
		   ELSE TIMESTAMPTZ 'epoch' + range_end * INTERVAL '1 microsecond'
		   END) AS time_range
    FROM test_info.chunks_base
   WHERE column_type = 'TIMESTAMPTZ'::regtype;

-- View to provide some information about chunks
-- chunk_oid      Chunk ID
-- hypertable_oid Hypertable that the chunk belongs to
-- time_range     Time constraint range for chunk
CREATE VIEW test_info.chunks_ts AS
  SELECT chunk_oid, hypertable_oid,
         tsrange(CASE
		 WHEN range_start = -9223372036854775808 THEN NULL
		 ELSE TIMESTAMP 'epoch' + range_start * INTERVAL '1 microsecond'
		 END,
		 CASE WHEN range_end = 9223372036854775807 THEN NULL
		 ELSE TIMESTAMP 'epoch' + range_end * INTERVAL '1 microsecond'
		 END) AS time_range
    FROM test_info.chunks_base
   WHERE column_type = 'TIMESTAMP'::regtype;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA test_info TO PUBLIC;
