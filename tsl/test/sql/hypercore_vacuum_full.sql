-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\ir include/setup_hypercore.sql

alter table :chunk1 set access method hypercore;

-- check that all chunks are compressed
select chunk_name, compression_status from chunk_compression_stats(:'hypertable');

create view compressed_chunks as
with reg_chunk as (
     select * from _timescaledb_catalog.chunk where compressed_chunk_id IS NOT NULL
)
select format('%I.%I', reg_chunk.schema_name, reg_chunk.table_name)::regclass as chunk,
       format('%I.%I', cpr_chunk.schema_name, cpr_chunk.table_name)::regclass as compressed_chunk
from _timescaledb_catalog.chunk cpr_chunk
inner join reg_chunk on (cpr_chunk.id = reg_chunk.compressed_chunk_id);

select compressed_chunk as cchunk1 from compressed_chunks where chunk = :'chunk1'::regclass \gset

-- save original counts
select location_id, count(*) into orig from :hypertable GROUP BY location_id;

-- show original compressed segment count
select count(*) from :cchunk1;
-- update one location_id to decompress some data
update :hypertable set temp=1.0 where location_id=1;

-- first try CLUSTER since implemented with same codepath as VACUUM FULL
\set ON_ERROR_STOP 0
cluster :chunk1;
cluster :hypertable using hypertable_location_id_idx;
\set ON_ERROR_STOP 1

-- some, but not all, data decompressed
select count(*) from :cchunk1;
-- run vacuum full to recompress
vacuum full :hypertable;
-- should now be fully compressed again
select count(*) from :cchunk1;
-- also try vacuum full on chunk level
update :hypertable set temp=1.0 where location_id=1;
select count(*) from :cchunk1;
vacuum full :hypertable;
select count(*) from :cchunk1;

-- check that table data (or at least counts) is still the same
select location_id, count(*) into comp from :hypertable GROUP BY location_id;
select * from orig join comp using (location_id) where orig.count != comp.count;
