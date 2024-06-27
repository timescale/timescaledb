-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\ir include/setup_hyperstore.sql

-- Set parallel cost to zero to force parallel plans and avoid flaky test.
set parallel_tuple_cost to 0;
set parallel_setup_cost to 0;

-- We need to drop the index to trigger parallel plans. Otherwise they
-- will use the index.
drop index hypertable_device_id_idx;

-- Show parallel plan and count on uncompressed (non-hyperstore)
-- hypertable
set max_parallel_workers_per_gather=2;

select explain_anonymize(format($$
       select device_id, count(*) from %s where device_id=1 group by device_id
$$, :'hypertable'));
select device_id, count(*) from :hypertable where device_id=1 group by device_id;

-- Save counts collected over entire hypertable
select device_id, count(*) into orig from :hypertable group by device_id;
-- Save counts over a single chunk
select device_id, count(*) into orig_chunk from :chunk1 group by device_id;

-----------------------
-- Enable hyperstore --
-----------------------
select compress_chunk(show_chunks(:'hypertable'), compress_using => 'hyperstore');

-- Show count without parallel plan and without ColumnarScan
set timescaledb.enable_columnarscan=false;
set max_parallel_workers_per_gather=0;
select explain_anonymize(format($$
       select device_id, count(*) from %s where device_id=1 group by device_id
$$, :'hypertable'));
select device_id, count(*) from :hypertable where device_id=1 group by device_id;

-- Enable parallel on SeqScan and check for same result
set max_parallel_workers_per_gather=2;
select explain_anonymize(format($$
       select device_id, count(*) from %s where device_id=1 group by device_id
$$, :'hypertable'));
select device_id, count(*) from :hypertable where device_id=1 group by device_id;

-- Enable ColumnarScan and check for same result
set timescaledb.enable_columnarscan=true;
select explain_anonymize(format($$
       select device_id, count(*) from %s where device_id=1 group by device_id
$$, :'hypertable'));
select device_id, count(*) from :hypertable where device_id=1 group by device_id;

-- Parallel plan with hyperstore on single chunk
select explain_anonymize(format($$
       select device_id, count(*) from %s where device_id=1 group by device_id
$$, :'hypertable'));
select device_id, count(*) from :chunk1 where device_id=1 group by device_id;

-- Compare hyperstore per-location counts with original counts without
-- hyperstore
select device_id, count(*) into comp from :hypertable group by device_id;
select * from orig join comp using (device_id) where orig.count != comp.count;

-- Compare counts on single chunk
select device_id, count(*) into comp_chunk from :chunk1 group by device_id;
select * from orig_chunk join comp_chunk using (device_id) where orig_chunk.count != comp_chunk.count;
