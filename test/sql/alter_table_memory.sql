-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Test that ALTER TABLE propagation to chunks doesn't leak memory.
-- We use an event trigger to capture PortalContext memory after each
-- ALTER TABLE completes.

\c :TEST_DBNAME :ROLE_SUPERUSER;

-- Returns the amount of memory currently allocated in a given
-- memory context.
CREATE OR REPLACE FUNCTION ts_debug_allocated_bytes(text = 'PortalContext') RETURNS bigint
AS :MODULE_PATHNAME, 'ts_debug_allocated_bytes' LANGUAGE C STRICT VOLATILE;

CREATE TABLE memory_log(id serial, bytes bigint);

-- Log current memory usage into the log table.
CREATE OR REPLACE FUNCTION log_memory() RETURNS event_trigger as $$
BEGIN
  INSERT INTO memory_log(bytes) SELECT ts_debug_allocated_bytes();
END;
$$ LANGUAGE PLPGSQL;

-- Create hypertables with increasing chunk counts using 1-day chunk intervals.
DO $$
BEGIN
  FOR i IN 1..5 LOOP
    EXECUTE format('CREATE TABLE alter_mem_%s(time timestamptz NOT NULL, value float) WITH (tsdb.hypertable,tsdb.partition_column=''time'',tsdb.chunk_interval=''1day'')', i);
    EXECUTE format($sql$INSERT INTO alter_mem_%s SELECT t, 1.0
      FROM generate_series('2020-01-01'::timestamptz, '2020-01-01'::timestamptz + make_interval(days => %s - 1), interval '1 day') t$sql$, i, i * 100);
  END LOOP;
END $$;

-- Add the event trigger after setup to avoid capturing setup DDL.
CREATE EVENT TRIGGER alter_memory_trigger ON ddl_command_end WHEN TAG IN ('ALTER TABLE') EXECUTE FUNCTION log_memory();

-- Run ALTER TABLE on hypertables with 100, 200, 300, 400, 500 chunks.
ALTER TABLE alter_mem_1 SET (fillfactor = 50);
ALTER TABLE alter_mem_2 SET (fillfactor = 50);
ALTER TABLE alter_mem_3 SET (fillfactor = 50);
ALTER TABLE alter_mem_4 SET (fillfactor = 50);
ALTER TABLE alter_mem_5 SET (fillfactor = 50);

DROP EVENT TRIGGER alter_memory_trigger;

select count(*) from memory_log;

-- Check that the memory doesn't increase with number of chunks by using
-- linear regression.
select * from memory_log where (
  select regr_slope(bytes, id - 1) / regr_intercept(bytes, id - 1)::float > 0.05 from memory_log
);
