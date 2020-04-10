-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--------------------------------------------------------------------------
-- show_chunks and drop_chunks functions on a compressed table 
-- (issue https://github.com/timescale/timescaledb/issues/1535)

-- create a table that will not be compressed
CREATE TABLE public.uncompressed_table(time date NOT NULL, temp float8, device_id text);
CREATE INDEX ON public.uncompressed_table(time DESC);
SELECT create_hypertable('public.uncompressed_table', 'time', chunk_time_interval => interval '1 day');

INSERT INTO public.uncompressed_table VALUES('2020-03-01', 1.0, 'dev1');
INSERT INTO public.uncompressed_table VALUES('2020-03-05', 2.0, 'dev1');
INSERT INTO public.uncompressed_table VALUES('2020-03-07', 3.0, 'dev1');
INSERT INTO public.uncompressed_table VALUES('2020-03-08', 4.0, 'dev7');
INSERT INTO public.uncompressed_table VALUES('2020-03-09', 5.0, 'dev7');
INSERT INTO public.uncompressed_table VALUES('2020-03-10', 6.0, 'dev7');

-- create next table that is going to be compressed:
CREATE TABLE public.table_to_compress (time date NOT NULL, acq_id bigint, value bigint);
CREATE INDEX idx_table_to_compress_acq_id ON public.table_to_compress(acq_id);
SELECT create_hypertable('public.table_to_compress', 'time', chunk_time_interval => interval '1 day');
ALTER TABLE public.table_to_compress SET (timescaledb.compress, timescaledb.compress_segmentby = 'acq_id');
INSERT INTO public.table_to_compress VALUES ('2020-01-01', 1234567, 777888);
INSERT INTO public.table_to_compress VALUES ('2020-02-01', 567567, 890890);
INSERT INTO public.table_to_compress VALUES ('2020-02-10', 1234, 5678);
SELECT show_chunks('public.table_to_compress');
SELECT show_chunks(hypertable=>'public.table_to_compress', older_than=>'1 day'::interval);
SELECT show_chunks(hypertable=>'public.table_to_compress', newer_than=>'1 day'::interval);
SELECT show_chunks(); -- across all hypertables
-- compress all chunks of the table:
SELECT compress_chunk(show_chunks(hypertable=>'public.table_to_compress'));
-- and run the queries again to make sure results are the same
SELECT show_chunks('public.table_to_compress');
SELECT show_chunks(hypertable=>'public.table_to_compress', older_than=>'1 day'::interval);
SELECT show_chunks(hypertable=>'public.table_to_compress', newer_than=>'1 day'::interval);
SELECT show_chunks(); 
-- drop all hypertables' old chunks
SELECT drop_chunks(older_than=>'1 day'::interval);
SELECT show_chunks(); 
