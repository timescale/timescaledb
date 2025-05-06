-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Some primitive tests that show cost of DecompressChunk node so that we can
-- monitor the changes.

create table t(ts int, s text, c text);
select create_hypertable('t', 'ts');
alter table t set (timescaledb.compress, timescaledb.compress_segmentby = 's',
    timescaledb.compress_orderby = 'ts');
insert into t select ts, ts % 10, ts::text from generate_series(1, 10000) ts;
select count(compress_chunk(x)) from show_chunks('t') x;
vacuum freeze analyze t;


explain select * from t;

explain select * from t where s = '1';

explain select * from t where c = '100';

explain select ts from t;

explain select ts from t where s = '1';

explain select ts from t where c = '100';

explain select ts, s from t;

explain select ts, s from t where s = '1';

explain select ts, s from t where c = '100';
