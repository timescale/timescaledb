-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- A NOT VALID NOT NULL constraint must not exclude chunks from an IS NULL query.
create table nn(ts timestamptz not null, v int);
select create_hypertable('nn', by_range('ts', interval '1 day'));
insert into nn
select '2024-01-01'::timestamptz + (i || ' min')::interval,
       case when i % 10 = 0 then null else i end
from generate_series(1, 5000) i;
insert into nn
select '2024-01-03'::timestamptz + (i || ' min')::interval,
       case when i % 10 = 0 then null else i end
from generate_series(1, 5000) i;

create index on nn (v);
analyze nn;

alter table nn add constraint v_nn not null v not valid;

set plan_cache_mode = force_generic_plan;
prepare q(timestamptz) as select count(*) from nn where ts >= $1 and v is null;

-- the NULL rows must still be found
explain (analyze, buffers off, costs off, summary off, timing off)
execute q('2024-01-01');

execute q('2024-01-01');
execute q('2024-01-01');

deallocate q;
drop table nn;

-- SkipScan must still return the NULL group when a NOT VALID NOT NULL constraint allows NULLs.
create table sk(time int not null, dev int);
select create_hypertable('sk', by_range('time', 100), create_default_indexes => false);
insert into sk
select i, case when i % 7 = 0 then null else i % 4 end
from generate_series(1, 50) i;
create index on sk(dev);
analyze sk;

alter table sk add constraint dev_nn not null dev not valid;

set timescaledb.debug_skip_scan_info to true;
set enable_seqscan to off;

-- the NULL group must still be returned
explain (analyze, buffers off, costs off, timing off, summary off)
select distinct dev from sk order by dev;

select distinct dev from sk order by dev;

reset timescaledb.debug_skip_scan_info;
reset enable_seqscan;
drop table sk;
