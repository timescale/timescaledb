-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

create table vectorqual(metric1 int8, ts timestamp, metric2 int8, device int8);
select create_hypertable('vectorqual', 'ts');
alter table vectorqual set (timescaledb.compress, timescaledb.compress_segmentby = 'device');

insert into vectorqual(ts, device, metric1, metric2) values ('2020-01-01 00:00:00', 1, 11, 12);
select count(compress_chunk(x, true)) from show_chunks('vectorqual') x;

alter table vectorqual drop column metric1;
insert into vectorqual(ts, device, metric2) values ('2021-01-01 00:00:00', 2, 22);
select count(compress_chunk(x, true)) from show_chunks('vectorqual') x;

alter table vectorqual add column metric3 int8 default 777;
insert into vectorqual(ts, device, metric2, metric3) values ('2022-01-01 00:00:00', 3, 32, 33);
select count(compress_chunk(x, true)) from show_chunks('vectorqual') x;

alter table vectorqual add column metric4 int8;
insert into vectorqual(ts, device, metric2, metric3, metric4) values ('2023-01-01 00:00:00', 4, 42, 43, 44);
select count(compress_chunk(x, true)) from show_chunks('vectorqual') x;

set timescaledb.debug_enable_vector_qual to 'only' /* all following quals must be vectorized */;
select count(*) from vectorqual where ts > '1999-01-01 00:00:00';
select count(*) from vectorqual where metric2 = 22;
select count(*) from vectorqual where 22 = metric2 /* commutators */;
select count(*) from vectorqual where metric3 = 33;
select count(*) from vectorqual where metric3 = 777 /* default value */;
select count(*) from vectorqual where metric4 = 44 /* column with default null */;
select count(*) from vectorqual where metric4 >= 0 /* nulls shouldn't pass the qual */;

set timescaledb.debug_enable_vector_qual to 'off';
select count(*) from vectorqual where device = 1 /* can't apply vector ops to the segmentby column */;