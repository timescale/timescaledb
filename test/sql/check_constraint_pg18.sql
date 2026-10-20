-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- CHECK constraints on chunks over virtual generated columns
create table virtual_check (
    measured_at timestamptz not null,
    reading int,
    doubled int generated always as (reading * 2) virtual check (doubled > 0)
) with (tsdb.hypertable, tsdb.partition_column = 'measured_at');

insert into virtual_check (measured_at, reading) values ('2025-04-11', 3);
\set ON_ERROR_STOP 0
insert into virtual_check (measured_at, reading) values ('2025-04-11', -3);
\set ON_ERROR_STOP 1
select * from virtual_check;

-- NOT ENFORCED check constraints are not checked on chunks
create table not_enforced_check (
    time timestamptz not null,
    value int check (value > 0) not enforced
) with (tsdb.hypertable, tsdb.partition_column = 'time');

insert into not_enforced_check values ('2025-04-11', -1);
select * from not_enforced_check;

drop table virtual_check, not_enforced_check;
