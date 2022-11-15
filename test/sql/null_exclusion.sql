-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

create table metrics(ts timestamp, id int, value float);
select create_hypertable('metrics', 'ts');
insert into metrics values ('2022-02-02 02:02:02', 2, 2.),
    ('2023-03-03 03:03:03', 3, 3.);
analyze metrics;

-- non-const condition
explain (analyze, costs off, summary off, timing off)
select * from metrics
where ts >= (select max(ts) from metrics);

-- two non-const conditions
explain (analyze, costs off, summary off, timing off)
select * from metrics
where ts >= (select max(ts) from metrics)
    and id = 1;

-- condition that becomes const null after evaluating the param
explain (analyze, costs off, summary off, timing off)
select * from metrics
where ts >= (select max(ts) from metrics where id = -1);

-- const null condition and some other condition
explain (analyze, costs off, summary off, timing off)
select * from metrics
where ts >= (select max(ts) from metrics where id = -1)
    and id = 1;
