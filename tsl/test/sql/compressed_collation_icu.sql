-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Vectorized text comparison must respect a non-deterministic collation given
-- in the query, not only one declared on the column
create collation nocase (provider = icu, locale = 'und-u-ks-level2', deterministic = false);

create table t_nocase (
    ts    int not null,
    label text not null
);
select count(*) from create_hypertable('t_nocase', 'ts', chunk_time_interval => 1000);
alter table t_nocase set (timescaledb.compress);
insert into t_nocase values (1, 'merlot');
select count(compress_chunk(ch)) from show_chunks('t_nocase') ch;

-- the comparison cannot be a vectorized filter
explain (costs off) select label from t_nocase where label = 'MERLOT' collate nocase;
select count(*) from t_nocase where label = 'MERLOT' collate nocase;
select count(*) from t_nocase where label <> 'MERLOT' collate nocase;
select count(*) from t_nocase where label collate nocase in ('MERLOT', 'x');
select count(*) filter (where label = 'MERLOT' collate nocase) from t_nocase;

create table t10297 (
    batch_date int not null,
    label text not null,
    score int
);
select count(*) from create_hypertable('t10297', 'batch_date', chunk_time_interval => 1000);
insert into t10297 values (10, 'merlot', 90);
alter table t10297 set (timescaledb.compress);
select count(compress_chunk(ch)) from show_chunks('t10297') ch;
create unique index t10297_uq on t10297 (label collate nocase, batch_date);

-- rejected, like on a plain table
\set ON_ERROR_STOP 0
insert into t10297 values (10, 'MERLOT', 85);
\set ON_ERROR_STOP 1
insert into t10297 values (10, 'MERLOT', 85) on conflict do nothing;
insert into t10297 values (10, 'MERLOT', 85)
    on conflict (label collate nocase, batch_date) do update set score = excluded.score;
select * from t10297;

-- the same with label as segmentby column and with label as orderby column
create table t10297_segmentby (like t10297);
select count(*) from create_hypertable('t10297_segmentby', 'batch_date', chunk_time_interval => 1000);
create unique index t10297_segmentby_uq on t10297_segmentby (label collate nocase, batch_date);
alter table t10297_segmentby set (timescaledb.compress, timescaledb.compress_segmentby = 'label');
insert into t10297_segmentby values (10, 'merlot', 90);
select count(compress_chunk(ch)) from show_chunks('t10297_segmentby') ch;

create table t10297_orderby (like t10297);
select count(*) from create_hypertable('t10297_orderby', 'batch_date', chunk_time_interval => 1000);
create unique index t10297_orderby_uq on t10297_orderby (label collate nocase, batch_date);
alter table t10297_orderby set (timescaledb.compress, timescaledb.compress_orderby = 'label, batch_date');
insert into t10297_orderby values (10, 'merlot', 90);
select count(compress_chunk(ch)) from show_chunks('t10297_orderby') ch;

\set ON_ERROR_STOP 0
insert into t10297_segmentby values (10, 'MERLOT', 85);
insert into t10297_orderby values (10, 'MERLOT', 85);
\set ON_ERROR_STOP 1
select count(*) from t10297_segmentby;
select count(*) from t10297_orderby;

-- UPDATE and DELETE compare with the collation of the WHERE clause, so the
-- vector predicates cannot be used for a non-deterministic one
create table t10297_dml (like t10297);
select count(*) from create_hypertable('t10297_dml', 'batch_date', chunk_time_interval => 1000);
alter table t10297_dml set (timescaledb.compress);
insert into t10297_dml values (10, 'merlot', 90);
select count(compress_chunk(ch)) from show_chunks('t10297_dml') ch;
update t10297_dml set score = 0 where label = 'MERLOT' collate nocase;
select * from t10297_dml;
select count(compress_chunk(ch)) from show_chunks('t10297_dml') ch;
delete from t10297_dml where label = 'MERLOT' collate nocase;
select count(*) from t10297_dml;

-- a deterministic unique index on the same column does not weaken the check
create unique index t10297_det on t10297 (label, batch_date);
\set ON_ERROR_STOP 0
insert into t10297 values (10, 'MERLOT', 85);
insert into t10297 values (10, 'merlot', 85);
\set ON_ERROR_STOP 1
select count(*) from t10297;

-- two unique indexes with different non-deterministic collations on the same
-- column, each one matching rows the other does not
create collation noaccent (provider = icu, locale = 'und-u-kc-ks-level1', deterministic = false);
create table t10297_mixed (like t10297);
select count(*) from create_hypertable('t10297_mixed', 'batch_date', chunk_time_interval => 1000);
alter table t10297_mixed set (timescaledb.compress);
insert into t10297_mixed values (10, 'cafe', 90);
select count(compress_chunk(ch)) from show_chunks('t10297_mixed') ch;
create unique index t10297_mixed_nocase on t10297_mixed (label collate nocase, batch_date);
create unique index t10297_mixed_noaccent on t10297_mixed (label collate noaccent, batch_date);
\set ON_ERROR_STOP 0
insert into t10297_mixed values (10, 'CAFE', 85);
insert into t10297_mixed values (10, 'café', 85);
\set ON_ERROR_STOP 1
select count(*) from t10297_mixed;

-- the same with the deterministic unique index created first, and a second
-- index with the same non-deterministic collation changes nothing
create table t10297_det_first (like t10297);
select count(*) from create_hypertable('t10297_det_first', 'batch_date', chunk_time_interval => 1000);
alter table t10297_det_first set (timescaledb.compress);
insert into t10297_det_first values (10, 'merlot', 90);
select count(compress_chunk(ch)) from show_chunks('t10297_det_first') ch;
create unique index t10297_det_first_det on t10297_det_first (label, batch_date);
create unique index t10297_det_first_nocase on t10297_det_first (label collate nocase, batch_date);
create unique index t10297_det_first_nocase2 on t10297_det_first (label collate nocase, batch_date, score);
\set ON_ERROR_STOP 0
insert into t10297_det_first values (10, 'MERLOT', 85);
insert into t10297_det_first values (10, 'merlot', 85);
\set ON_ERROR_STOP 1
select count(*) from t10297_det_first;

drop table t_nocase, t10297, t10297_segmentby, t10297_orderby, t10297_dml, t10297_det_first, t10297_mixed cascade;
drop collation noaccent;
drop collation nocase;

