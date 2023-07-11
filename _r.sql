set timescaledb.enable_trace=false;

\set t 2
\set off 0

explain analyze insert into logdata (timestamp, idSite, code, instance, valuefloat)
values
(1688626800+:t, 164+:off, 'OV1', 0, 33.33),
(1688626801+:t, 165+:off, 'OV1', 0, 33.33),
(1688626802+:t, 166+:off, 'OV1', 0, 33.33),
(1688626803+:t, 167+:off, 'OV1', 0, 33.33),
(1688626804+:t, 168+:off, 'OV1', 0, 33.33),
(1688626805+:t, 169+:off, 'OV1', 0, 33.33);
select :t+10 as t \gset

explain analyze insert into logdata (timestamp, idSite, code, instance, valuefloat)
values
(1688626800+:t, 164+:off, 'OV1', 0, 33.33),
(1688626801+:t, 165+:off, 'OV1', 0, 33.33),
(1688626802+:t, 166+:off, 'OV1', 0, 33.33),
(1688626803+:t, 167+:off, 'OV1', 0, 33.33),
(1688626804+:t, 168+:off, 'OV1', 0, 33.33),
(1688626805+:t, 169+:off, 'OV1', 0, 33.33);

select :t+10 as t \gset

set timescaledb.enable_trace=true;

explain analyze insert into logdata (timestamp, idSite, code, instance, valuefloat)
values
(1688626800+:t, 164+:off, 'OV1', 0, 33.33),
(1688626801+:t, 165+:off, 'OV1', 0, 33.33),
(1688626802+:t, 166+:off, 'OV1', 0, 33.33),
(1688626803+:t, 167+:off, 'OV1', 0, 33.33),
(1688626804+:t, 168+:off, 'OV1', 0, 33.33),
(1688626805+:t, 168+:off, 'OV1', 0, 33.33),
(1688626806+:t, 168+:off, 'OV1', 0, 33.33),
(1688626807+:t, 168+:off, 'OV1', 0, 33.33),
(1688626808+:t, 168+:off, 'OV1', 0, 33.33),
(1688626809+:t, 168+:off, 'OV1', 0, 33.33),
(1688626810+:t, 168+:off, 'OV1', 0, 33.33),
(1688626811+:t, 168+:off, 'OV1', 0, 33.33),
(1688626812+:t, 168+:off, 'OV1', 0, 33.33),
(1688626813+:t, 168+:off, 'OV1', 0, 33.33),
(1688626814+:t, 168+:off, 'OV1', 0, 33.33),
(1688626815+:t, 168+:off, 'OV1', 0, 33.33),
(1688626816+:t, 168+:off, 'OV1', 0, 33.33),
(1688626817+:t, 168+:off, 'OV1', 0, 33.33),
(1688626818+:t, 168+:off, 'OV1', 0, 33.33),
(1688626819+:t, 168+:off, 'OV1', 0, 33.33),
(1688626820+:t, 168+:off, 'OV1', 0, 33.33),
(1688626821+:t, 169+:off, 'OV1', 0, 33.33);
select :t+30 as t \gset

-- explain analyze insert into logdata (timestamp, idSite, code, instance, valuefloat)
-- values
-- (1688626800+:t, 164+:off, 'OV1', 0, 33.33),
-- (1688626801+:t, 165+:off, 'OV1', 0, 33.33),
-- (1688626802+:t, 166+:off, 'OV1', 0, 33.33),
-- (1688626803+:t, 167+:off, 'OV1', 0, 33.33),
-- (1688626804+:t, 168+:off, 'OV1', 0, 33.33),
-- (1688626805+:t, 169+:off, 'OV1', 0, 33.33);
-- select :t+10 as t \gset

-- explain analyze insert into logdata (timestamp, idSite, code, instance, valuefloat)
-- values
-- (1688626800+:t, 164+:off, 'OV1', 0, 33.33),
-- (1688626801+:t, 165+:off, 'OV1', 0, 33.33),
-- (1688626802+:t, 166+:off, 'OV1', 0, 33.33),
-- (1688626803+:t, 167+:off, 'OV1', 0, 33.33),
-- (1688626804+:t, 168+:off, 'OV1', 0, 33.33),
-- (1688626805+:t, 169+:off, 'OV1', 0, 33.33);
-- select :t+10 as t \gset

set timescaledb.enable_trace=false;

explain analyze insert into logdata (timestamp, idSite, code, instance, valuefloat)
values
(1688626800+:t, 164+:off, 'OV1', 0, 33.33),
(1688626801+:t, 165+:off, 'OV1', 0, 33.33),
(1688626802+:t, 166+:off, 'OV1', 0, 33.33),
(1688626803+:t, 167+:off, 'OV1', 0, 33.33),
(1688626804+:t, 168+:off, 'OV1', 0, 33.33),
(1688626805+:t, 168+:off, 'OV1', 0, 33.33),
(1688626806+:t, 168+:off, 'OV1', 0, 33.33),
(1688626807+:t, 168+:off, 'OV1', 0, 33.33),
(1688626808+:t, 168+:off, 'OV1', 0, 33.33),
(1688626809+:t, 168+:off, 'OV1', 0, 33.33),
(1688626810+:t, 168+:off, 'OV1', 0, 33.33),
(1688626811+:t, 168+:off, 'OV1', 0, 33.33),
(1688626812+:t, 168+:off, 'OV1', 0, 33.33),
(1688626813+:t, 168+:off, 'OV1', 0, 33.33),
(1688626814+:t, 168+:off, 'OV1', 0, 33.33),
(1688626815+:t, 168+:off, 'OV1', 0, 33.33),
(1688626816+:t, 168+:off, 'OV1', 0, 33.33),
(1688626817+:t, 168+:off, 'OV1', 0, 33.33),
(1688626818+:t, 168+:off, 'OV1', 0, 33.33),
(1688626819+:t, 168+:off, 'OV1', 0, 33.33),
(1688626820+:t, 168+:off, 'OV1', 0, 33.33),
(1688626821+:t, 169+:off, 'OV1', 0, 33.33);
select :t+30 as t \gset
