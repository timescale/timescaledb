drop table if exists logdata;
create table logdata
(
  timestamp bigint not null,
  idSite integer not null,
  code text not null,
  instance smallint not null,
  venusVersion text,
  productId text,
  valueFloat float,
  valueString text,
  valueEnum smallint
);

select create_hypertable('logdata', 'timestamp', chunk_time_interval=>3600);

create unique index logDataComposite on logdata (idSite, code, instance, timestamp desc);

-- function needed for retention policy/compression
CREATE OR REPLACE FUNCTION unix_now() returns BIGINT LANGUAGE SQL STABLE as $$ SELECT 1688626801-4000 $$;
select set_integer_now_func('logdata', 'unix_now');

select add_retention_policy('logdata', 15552000); -- 6 months

alter table logdata set (timescaledb.compress, timescaledb.compress_segmentby = 'idSite');
select add_compression_policy('logdata', 14400);


\set t0 (1688626801-4000)

INSERT into logdata (timestamp, idSite, code, instance, valuefloat) 
select :t0 + ((97*x)%11971), x%1001, 'JP', 0 , 33.33
from generate_series(1,3000) as t(x);

select compress_chunk(show_chunks('logdata'));

--call run_job (1000); --to compress the chunks.
--call run_job (policy_id) --to compress the chunks.

-- Look for a chunk that is compressed the insert a record. on my test below is my test insert in a compressed chunk

--explain analyze insert into logdata (timestamp, idSite, code, instance, valuefloat) values (1679068801, 35164, 'OV1', 0, 33.33);
