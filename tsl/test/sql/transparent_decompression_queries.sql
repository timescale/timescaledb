-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--TEST github issue 1650 character segment by column 
CREATE TABLE test_chartab ( job_run_id INTEGER NOT NULL, mac_id CHAR(16) NOT NULL, rtt INTEGER NOT NULL, ts TIMESTAMP(3) NOT NULL );

SELECT create_hypertable('test_chartab', 'ts', chunk_time_interval => interval '1 day', migrate_data => true);
insert into test_chartab
values(8864, '0014070000006190' , 392 , '2019-12-14 02:52:05.863');
insert into test_chartab
values( 8864 , '0014070000011039' , 150 , '2019-12-14 02:52:05.863');
insert into test_chartab
values( 8864 , '001407000001DD2E' , 228 , '2019-12-14 02:52:05.863');
insert into test_chartab
values( 8890 , '001407000001DD2E' , 228 , '2019-12-20 02:52:05.863');

ALTER TABLE test_chartab SET (timescaledb.compress, timescaledb.compress_segmentby = 'mac_id', timescaledb.compress_orderby = 'ts DESC');

select * from test_chartab order by mac_id , ts limit 2;

--compress the data and check --
SELECT compress_chunk('_timescaledb_internal._hyper_1_1_chunk');
select * from test_chartab order by mac_id , ts limit 2;

SELECT compress_chunk('_timescaledb_internal._hyper_1_2_chunk');
select * from test_chartab order by mac_id , ts limit 2;


