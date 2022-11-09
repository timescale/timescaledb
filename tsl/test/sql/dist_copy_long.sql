-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test distributed COPY with a bigger data set to help find rare effects.

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

\set DN_DBNAME_1 :TEST_DBNAME _1
\set DN_DBNAME_2 :TEST_DBNAME _2
\set DN_DBNAME_3 :TEST_DBNAME _3

SELECT 1 FROM add_data_node('data_node_1', host => 'localhost',
                            database => :'DN_DBNAME_1');
SELECT 1 FROM add_data_node('data_node_2', host => 'localhost',
                            database => :'DN_DBNAME_2');
SELECT 1 FROM add_data_node('data_node_3', host => 'localhost',
                            database => :'DN_DBNAME_3');
GRANT USAGE ON FOREIGN SERVER data_node_1, data_node_2, data_node_3 TO PUBLIC;
-- though user on access node has required GRANTS, this will propagate GRANTS to the connected data nodes
GRANT CREATE ON SCHEMA public TO :ROLE_1;
SET ROLE :ROLE_1;


create table uk_price_paid(price integer, "date" date, postcode1 text, postcode2 text, type smallint, is_new bool, duration smallint, addr1 text, addr2 text, street text, locality text, town text, district text, country text, category smallint);
-- Aim to about 100 partitions, the data is from 1995 to 2022.
select create_distributed_hypertable('uk_price_paid', 'date', chunk_time_interval => interval '270 day');

create table uk_price_paid_space2(like uk_price_paid);
select create_distributed_hypertable('uk_price_paid_space2', 'date', 'postcode2', 2, chunk_time_interval => interval '270 day');

create table uk_price_paid_space10(like uk_price_paid);
select create_distributed_hypertable('uk_price_paid_space10', 'date', 'postcode2', 10, chunk_time_interval => interval '270 day');


\copy uk_price_paid_space2 from program 'zcat < data/prices-10k-random-1.tsv.gz';
select count(*) from uk_price_paid_space2;
\copy uk_price_paid_space2 from program 'zcat < data/prices-10k-random-1.tsv.gz';
select count(*) from uk_price_paid_space2;

\copy uk_price_paid_space10 from program 'zcat < data/prices-10k-random-1.tsv.gz';
select count(*) from uk_price_paid_space10;
\copy uk_price_paid_space10 from program 'zcat < data/prices-10k-random-1.tsv.gz';
select count(*) from uk_price_paid_space10;


set timescaledb.max_open_chunks_per_insert = 1;

\copy uk_price_paid from program 'zcat < data/prices-100k-random-1.tsv.gz';
select count(*) from uk_price_paid;

\copy uk_price_paid from program 'zcat < data/prices-100k-random-1.tsv.gz';
select count(*) from uk_price_paid;

truncate uk_price_paid;


set timescaledb.max_open_chunks_per_insert = 2;

\copy uk_price_paid from program 'zcat < data/prices-100k-random-1.tsv.gz';
select count(*) from uk_price_paid;

\copy uk_price_paid from program 'zcat < data/prices-100k-random-1.tsv.gz';
select count(*) from uk_price_paid;

truncate uk_price_paid;


set timescaledb.max_open_chunks_per_insert = 1117;

\copy uk_price_paid from program 'zcat < data/prices-100k-random-1.tsv.gz';
select count(*) from uk_price_paid;

\copy uk_price_paid from program 'zcat < data/prices-100k-random-1.tsv.gz';
select count(*) from uk_price_paid;

-- Different replication factors
create table uk_price_paid_r2(like uk_price_paid);
select create_distributed_hypertable('uk_price_paid_r2', 'date', 'postcode2',
    chunk_time_interval => interval '90 day', replication_factor => 2);

create table uk_price_paid_r3(like uk_price_paid);
select create_distributed_hypertable('uk_price_paid_r3', 'date', 'postcode2',
    chunk_time_interval => interval '90 day', replication_factor => 3);

select hypertable_name, replication_factor from timescaledb_information.hypertables
where hypertable_name like 'uk_price_paid%' order by hypertable_name;

\copy uk_price_paid_r2 from program 'zcat < data/prices-10k-random-1.tsv.gz';
select count(*) from uk_price_paid_r2;

\copy uk_price_paid_r3 from program 'zcat < data/prices-10k-random-1.tsv.gz';
select count(*) from uk_price_paid_r3;

-- 0, 1, 2 rows
\copy uk_price_paid from stdin
\.

select count(*) from uk_price_paid;

\copy uk_price_paid from program 'zcat < data/prices-10k-random-1.tsv.gz | head -1';

select count(*) from uk_price_paid;

\copy uk_price_paid from program 'zcat < data/prices-10k-random-1.tsv.gz | head -2';

select count(*) from uk_price_paid;

-- Teardown
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
DROP DATABASE :DN_DBNAME_1;
DROP DATABASE :DN_DBNAME_2;
DROP DATABASE :DN_DBNAME_3;

