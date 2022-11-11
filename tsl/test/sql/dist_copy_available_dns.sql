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

CREATE VIEW chunk_query_data_node AS
SELECT ch.hypertable_name, format('%I.%I', ch.chunk_schema, ch.chunk_name)::regclass AS chunk, ch.data_nodes, fs.srvname default_data_node
       FROM timescaledb_information.chunks ch
       INNER JOIN pg_foreign_table ft ON (format('%I.%I', ch.chunk_schema, ch.chunk_name)::regclass = ft.ftrelid)
       INNER JOIN pg_foreign_server fs ON (ft.ftserver = fs.oid)
       ORDER BY 1 DESC, 2 DESC;

create table uk_price_paid(price integer, "date" date, postcode1 text, postcode2 text, type smallint, is_new bool, duration smallint, addr1 text, addr2 text, street text, locality text, town text, district text, country text, category smallint);
-- Aim to about 100 partitions, the data is from 1995 to 2022.
select create_distributed_hypertable('uk_price_paid', 'date', chunk_time_interval => interval '270 day', replication_factor=>3);

create table uk_price_paid_space2(like uk_price_paid);
select create_distributed_hypertable('uk_price_paid_space2', 'date', 'postcode2', 2, chunk_time_interval => interval '270 day', replication_factor => 2);

\copy uk_price_paid_space2 from program 'zcat < data/prices-10k-random-1.tsv.gz';
SELECT * FROM chunk_query_data_node WHERE hypertable_name = 'uk_price_paid_space2' LIMIT 5;

set timescaledb.max_open_chunks_per_insert = 1;

\copy uk_price_paid from program 'zcat < data/prices-10k-random-1.tsv.gz';
select count(*) from uk_price_paid;
SELECT * FROM chunk_query_data_node WHERE hypertable_name = 'uk_price_paid' LIMIT 5;

SET ROLE :ROLE_CLUSTER_SUPERUSER;
SELECT * FROM alter_data_node('data_node_1', available=>false);
SET ROLE :ROLE_1;

set timescaledb.max_open_chunks_per_insert = 2;

-- we will write to the same set of chunks and update AN metadata for down DN
\copy uk_price_paid from program 'zcat < data/prices-10k-random-1.tsv.gz';
select count(*) from uk_price_paid;
SELECT * FROM chunk_query_data_node WHERE hypertable_name = 'uk_price_paid' LIMIT 5;

set timescaledb.max_open_chunks_per_insert = 1117;

SET ROLE :ROLE_CLUSTER_SUPERUSER;
SELECT * FROM alter_data_node('data_node_1', available=>true);
SET ROLE :ROLE_1;

TRUNCATE uk_price_paid;

\copy uk_price_paid from program 'zcat < data/prices-10k-random-1.tsv.gz';
select count(*) from uk_price_paid;
SELECT * FROM chunk_query_data_node WHERE hypertable_name = 'uk_price_paid' LIMIT 5;

select hypertable_name, replication_factor from timescaledb_information.hypertables
where hypertable_name like 'uk_price_paid%' order by hypertable_name;

-- 0, 1, 2 rows
\copy uk_price_paid from stdin
\.

select count(*) from uk_price_paid;

\copy uk_price_paid from program 'zcat < data/prices-10k-random-1.tsv.gz | head -1';

select count(*) from uk_price_paid;

\copy uk_price_paid from program 'zcat < data/prices-10k-random-1.tsv.gz | head -2';

select count(*) from uk_price_paid;


select count(*), sum(price), sum(price) / count(*) from uk_price_paid;

-- Make binary file.
\copy (select * from uk_price_paid) to 'prices-10k.pgbinary' with (format binary);

-- Binary input with binary data transfer.
set timescaledb.enable_connection_binary_data = true;
set timescaledb.dist_copy_transfer_format = 'binary';
create table uk_price_paid_bin(like uk_price_paid);
select create_distributed_hypertable('uk_price_paid_bin', 'date', 'postcode2',
    chunk_time_interval => interval '90 day', replication_factor => 3);

\copy uk_price_paid_bin from 'prices-10k.pgbinary' with (format binary);
select count(*), sum(price), sum(price) / count(*) from uk_price_paid_bin;
SELECT * FROM chunk_query_data_node WHERE hypertable_name = 'uk_price_paid_bin' LIMIT 5;

SET ROLE :ROLE_CLUSTER_SUPERUSER;
SELECT * FROM alter_data_node('data_node_1', available=>false);
SET ROLE :ROLE_1;

-- Text input with explicit format option and binary data transfer. This will
-- update AN metadata for the down DN
\copy uk_price_paid_bin from program 'zcat < data/prices-10k-random-1.tsv.gz' with (format text);
select count(*), sum(price), sum(price) / count(*) from uk_price_paid_bin;
SELECT * FROM chunk_query_data_node WHERE hypertable_name = 'uk_price_paid_bin' LIMIT 5;

-- Text input with text data transfer.
\copy uk_price_paid_bin from program 'zcat < data/prices-10k-random-1.tsv.gz';
select count(*), sum(price), sum(price) / count(*) from uk_price_paid_bin;
SELECT * FROM chunk_query_data_node WHERE hypertable_name = 'uk_price_paid_bin' LIMIT 5;

SET ROLE :ROLE_CLUSTER_SUPERUSER;
SELECT * FROM alter_data_node('data_node_1', available=>true);
SET ROLE :ROLE_1;

TRUNCATE uk_price_paid;

SET timescaledb.enable_distributed_insert_with_copy=false;
INSERT INTO uk_price_paid SELECT * FROM uk_price_paid_bin;
select count(*), sum(price), sum(price) / count(*) from uk_price_paid;
SELECT * FROM chunk_query_data_node WHERE hypertable_name = 'uk_price_paid' LIMIT 5;

SET ROLE :ROLE_CLUSTER_SUPERUSER;
SELECT * FROM alter_data_node('data_node_2', available=>false);
SET ROLE :ROLE_1;
INSERT INTO uk_price_paid SELECT * FROM uk_price_paid_bin;
select count(*), sum(price), sum(price) / count(*) from uk_price_paid;
SELECT * FROM chunk_query_data_node WHERE hypertable_name = 'uk_price_paid' LIMIT 5;

SET ROLE :ROLE_CLUSTER_SUPERUSER;
SELECT * FROM alter_data_node('data_node_2', available=>true);
SET ROLE :ROLE_1;

truncate uk_price_paid;
SET timescaledb.enable_distributed_insert_with_copy=true;
INSERT INTO uk_price_paid SELECT * FROM uk_price_paid_bin;
SET ROLE :ROLE_CLUSTER_SUPERUSER;
SELECT * FROM alter_data_node('data_node_3', available=>false);
SET ROLE :ROLE_1;
INSERT INTO uk_price_paid SELECT * FROM uk_price_paid_bin;
select count(*), sum(price), sum(price) / count(*) from uk_price_paid;
SELECT * FROM chunk_query_data_node WHERE hypertable_name = 'uk_price_paid' LIMIT 5;

-- Teardown
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
DROP DATABASE :DN_DBNAME_1;
DROP DATABASE :DN_DBNAME_2;
DROP DATABASE :DN_DBNAME_3;
