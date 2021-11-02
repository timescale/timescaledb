-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Need to be super user to create extension and add data nodes
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

\unset ECHO
SET client_min_messages TO ERROR;
\o /dev/null
\ir include/debugsupport.sql
\ir include/filter_exec.sql
\ir include/remote_exec.sql
\o
RESET client_min_messages;
\set ECHO all

\set DATA_NODE_1 :TEST_DBNAME _1
\set DATA_NODE_2 :TEST_DBNAME _2
\set DATA_NODE_3 :TEST_DBNAME _3
\set TABLESPACE_1 :TEST_DBNAME _1
\set TABLESPACE_2 :TEST_DBNAME _2
SELECT
    test.make_tablespace_path(:'TEST_TABLESPACE1_PREFIX', :'TEST_DBNAME') AS spc1path,
    test.make_tablespace_path(:'TEST_TABLESPACE2_PREFIX', :'TEST_DBNAME') AS spc2path
\gset

SELECT (add_data_node (name, host => 'localhost', DATABASE => name)).*
FROM (VALUES (:'DATA_NODE_1'), (:'DATA_NODE_2'), (:'DATA_NODE_3')) v (name);

GRANT USAGE ON FOREIGN SERVER :DATA_NODE_1, :DATA_NODE_2, :DATA_NODE_3 TO PUBLIC;

-- Import testsupport.sql file to data nodes
\unset ECHO
\o /dev/null
\c :DATA_NODE_1
SET client_min_messages TO ERROR;
\ir :TEST_SUPPORT_FILE
\c :DATA_NODE_2
SET client_min_messages TO ERROR;
\ir :TEST_SUPPORT_FILE
\c :DATA_NODE_3
SET client_min_messages TO ERROR;
\ir :TEST_SUPPORT_FILE
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
\o
SET client_min_messages TO NOTICE;
\set ECHO all


-- Test pushdown of stable functions to data nodes
create table words(ts timestamp, text text);
select create_distributed_hypertable('words', 'ts');
insert into words values ('2021-01-01 01:01:01', 'testing'),
	('2022-02-02 02:02:02', 'some other words');
	
-- The normal stuff, to_tsvector is evaluated on the access node. We'll check
-- different text search configurations and make sure that they give different
-- results.
set timescaledb.pushdown_functions = 'whitelisted';
set default_text_search_config = 'english';

select to_tsvector(text), to_tsvector('testing')
from words where to_tsvector(text) != to_tsvector('testing');

explain (analyze, verbose, costs off, timing off, summary off)
select to_tsvector(text), to_tsvector('testing')
from words where to_tsvector(text) != to_tsvector('testing');

-- A different stemming configuration.
set default_text_search_config = 'simple';

select to_tsvector(text), to_tsvector('testing')
from words where to_tsvector(text) != to_tsvector('testing');

explain (analyze, verbose, costs off, timing off, summary off)
select to_tsvector(text), to_tsvector('testing')
from words where to_tsvector(text) != to_tsvector('testing');

-- Time for the weirdly broken stuff. This configuration will evaluate the
-- to_tsvector('testing') on the access node, giving 'testing':1,
-- and to_tsvector(text) on the data nodes, giving 'test':1. So this row will
-- be returned from the data node. The access node won't check the filter again.
-- To fix, we should start shipping the changed settings to the data nodes.
-- For the time being, either configure the data nodes manually or limit the
-- pushdown of functions to the whitelist (timezone stuff).
set default_text_search_config = 'simple';
set timescaledb.pushdown_functions = 'stable';

select to_tsvector(text), to_tsvector('testing')
from words where to_tsvector(text) != to_tsvector('testing');

explain (analyze, verbose, costs off, timing off, summary off)
select to_tsvector(text), to_tsvector('testing')
from words where to_tsvector(text) != to_tsvector('testing');


-- Clean up.
reset default_text_search_config;
reset timescaledb.pushdown_functions;
drop table words;