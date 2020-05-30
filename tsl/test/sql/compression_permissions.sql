-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ON_ERROR_STOP 0

CREATE TABLE conditions (
      timec        TIMESTAMPTZ       NOT NULL,
      location    TEXT              NOT NULL,
      location2    char(10)              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL
    );

select table_name from create_hypertable( 'conditions', 'timec', chunk_time_interval=>'1month'::interval);

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
alter table conditions set (timescaledb.compress, timescaledb.compress_segmentby = 'location', timescaledb.compress_orderby = 'timec');
insert into conditions
select generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 'POR', 'klick', 55, 75;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
--now owner tries and succeeds --
alter table conditions set (timescaledb.compress, timescaledb.compress_segmentby = 'location', timescaledb.compress_orderby = 'timec');
insert into conditions
select generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 'POR', 'klick', 55, 75;

--try modifying compress properties --
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
alter table conditions set (timescaledb.compress, timescaledb.compress_segmentby = 'location', timescaledb.compress_orderby = 'humidity');

--- compress_chunks and decompress_chunks fail without correct perm --
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
select  compress_chunk(ch1.schema_name|| '.' || ch1.table_name)
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id and ht.table_name like 'conditions' and ch1.compressed_chunk_id IS NULL;
select decompress_chunk(ch1.schema_name|| '.' || ch1.table_name)
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id and ht.table_name like 'conditions';
select add_compress_chunks_policy('conditions', '1day'::interval);

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
select add_compress_chunks_policy('conditions', '1day'::interval);
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
--try dropping policy
select remove_compress_chunks_policy('conditions', true);

--Tests for GRANTS.
-- as owner grant select , compress chunk and check SELECT works
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
GRANT SELECT on conditions to :ROLE_DEFAULT_PERM_USER_2;
select count(*) from
(select  compress_chunk(ch1.schema_name|| '.' || ch1.table_name)
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id and ht.table_name like 'conditions' and ch1.compressed_chunk_id IS NULL ) as subq;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
select count(*) from conditions;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
GRANT INSERT on conditions to :ROLE_DEFAULT_PERM_USER_2;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
insert into conditions
select '2019-04-01 00:00+0'::timestamp with time zone, 'NYC', 'klick', 55, 75;
select count(*) from conditions;
update conditions
set location = 'SFO'
where timec = '2019-04-01 00:00+0'::timestamp with time zone; 

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
GRANT UPDATE, DELETE on conditions to :ROLE_DEFAULT_PERM_USER_2;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
select location from conditions where timec = '2019-04-01 00:00+0';
--NOTE constraint exclusion excludes the other chunks here
--otherwise update would fail
update conditions
set location = 'SFO'
where timec = '2019-04-01 00:00+0'::timestamp with time zone;
select location from conditions where timec = '2019-04-01 00:00+0';
--update expected to fail as executor touches all chunks
update conditions
set location = 'PNC'
where location = 'SFO';

delete from conditions
where timec = '2019-04-01 00:00+0'::timestamp with time zone;
select location from conditions where timec = '2019-04-01 00:00+0';

--should fail.
SELECT ch1.schema_name|| '.' || ch1.table_name as "COMPT_NAME"
FROM _timescaledb_catalog.hypertable ch1, _timescaledb_catalog.hypertable ht where ht.table_name like 'conditions' and ht.compressed_hypertable_id = ch1.id
\gset

select count(*) from :COMPT_NAME;

CREATE VIEW v2 as select * from conditions;
select count(*) from v2;
--should fail after revoking permissions 
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
REVOKE SELECT on conditions FROM :ROLE_DEFAULT_PERM_USER_2;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
select count(*) from v2;
