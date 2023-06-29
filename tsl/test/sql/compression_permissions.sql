-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ON_ERROR_STOP 0

CREATE VIEW hypertable_details AS
WITH
  names AS (SELECT * FROM pg_class cl JOIN pg_namespace ns ON ns.oid = relnamespace)
SELECT (SELECT format('%I.%I', ht.schema_name, ht.table_name) FROM names
	WHERE ht.schema_name = nspname AND ht.table_name = relname) AS hypertable,
       (SELECT relacl FROM names
	WHERE ht.schema_name = nspname AND ht.table_name = relname) AS hypertable_acl,
       (SELECT format('%I.%I', ct.schema_name, ct.table_name) FROM names
	WHERE ct.schema_name = nspname AND ct.table_name = relname) AS compressed,
       (SELECT relacl FROM names
	WHERE ct.schema_name = nspname AND ct.table_name = relname) AS compressed_acl
  FROM _timescaledb_catalog.hypertable ht
  LEFT JOIN _timescaledb_catalog.hypertable ct ON ht.compressed_hypertable_id = ct.id;

CREATE VIEW chunk_details AS
WITH
  names AS (SELECT * FROM pg_class cl JOIN pg_namespace ns ON ns.oid = relnamespace)
SELECT (SELECT format('%I.%I', ht.schema_name, ht.table_name) FROM names
	WHERE ht.schema_name = nspname AND ht.table_name = relname) AS hypertable,
       (SELECT relacl FROM names
	WHERE ht.schema_name = nspname AND ht.table_name = relname) AS hypertable_acl,
       (SELECT format('%I.%I', ch.schema_name, ch.table_name) FROM names
	WHERE ch.schema_name = nspname AND ch.table_name = relname) AS chunk,
       (SELECT relacl FROM names
	WHERE ch.schema_name = nspname AND ch.table_name = relname) AS chunk_acl
  FROM _timescaledb_catalog.hypertable ht
  JOIN _timescaledb_catalog.chunk ch ON ch.hypertable_id = ht.id;

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
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id and ht.table_name like 'conditions' and ch1.status & 1 = 0;
select decompress_chunk(ch1.schema_name|| '.' || ch1.table_name)
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id and ht.table_name like 'conditions';
select add_compression_policy('conditions', '1day'::interval);

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
select add_compression_policy('conditions', '1day'::interval);
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
--try dropping policy
select remove_compression_policy('conditions', true);

--Tests for GRANTS.
-- as owner grant select , compress chunk and check SELECT works
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
GRANT SELECT on conditions to :ROLE_DEFAULT_PERM_USER_2;
select count(*) from
(select  compress_chunk(ch1.schema_name|| '.' || ch1.table_name)
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id and ht.table_name like 'conditions' and ch1.status & 1 = 0 ) as subq;
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

CREATE VIEW v2 as select * from conditions;
select count(*) from v2;
--should fail after revoking permissions
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
REVOKE SELECT on conditions FROM :ROLE_DEFAULT_PERM_USER_2;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
select count(*) from v2;

\c :TEST_DBNAME :ROLE_SUPERUSER
DROP TABLE conditions CASCADE;

-- Testing that permissions propagate to compressed hypertables and to
-- compressed chunks.
-- Table is created by superuser

CREATE TABLE conditions (
      timec TIMESTAMPTZ NOT NULL,
      location TEXT NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity DOUBLE PRECISION  NULL
    );

SELECT table_name FROM create_hypertable( 'conditions', 'timec',
       chunk_time_interval => '1 week'::interval);

ALTER TABLE conditions SET (
      timescaledb.compress,
      timescaledb.compress_segmentby = 'location',
      timescaledb.compress_orderby = 'timec'
);

INSERT INTO conditions
SELECT generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 'POR', 55, 75;

SELECT compress_chunk(show_chunks('conditions'));

-- Check that ACL propagates to compressed hypertable.  We could prune
-- the listing by only selecting chunks where the ACL does not match
-- the hypertable ACL, but for now we list all to make debugging easy.

\x on
SELECT * FROM hypertable_details WHERE hypertable = 'public.conditions';

SELECT htd.hypertable, htd.hypertable_acl, chunk, chunk_acl
  FROM chunk_details chd JOIN hypertable_details htd ON chd.hypertable = htd.compressed
ORDER BY hypertable, chunk;

GRANT SELECT ON conditions TO :ROLE_DEFAULT_PERM_USER;

SELECT * FROM hypertable_details WHERE hypertable = 'public.conditions';

SELECT htd.hypertable, htd.hypertable_acl, chunk, chunk_acl
  FROM chunk_details chd JOIN hypertable_details htd ON chd.hypertable = htd.compressed
ORDER BY hypertable, chunk;

-- Add some new data and compress the chunks. The chunks should get
-- the permissions of the hypertable. We pick a start date to make
-- sure that we are not inserting into an already compressed chunk.
INSERT INTO conditions
SELECT generate_series('2019-01-07 00:00'::timestamp, '2019-02-07 00:00'::timestamp, '1 day'), 'XYZ', 47, 11;

SELECT compress_chunk(show_chunks('conditions', newer_than => '2019-01-01'));

SELECT htd.hypertable, htd.hypertable_acl, chunk, chunk_acl
  FROM chunk_details chd JOIN hypertable_details htd ON chd.hypertable = htd.compressed
ORDER BY hypertable, chunk;
\x off

--TEST user that has insert permission can insert into a compressed chunk
GRANT INSERT ON conditions TO :ROLE_DEFAULT_PERM_USER;
SELECT count(*) FROM conditions;
SELECT count(*) FROM ( SELECT show_chunks('conditions'))q;
SET ROLE :ROLE_DEFAULT_PERM_USER;
--insert into a compressed chunk --
INSERT INTO conditions VALUES( '2018-12-02 00:00'::timestamp, 'NYC', 75, 95);
SELECT count(*) FROM conditions;
SELECT count(*) FROM ( SELECT show_chunks('conditions'))q;
