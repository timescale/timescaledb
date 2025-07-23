-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--install necessary functions for tests
\c :TEST_DBNAME :ROLE_SUPERUSER
\ir include/compression_utils.sql
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

SET timescaledb.enable_uuid_compression = on;
CREATE TABLE t (ts int, u uuid);
SELECT create_hypertable('t', 'ts', chunk_time_interval => 500);
ALTER TABLE t SET (timescaledb.compress, timescaledb.compress_orderby = 'ts');
INSERT INTO t SELECT g AS ts, NULL AS uuid FROM generate_series(1, 3000) g;

-- The first hundred elements are generated with the Go uuid v7 routines
CREATE TABLE uuids (i int, u uuid);
INSERT INTO uuids (i, u) VALUES
(1, '0197a7a9-b48b-7c70-be05-e376afc66ee1'), (2, '0197a7a9-b48b-7c71-92cb-eb724822bb0f'), (3, '0197a7a9-b48b-7c72-bd57-49f981f064fd'), (4, '0197a7a9-b48b-7c73-b521-91172c2e770a'),
(5, '0197a7a9-b48b-7c74-a2a4-dcdbce635d11'), (6, '0197a7a9-b48b-7c75-a810-8acf630e634f'), (7, '0197a7a9-b48b-7c76-b616-69e64a802b5c'), (8, '0197a7a9-b48b-7c77-b54f-c5f3d64d68d0'),
(9, '0197a7a9-b48b-7c78-ab14-78b3dd81dbbc'), (10, '0197a7a9-b48b-7c79-92c7-7dde3bea6252'), (11, '0197a7a9-b48b-7c7a-8d9e-5afc3bf15234'), (12, '0197a7a9-b48b-7c7b-bc49-7150f16d8d63'),
(13, '0197a7a9-b48b-7c7c-aa7a-60d47bf04ff8'), (14, '0197a7a9-b48b-7c7d-8cfe-9503ed9bb1c9'), (15, '0197a7a9-b48b-7c7e-9ebb-acf63f5b625e'), (16, '0197a7a9-b48b-7c7f-a0c1-ba4adf950a2a'),
(17, '0197a7a9-b48b-7c80-a534-4eda33d89b41'), (18, '0197a7a9-b48b-7c81-abaf-e4d27888f6ea'), (19, '0197a7a9-b48b-7c82-a07c-bb5278039b67'), (20, '0197a7a9-b48b-7c83-9df3-2826632fcb42'),
(21, '0197a7a9-b48b-7c84-8588-dc4e6f10a5be'), (22, '0197a7a9-b48b-7c85-98a4-5ba69598ba88'), (23, '0197a7a9-b48b-7c86-9a96-69906846edf4'), (24, '0197a7a9-b48b-7c87-843a-d0c409e538d2'),
(25, '0197a7a9-b48b-7c88-a9c9-8e03283979dd'), (26, '0197a7a9-b48c-7c88-b962-5f38a5f5bb19'), (27, '0197a7a9-b48c-7c89-ad12-19c425cfe319'), (28, '0197a7a9-b48c-7c8a-9652-43b070f806b6'),
(29, '0197a7a9-b48c-7c8b-8b95-2e4b27b7c359'), (30, '0197a7a9-b48c-7c8c-9230-5a1b6a126d4e'), (31, '0197a7a9-b48c-7c8d-98e9-3622fe1418ae'), (32, '0197a7a9-b48c-7c8e-b262-e91dcf84f985'),
(33, '0197a7a9-b48c-7c8f-90c0-1036d19e438e'), (34, '0197a7a9-b48c-7c90-9fcb-f092518ae1e6'), (35, '0197a7a9-b48c-7c91-bf68-433e366f751d'), (36, '0197a7a9-b48c-7c92-95b6-82cb29498e5a'),
(37, '0197a7a9-b48c-7c93-9397-6ebbb9d4194d'), (38, '0197a7a9-b48c-7c94-8484-f47122e2dea3'), (39, '0197a7a9-b48c-7c95-a6e5-fe8d062f4e3c'), (40, '0197a7a9-b48c-7c96-914c-690b7930262f'),
(41, '0197a7a9-b48c-7c97-ac2b-473d61e0c396'), (42, '0197a7a9-b48c-7c98-93bd-ca093b30f6e8'), (43, '0197a7a9-b48c-7c99-b906-7fa2180536d3'), (44, '0197a7a9-b48c-7c9a-a090-fe01428ccefc'),
(45, '0197a7a9-b48c-7c9b-9319-de9dd58deeee'), (46, '0197a7a9-b48c-7c9c-a9d4-ed6f3e6a41b7'), (47, '0197a7a9-b48c-7c9d-8036-4141e0780323'), (48, '0197a7a9-b48c-7c9e-bfbe-f00eb49ed7f2'),
(49, '0197a7a9-b48c-7c9f-8ffe-71cf00a0c0c0'), (50, '0197a7a9-b48c-7ca0-822f-95ced2f95702'), (51, '0197a7a9-b48c-7ca1-8c8a-66582aec95fa'), (52, '0197a7a9-b48c-7ca2-95c3-fe80362a2251'),
(53, '0197a7a9-b48c-7ca3-855f-f681b254a8c8'), (54, '0197a7a9-b48c-7ca4-856b-a562eca93c3f'), (55, '0197a7a9-b48c-7ca5-a30e-37c247fb4c46'), (56, '0197a7a9-b48c-7ca6-9e78-a148d54a44ac'),
(57, '0197a7a9-b48c-7ca7-badb-4b650bf5bf5f'), (58, '0197a7a9-b48c-7ca8-8275-a8590869ef13'), (59, '0197a7a9-b48c-7ca9-b328-b54c3901223c'), (60, '0197a7a9-b48c-7caa-a15d-f5564e4e552c'),
(61, '0197a7a9-b48c-7cab-a4ac-017259746322'), (62, '0197a7a9-b48c-7cac-bda5-74ef12abd6b8'), (63, '0197a7a9-b48c-7cad-a3cd-0e4c93eaba80'), (64, '0197a7a9-b48c-7cae-9667-de4a226418df'),
(65, '0197a7a9-b48c-7caf-8aa2-067619170f32'), (66, '0197a7a9-b48c-7cb0-ba8c-91d9e8920845'), (67, '0197a7a9-b48c-7cb1-9681-a62bfffe9237'), (68, '0197a7a9-b48c-7cb2-b78b-037e5ee26ff6'),
(69, '0197a7a9-b48c-7cb3-ac27-e24382445188'), (70, '0197a7a9-b48b-7c7d-8cfe-9503ed9bb1c9'), (71, '0197a7a9-b48b-7c7e-9ebb-acf63f5b625e'), (72, '0197a7a9-b48b-7c7f-a0c1-ba4adf950a2a'),
(73, '0197a7a9-b48b-7c80-a534-4eda33d89b41'), (74, '0197a7a9-b48b-7c81-abaf-e4d27888f6ea'), (75, '0197a7a9-b48b-7c82-a07c-bb5278039b67'), (76, '0197a7a9-b48b-7c83-9df3-2826632fcb42'),
(77, '0197a7a9-b48b-7c84-8588-dc4e6f10a5be'), (78, '0197a7a9-b48b-7c85-98a4-5ba69598ba88'), (79, '0197a7a9-b48b-7c86-9a96-69906846edf4'), (80, '0197a7a9-b48b-7c87-843a-d0c409e538d2'),
(81, '0197a7a9-b48b-7c88-a9c9-8e03283979dd'), (82, '0197a7a9-b48c-7c88-b962-5f38a5f5bb19'), (83, '0197a7a9-b48c-7c89-ad12-19c425cfe319'), (84, '0197a7a9-b48c-7c8a-9652-43b070f806b6'),
(85, '0197a7a9-b48c-7c8b-8b95-2e4b27b7c359'), (86, '0197a7a9-b48c-7c8c-9230-5a1b6a126d4e'), (87, '0197a7a9-b48c-7c8d-98e9-3622fe1418ae'), (88, '0197a7a9-b48c-7c8e-b262-e91dcf84f985'),
(89, '0197a7a9-b48c-7c8f-90c0-1036d19e438e'), (90, '0197a7a9-b48c-7c90-9fcb-f092518ae1e6'), (91, '0197a7a9-b48c-7c91-bf68-433e366f751d'), (92, '0197a7a9-b48c-7c92-95b6-82cb29498e5a'),
(93, '0197a7a9-b48c-7c93-9397-6ebbb9d4194d'), (94, '0197a7a9-b48c-7c94-8484-f47122e2dea3'), (95, '0197a7a9-b48c-7c95-a6e5-fe8d062f4e3c'), (96, '0197a7a9-b48c-7c96-914c-690b7930262f'),
(97, '0197a7a9-b48c-7c97-ac2b-473d61e0c396'), (98, '0197a7a9-b48c-7c98-93bd-ca093b30f6e8'), (99, '0197a7a9-b48c-7c99-b906-7fa2180536d3'), (100, '0197a7a9-b48c-7c9a-a090-fe01428ccefc');

-- set data to be compressed with the uuid compressor
UPDATE t SET u = (select u from uuids where i = ts) where ts <= 100;

-- set data to be compressed with the dictionary compressor
UPDATE t SET u = (select u from uuids where i = ts%100) where ts > 1000 and ts <= 2000;

-- add random UUID v4s to the data
UPDATE t SET u = gen_random_uuid() where ts > 2000;

-- compress the data
SELECT compress_chunk(show_chunks('t'));

-- decompress the data
SELECT decompress_chunk(show_chunks('t'));

-- disable uuid compression and compress the data again
SET timescaledb.enable_uuid_compression = off;
SELECT compress_chunk(show_chunks('t'));

CREATE TABLE mixed_compressed (ts int, u uuid);
SELECT create_hypertable('mixed_compressed', 'ts', chunk_time_interval => 500);
ALTER TABLE mixed_compressed SET (timescaledb.compress, timescaledb.compress_orderby = 'ts');

-- the new UUID compression is OFF at this point, so we use the original method
-- add the base uuids to the data and compress it
INSERT INTO mixed_compressed SELECT i, u FROM uuids;
INSERT INTO mixed_compressed SELECT i+100 as i, NULL as u FROM uuids;

-- compress the data
SELECT compress_chunk(show_chunks('mixed_compressed'));

-- enable uuid compression and compress the data again
-- and add the base uuids to the data again
SET timescaledb.enable_uuid_compression = on;
INSERT INTO mixed_compressed SELECT i+1000 as i, u FROM uuids;
INSERT INTO mixed_compressed SELECT i+1100 as i, NULL as u FROM uuids;

-- compress the data
SELECT compress_chunk(show_chunks('mixed_compressed'));

-- now turn it off again and add the whole data twice
SET timescaledb.enable_uuid_compression = off;
INSERT INTO mixed_compressed SELECT i+2000, u FROM uuids;
INSERT INTO mixed_compressed SELECT i+2200, u FROM uuids;
INSERT INTO mixed_compressed SELECT i+2300 as i, NULL as u FROM uuids;

-- compress the data again
SELECT compress_chunk(show_chunks('mixed_compressed'));

-- turn it on again and do the same
SET timescaledb.enable_uuid_compression = off;
INSERT INTO mixed_compressed SELECT i+3000, u FROM uuids;
INSERT INTO mixed_compressed SELECT i+3200, u FROM uuids;
INSERT INTO mixed_compressed SELECT i+3300 as i, NULL as u FROM uuids;

-- compress the data again
SELECT compress_chunk(show_chunks('mixed_compressed'));

-- check the compression algorithm for the compressed chunks
CREATE TABLE compressed_chunks AS
SELECT
	format('%I.%I', comp.schema_name, comp.table_name)::regclass as compressed_chunk,
	ccs.compressed_heap_size,
	ccs.compressed_toast_size,
	ccs.compressed_index_size,
	ccs.numrows_pre_compression,
	ccs.numrows_post_compression
FROM
	show_chunks('mixed_compressed') c
	INNER JOIN _timescaledb_catalog.chunk cat
		ON (c = format('%I.%I', cat.schema_name, cat.table_name)::regclass)
	INNER JOIN _timescaledb_catalog.chunk comp
		ON (cat.compressed_chunk_id = comp.id)
	INNER JOIN _timescaledb_catalog.compression_chunk_size ccs
		ON (comp.id = ccs.compressed_chunk_id);

CREATE TABLE compression_info (compressed_chunk regclass, result text, compressed_size int, num_rows int);

DO $$
DECLARE
	table_ref regclass;
BEGIN
	FOR table_ref IN
		SELECT compressed_chunk as table_ref FROM compressed_chunks
	LOOP
		EXECUTE format(
			'INSERT INTO compression_info (
				SELECT
					%L::regclass as compressed_chunk,
					(_timescaledb_functions.compressed_data_info(u))::text as result,
					sum(pg_column_size(u)::int) as compressed_size,
					count(*) as num_rows
				FROM %s
				GROUP BY 1,2)',
			table_ref, table_ref
		);
	END LOOP;
END;
$$;

SELECT
	ci.*,
	ccs.compressed_toast_size,
	ccs.numrows_pre_compression,
	ccs.numrows_post_compression
FROM
	compression_info ci
	INNER JOIN compressed_chunks ccs
		ON (ci.compressed_chunk = ccs.compressed_chunk)
ORDER BY
	1,2,3;

-- -------------------------------
-- Test the new UUID v7 functions
-- -------------------------------

-- The next 3 items are generated with the new functions
BEGIN;
INSERT INTO uuids VALUES (101, _timescaledb_functions.uuid_v7_from_timestamptz('2025-07-02:03:04:05'::timestamptz));
INSERT INTO uuids VALUES (102, _timescaledb_functions.uuid_v7_from_timestamptz('2029-01-02:03:04:05'::timestamptz));
INSERT INTO uuids VALUES (103, _timescaledb_functions.uuid_v7_from_timestamptz('2059-02-03:04:05:06'::timestamptz));
INSERT INTO uuids VALUES (104, _timescaledb_functions.uuid_v7_from_timestamptz('2100-07-08:09:10:11'::timestamptz));
COMMIT;

SELECT
  to_char(_timescaledb_functions.timestamptz_from_uuid_v7(u), 'YYYY-MM-DD:HH24:MI:SS'),
  _timescaledb_functions.uuid_version(u),
  count(*)
FROM
  uuids
GROUP BY 1,2
ORDER BY 1,2;

-- Add some v4 timestamps for sanity check
BEGIN;
INSERT INTO uuids VALUES (105, _timescaledb_functions.generate_uuid());
INSERT INTO uuids VALUES (106, _timescaledb_functions.generate_uuid());
INSERT INTO uuids VALUES (107, _timescaledb_functions.generate_uuid());

-- Add some random v7 timestamps too
INSERT INTO uuids VALUES (108, _timescaledb_functions.generate_uuid_v7());
INSERT INTO uuids VALUES (109, _timescaledb_functions.generate_uuid_v7());
INSERT INTO uuids VALUES (110, _timescaledb_functions.generate_uuid_v7());
COMMIT;

-- There is a test flakyness that I want to debug with this:
SELECT
  i,
  _timescaledb_functions.uuid_version(u)
FROM
  uuids
WHERE
  i > 100
ORDER BY 1;

-- The version numbers should make sense
SELECT
  _timescaledb_functions.uuid_version(u),
  count(*)
FROM
  uuids
WHERE
-- The filters are here just to test, they don't blow up with random data from v4
  _timescaledb_functions.timestamptz_from_uuid_v7(u) > '2000-01-01:01:01:01'::timestamptz
GROUP BY 1
ORDER BY 1;

-- Sub ms timestamp test:
--   generate all microseconds timestamps between the two dates below and
--   generate a UUID v7 based on the timestamp and
--   extract the timestamp from the UUID and
--   compare the timestamp to the original one
--   1 microsecond difference is allowed due to scaling of decimals to binaries
--
CREATE TABLE subms AS SELECT _timescaledb_functions.uuid_v7_from_timestamptz(x) u, x ts
FROM
  generate_series('2025-01-01:00:00:00'::timestamptz, '2025-01-01:00:00:03'::timestamptz, '1 microsecond'::interval) x;

SELECT u, ts, ts2
FROM
  (
    SELECT
      u, ts, _timescaledb_functions.timestamptz_from_uuid_v7(u) ts2
    FROM
      subms
  ) x
WHERE
  (x.ts - x.ts2) > '00:00:00.000001' OR (x.ts - x.ts2) < '-00:00:00.000001';

-- Cleanup
DROP TABLE t;
DROP TABLE subms;
DROP TABLE uuids;
DROP TABLE mixed_compressed;
DROP TABLE compressed_chunks;
DROP TABLE compression_info;
RESET timescaledb.enable_uuid_compression;
