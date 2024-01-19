# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

###
# Test parallel operations on distinct chunks of the same hypertable
###

setup
{
  CREATE TABLE metrics (time timestamptz not null, device text, value float);

  SELECT FROM create_hypertable('metrics','time');
  ALTER TABLE metrics SET (timescaledb.compress, timescaledb.compress_segmentby = 'device');

  INSERT INTO metrics SELECT '2000-01-01', 'device1', random() FROM generate_series(1,20000);
  INSERT INTO metrics SELECT '2001-01-01', 'device2', random() FROM generate_series(1,20000);

  CREATE VIEW chunks_to_compress AS SELECT ch.id, format('%I.%I',ch.schema_name,ch.table_name) AS name from _timescaledb_catalog.chunk ch INNER JOIN _timescaledb_catalog.hypertable ht ON ht.id=ch.hypertable_id AND ht.table_name='metrics' ORDER BY ch.id;

}

teardown {
	DROP TABLE metrics;
	DROP VIEW chunks_to_compress;
}

session "s1"
setup	{
	SET LOCAL lock_timeout = '500ms';
	SET LOCAL deadlock_timeout = '300ms';
}

step "s1_begin"	{ BEGIN; }
step "s1_compress"	 {
	SELECT compress_chunk(name) IS NOT NULL AS compress FROM (SELECT name FROM chunks_to_compress ORDER BY id LIMIT 1 OFFSET 0) ch;
}
step "s1_decompress"	 {
	SELECT decompress_chunk(name) IS NOT NULL AS decompress FROM (SELECT name FROM chunks_to_compress ORDER BY id LIMIT 1 OFFSET 0) ch;
}
step "s1_commit" { COMMIT; }

session "s2"
setup	{
	SET LOCAL lock_timeout = '500ms';
	SET LOCAL deadlock_timeout = '300ms';
}

step "s2_begin"	{ BEGIN; }
step "s2_commit"   { COMMIT; }
step "s2_compress" {
	SELECT compress_chunk(name) IS NOT NULL AS compress FROM (SELECT name FROM chunks_to_compress ORDER BY id LIMIT 1 OFFSET 1) ch;
}
step "s2_decompress" {
	SELECT decompress_chunk(name) IS NOT NULL AS decompress  FROM (SELECT name FROM chunks_to_compress ORDER BY id LIMIT 1 OFFSET 1) ch;
}

permutation s1_begin s2_begin s1_compress s2_compress s2_commit s1_commit
permutation s1_begin s2_begin s1_compress s2_compress s2_decompress s1_decompress s2_commit s1_commit
permutation s1_begin s2_begin s1_compress s2_compress s2_commit s2_begin s2_decompress s1_decompress s2_commit s1_commit
permutation s1_begin s2_begin s1_compress s2_compress s1_commit s1_begin s2_decompress s1_decompress s2_commit s1_commit

