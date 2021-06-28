-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ECHO errors
SELECT count(*) as before_recompress_count FROM :TABLE_NAME;
SELECT count(*) as before_recompress_chunk_table_count FROM :COMP_CHUNK_NAME;

CREATE TABLE temp as select * FROM :TABLE_NAME;

WITH cdel AS
(
DELETE FROM :COMP_CHUNK_NAME
WHERE segcol in
 ( SELECT distinct segcol FROM :COMP_CHUNK_NAME WHERE _ts_meta_sequence_num = 0 )
)
INSERT INTO :COMP_CHUNK_NAME
SELECT  (unnest(_timescaledb_internal.recompress_tuples(:'CHUNK_NAME'::regclass, c))).*
FROM :COMP_CHUNK_NAME c
where segcol in
 ( SELECT distinct segcol FROM :COMP_CHUNK_NAME WHERE _ts_meta_sequence_num = 0 ) GROUP BY segcol;

--check diff before/after recompress
SELECT * FROM
(( SELECT * FROM temp) EXCEPT (select * FROM :TABLE_NAME) ) q;

SELECT * FROM
(( SELECT * FROM :TABLE_NAME) EXCEPT (select * FROM temp) ) q;

SELECT count(*) as after_recompress_count FROM :TABLE_NAME;
SELECT count(*) as after_compress_chunk_table_count FROM :COMP_CHUNK_NAME;

DROP TABLE temp;
