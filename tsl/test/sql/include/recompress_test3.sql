-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ECHO errors
SELECT count(*) as before_recompress_count FROM :TABLE_NAME;
SELECT count(*) as before_recompress_chunk_table_count FROM :COMP_CHUNK_NAME;

CREATE TABLE temp as select * FROM :TABLE_NAME;

SELECT recompress_chunk_tuples(:'CHUNK_NAME'::regclass);

--check diff before/after recompress
SELECT * FROM
(( SELECT * FROM temp) EXCEPT (select * FROM :TABLE_NAME) ) q;

SELECT * FROM
(( SELECT * FROM :TABLE_NAME) EXCEPT (select * FROM temp) ) q;

SELECT count(*) as after_recompress_count FROM :TABLE_NAME;
SELECT count(*) as after_compress_chunk_table_count FROM :COMP_CHUNK_NAME;

DROP TABLE temp;
