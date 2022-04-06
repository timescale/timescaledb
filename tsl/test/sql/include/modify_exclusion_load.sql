-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE metrics_int2(c1 int,c2 int, c3 int, c4 int, c5 int, time int2 NOT NULL, value float, data text);
CREATE TABLE metrics_int4(c1 int,c2 int, c3 int, c4 int, c5 int, time int4 NOT NULL, value float, data text);
CREATE TABLE metrics_int8(c1 int,c2 int, c3 int, c4 int, c5 int, time int8 NOT NULL, value float, data text);
CREATE TABLE metrics_date(c1 int,c2 int, c3 int, c4 int, c5 int, time date NOT NULL, value float, data text);
CREATE TABLE metrics_timestamp(c1 int,c2 int, c3 int, c4 int, c5 int, time timestamp NOT NULL, value float, data text);
CREATE TABLE metrics_timestamptz(c1 int,c2 int, c3 int, c4 int, c5 int, time timestamptz NOT NULL, value float, data text);
CREATE TABLE metrics_space(c1 int,c2 int, c3 int, c4 int, c5 int, time timestamp NOT NULL, device text, value float, data text);

SELECT table_name FROM create_hypertable('metrics_int2','time',chunk_time_interval:=10);
SELECT table_name FROM create_hypertable('metrics_int4','time',chunk_time_interval:=10);
SELECT table_name FROM create_hypertable('metrics_int8','time',chunk_time_interval:=10);
SELECT table_name FROM create_hypertable('metrics_date','time');
SELECT table_name FROM create_hypertable('metrics_timestamp','time');
SELECT table_name FROM create_hypertable('metrics_timestamptz','time');
SELECT table_name FROM create_hypertable('metrics_space','time','device',4);

CREATE FUNCTION drop_column(text) RETURNS VOID LANGUAGE PLPGSQL AS $$
DECLARE
  tbl name;
BEGIN
  FOR tbl IN SELECT 'metrics_' || unnest(ARRAY['int2','int4','int8','date','timestamp','timestamptz','space'])
  LOOP
    EXECUTE format('ALTER TABLE %I DROP COLUMN %I;', tbl, $1);
  END LOOP;
END;
$$;

-- create 4 chunks each with different physical layout
SELECT drop_column('c1');
INSERT INTO metrics_int2(time) VALUES (0);
INSERT INTO metrics_int4(time) VALUES (0);
INSERT INTO metrics_int8(time) VALUES (0);
INSERT INTO metrics_date(time) VALUES ('2000-01-01');
INSERT INTO metrics_timestamp(time) VALUES ('2000-01-01');
INSERT INTO metrics_timestamptz(time) VALUES ('2000-01-01');
INSERT INTO metrics_space(time,device) VALUES ('2000-01-01','1'),('2000-01-01','2');

SELECT drop_column('c2');
INSERT INTO metrics_int2(time) VALUES (10);
INSERT INTO metrics_int4(time) VALUES (10);
INSERT INTO metrics_int8(time) VALUES (10);
INSERT INTO metrics_date(time) VALUES ('2001-01-01');
INSERT INTO metrics_timestamp(time) VALUES ('2001-01-01');
INSERT INTO metrics_timestamptz(time) VALUES ('2001-01-01');
INSERT INTO metrics_space(time,device) VALUES ('2001-01-01','1'),('2001-01-01','2');

SELECT drop_column('c3');
INSERT INTO metrics_int2(time) VALUES (20);
INSERT INTO metrics_int4(time) VALUES (20);
INSERT INTO metrics_int8(time) VALUES (20);
INSERT INTO metrics_date(time) VALUES ('2002-01-01');
INSERT INTO metrics_timestamp(time) VALUES ('2002-01-01');
INSERT INTO metrics_timestamptz(time) VALUES ('2002-01-01');
INSERT INTO metrics_space(time,device) VALUES ('2002-01-01','1'),('2002-01-01','2');

SELECT drop_column('c4');
INSERT INTO metrics_int2(time) VALUES (30);
INSERT INTO metrics_int4(time) VALUES (30);
INSERT INTO metrics_int8(time) VALUES (30);
INSERT INTO metrics_date(time) VALUES ('2003-01-01');
INSERT INTO metrics_timestamp(time) VALUES ('2003-01-01');
INSERT INTO metrics_timestamptz(time) VALUES ('2003-01-01');
INSERT INTO metrics_space(time,device) VALUES ('2003-01-01','1'),('2003-01-01','2');

SELECT drop_column('c5');

CREATE TABLE metrics_compressed(time timestamptz NOT NULL, device int, value float);
SELECT table_name FROM create_hypertable('metrics_compressed','time');
ALTER TABLE metrics_compressed SET (timescaledb.compress);

-- create first chunk and compress
INSERT INTO metrics_compressed VALUES ('2000-01-01',1,0.5);
SELECT count(compress_chunk(chunk)) FROM show_chunks('metrics_compressed') chunk;

-- create more chunks
INSERT INTO metrics_compressed VALUES ('2010-01-01',1,0.5),('2011-01-01',1,0.5),('2012-01-01',1,0.5);

