-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- these table definitions have been adjusted from
-- table defintions of the postgres test suite

CREATE TABLE INT2_TBL(f1 int2, ts timestamptz NOT NULL DEFAULT '2000-01-01');
SELECT table_name FROM create_hypertable('int2_tbl','ts');

INSERT INTO INT2_TBL(f1) VALUES ('0   ');
INSERT INTO INT2_TBL(f1) VALUES ('  1234 ');
INSERT INTO INT2_TBL(f1) VALUES ('    -1234');
-- largest and smallest values
INSERT INTO INT2_TBL(f1) VALUES ('32767');
INSERT INTO INT2_TBL(f1) VALUES ('-32767');


CREATE TABLE INT4_TBL(f1 int4, ts timestamptz NOT NULL DEFAULT '2000-01-01');
SELECT table_name FROM create_hypertable('int4_tbl','ts');

INSERT INTO INT4_TBL(f1) VALUES ('   0  ');
INSERT INTO INT4_TBL(f1) VALUES ('123456     ');
INSERT INTO INT4_TBL(f1) VALUES ('    -123456');
-- largest and smallest values
INSERT INTO INT4_TBL(f1) VALUES ('2147483647');
INSERT INTO INT4_TBL(f1) VALUES ('-2147483647');


CREATE TABLE INT8_TBL(q1 int8, q2 int8, ts timestamptz NOT NULL DEFAULT '2000-01-01');
SELECT table_name FROM create_hypertable('int8_tbl','ts');

INSERT INTO INT8_TBL VALUES('  123   ','  456');
INSERT INTO INT8_TBL VALUES('123   ','4567890123456789');
INSERT INTO INT8_TBL VALUES('4567890123456789','123');
INSERT INTO INT8_TBL VALUES(+4567890123456789,'4567890123456789');
INSERT INTO INT8_TBL VALUES('+4567890123456789','-4567890123456789');


CREATE TABLE FLOAT8_TBL(f1 float8, ts timestamptz NOT NULL DEFAULT '2000-01-01');
SELECT table_name FROM create_hypertable('float8_tbl','ts');

INSERT INTO FLOAT8_TBL(f1) VALUES ('    0.0   ');
INSERT INTO FLOAT8_TBL(f1) VALUES ('1004.30  ');
INSERT INTO FLOAT8_TBL(f1) VALUES ('   -34.84');
INSERT INTO FLOAT8_TBL(f1) VALUES ('1.2345678901234e+200');
INSERT INTO FLOAT8_TBL(f1) VALUES ('1.2345678901234e-200');

CREATE TABLE TEXT_TBL (f1 text, ts timestamptz NOT NULL DEFAULT '2000-01-01');
SELECT table_name FROM create_hypertable('text_tbl','ts');

INSERT INTO TEXT_TBL VALUES ('doh!');
INSERT INTO TEXT_TBL VALUES ('hi de ho neighbor');

CREATE TABLE a (aa TEXT);
CREATE TABLE b (bb TEXT) INHERITS (a);
CREATE TABLE c (cc TEXT) INHERITS (a);
CREATE TABLE d (dd TEXT) INHERITS (b,c,a);

INSERT INTO a(aa) VALUES('aaa');
INSERT INTO a(aa) VALUES('aaaa');
INSERT INTO a(aa) VALUES('aaaaa');
INSERT INTO a(aa) VALUES('aaaaaa');
INSERT INTO a(aa) VALUES('aaaaaaa');
INSERT INTO a(aa) VALUES('aaaaaaaa');

INSERT INTO b(aa) VALUES('bbb');
INSERT INTO b(aa) VALUES('bbbb');
INSERT INTO b(aa) VALUES('bbbbb');
INSERT INTO b(aa) VALUES('bbbbbb');
INSERT INTO b(aa) VALUES('bbbbbbb');
INSERT INTO b(aa) VALUES('bbbbbbbb');

INSERT INTO c(aa) VALUES('ccc');
INSERT INTO c(aa) VALUES('cccc');
INSERT INTO c(aa) VALUES('ccccc');
INSERT INTO c(aa) VALUES('cccccc');
INSERT INTO c(aa) VALUES('ccccccc');
INSERT INTO c(aa) VALUES('cccccccc');

INSERT INTO d(aa) VALUES('ddd');
INSERT INTO d(aa) VALUES('dddd');
INSERT INTO d(aa) VALUES('ddddd');
INSERT INTO d(aa) VALUES('dddddd');
INSERT INTO d(aa) VALUES('ddddddd');
INSERT INTO d(aa) VALUES('dddddddd');

CREATE TABLE onek (
  unique1   int4,
  unique2   int4,
  two     int4,
  four    int4,
  ten     int4,
  twenty    int4,
  hundred   int4,
  thousand  int4,
  twothousand int4,
  fivethous int4,
  tenthous  int4,
  odd     int4,
  even    int4,
  stringu1  name,
  stringu2  name,
  string4   name
);

SELECT table_name FROM create_hypertable('onek','unique2',chunk_time_interval:=1000);
\copy onek FROM 'data/onek.data'

CREATE TABLE tenk1 (
  unique1   int4,
  unique2   int4,
  two     int4,
  four    int4,
  ten     int4,
  twenty    int4,
  hundred   int4,
  thousand  int4,
  twothousand int4,
  fivethous int4,
  tenthous  int4,
  odd     int4,
  even    int4,
  stringu1  name,
  stringu2  name,
  string4   name
);

SELECT table_name FROM create_hypertable('tenk1','unique2',chunk_time_interval:=1000);
\copy tenk1 FROM 'data/tenk.data'

CREATE TABLE tenk2 (
  unique1   int4,
  unique2   int4,
  two     int4,
  four    int4,
  ten     int4,
  twenty    int4,
  hundred   int4,
  thousand  int4,
  twothousand int4,
  fivethous int4,
  tenthous  int4,
  odd     int4,
  even    int4,
  stringu1  name,
  stringu2  name,
  string4   name
);

SELECT table_name FROM create_hypertable('tenk2','unique2',chunk_time_interval:=1000);
INSERT INTO tenk2 SELECT * FROM tenk1;

