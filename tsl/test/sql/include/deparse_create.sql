-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Lets create some tabels that we will try to deparse and recreate

\c :TEST_DBNAME :ROLE_SUPERUSER

SET ROLE :ROLE_DEFAULT_PERM_USER;

CREATE TABLE table1(time TIMESTAMP, v FLOAT8, c CHAR(10), x NUMERIC(10,4), i interval hour to minute);

CREATE TABLE table2(time TIMESTAMP NOT NULL, v FLOAT8[], d TEXT COLLATE "POSIX", num INT DEFAULT 100);

CREATE TABLE table3(time TIMESTAMP PRIMARY KEY, v FLOAT8[][], num INT CHECK (num > 0), d INT UNIQUE, CONSTRAINT validate_num_and_d CHECK ( num > d));

CREATE TABLE table4(t TIMESTAMP , d INT, PRIMARY KEY (t, d));

CREATE TABLE ref_table(id INT PRIMARY KEY, d TEXT);

CREATE TABLE table5(t TIMESTAMP PRIMARY KEY, v FLOAT8, d INT REFERENCES ref_table ON DELETE CASCADE);

CREATE SEQUENCE my_seq;

CREATE UNLOGGED TABLE table6(id INT NOT NULL DEFAULT nextval('my_seq'), t TEXT);

CREATE INDEX ON table6 USING BTREE (t);

RESET ROLE;

CREATE TYPE device_status AS ENUM ('OFF', 'ON', 'BROKEN');

CREATE SCHEMA myschema AUTHORIZATION :ROLE_DEFAULT_PERM_USER;;

SET ROLE :ROLE_DEFAULT_PERM_USER;

CREATE TABLE table7(t TIMESTAMP, v INT);

CREATE TABLE table8(id INT, status device_status);

CREATE OR REPLACE FUNCTION empty_trigger_func()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
END
$BODY$;

CREATE TRIGGER test_trigger BEFORE UPDATE OR DELETE ON table8
FOR EACH STATEMENT EXECUTE FUNCTION empty_trigger_func();

CREATE RULE notify_me AS ON UPDATE TO table8 DO ALSO NOTIFY table8;

CREATE TABLE table9(c CIRCLE, EXCLUDE USING gist (c WITH &&));

CREATE TABLE myschema.table10(t TIMESTAMP);

