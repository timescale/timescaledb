--
-- Basic tests
--

-- Create a replication slot
SELECT slot_name FROM pg_create_logical_replication_slot('custom_slot', 'decoder_raw');

-- DEFAULT case with PRIMARY KEY
CREATE TABLE aa (a int primary key, b text NOT NULL);
INSERT INTO aa VALUES (1, 'aa'), (2, 'bb');
-- Update of Non-selective column
UPDATE aa SET b = 'cc' WHERE a = 1;
-- Update of only selective column
UPDATE aa SET a = 3 WHERE a = 1;
-- Update of both columns
UPDATE aa SET a = 4, b = 'dd' WHERE a = 2;
DELETE FROM aa WHERE a = 4;
-- Have a look at changes with different modes.
-- In the second call changes are consumed to not impact the next cases.
SELECT data FROM pg_logical_slot_peek_changes('custom_slot', NULL, NULL, 'include_transaction', 'off');
SELECT data FROM pg_logical_slot_get_changes('custom_slot', NULL, NULL, 'include_transaction', 'on');
DROP TABLE aa;

-- DEFAULT case without PRIMARY KEY
CREATE TABLE aa (a int, b text NOT NULL);
INSERT INTO aa VALUES (1, 'aa'), (2, 'bb');
-- Update of Non-selective column
UPDATE aa SET b = 'cc' WHERE a = 1;
-- Update of only selective column
UPDATE aa SET a = 3 WHERE a = 1;
-- Update of both columns
UPDATE aa SET a = 4, b = 'dd' WHERE a = 2;
DELETE FROM aa WHERE a = 4;
-- Have a look at changes with different modes.
-- In the second call changes are consumed to not impact the next cases.
SELECT data FROM pg_logical_slot_peek_changes('custom_slot', NULL, NULL, 'include_transaction', 'off');
SELECT data FROM pg_logical_slot_get_changes('custom_slot', NULL, NULL, 'include_transaction', 'on');
DROP TABLE aa;

-- INDEX case
CREATE TABLE aa (a int NOT NULL, b text);
CREATE UNIQUE INDEX aai ON aa(a);
ALTER TABLE aa REPLICA IDENTITY USING INDEX aai;
INSERT INTO aa VALUES (1, 'aa'), (2, 'bb');
-- Update of Non-selective column
UPDATE aa SET b = 'cc' WHERE a = 1;
-- Update of only selective column
UPDATE aa SET a = 3 WHERE a = 1;
-- Update of both columns
UPDATE aa SET a = 4, b = 'dd' WHERE a = 2;
DELETE FROM aa WHERE a = 4;
-- Have a look at changes with different modes
SELECT data FROM pg_logical_slot_peek_changes('custom_slot', NULL, NULL, 'include_transaction', 'off');
SELECT data FROM pg_logical_slot_get_changes('custom_slot', NULL, NULL, 'include_transaction', 'on');
DROP TABLE aa;

-- INDEX case using a second column
CREATE TABLE aa (b text, a int NOT NULL);
CREATE UNIQUE INDEX aai ON aa(a);
ALTER TABLE aa REPLICA IDENTITY USING INDEX aai;
INSERT INTO aa VALUES ('aa', 1), ('bb', 2);
-- Update of Non-selective column
UPDATE aa SET b = 'cc' WHERE a = 1;
-- Update of only selective column
UPDATE aa SET a = 3 WHERE a = 1;
-- Update of both columns
UPDATE aa SET a = 4, b = 'dd' WHERE a = 2;
DELETE FROM aa WHERE a = 4;
-- Have a look at changes with different modes
SELECT data FROM pg_logical_slot_peek_changes('custom_slot', NULL, NULL, 'include_transaction', 'off');
SELECT data FROM pg_logical_slot_get_changes('custom_slot', NULL, NULL, 'include_transaction', 'on');
DROP TABLE aa;

-- FULL case
CREATE TABLE aa (a int primary key, b text NOT NULL);
ALTER TABLE aa REPLICA IDENTITY FULL;
INSERT INTO aa VALUES (1, 'aa'), (2, 'bb');
-- Update of Non-selective column
UPDATE aa SET b = 'cc' WHERE a = 1;
-- Update of only selective column
UPDATE aa SET a = 3 WHERE a = 1;
-- Update of both columns
UPDATE aa SET a = 4, b = 'dd' WHERE a = 2;
DELETE FROM aa WHERE a = 4;
-- Have a look at changes with different modes
SELECT data FROM pg_logical_slot_peek_changes('custom_slot', NULL, NULL, 'include_transaction', 'off');
SELECT data FROM pg_logical_slot_get_changes('custom_slot', NULL, NULL, 'include_transaction', 'on');
DROP TABLE aa;

-- NOTHING case
CREATE TABLE aa (a int primary key, b text NOT NULL);
ALTER TABLE aa REPLICA IDENTITY NOTHING;
INSERT INTO aa VALUES (1, 'aa'), (2, 'bb');
UPDATE aa SET b = 'cc' WHERE a = 1;
UPDATE aa SET a = 3 WHERE a = 1;
DELETE FROM aa WHERE a = 4;
-- Have a look at changes with different modes
SELECT data FROM pg_logical_slot_peek_changes('custom_slot', NULL, NULL, 'include_transaction', 'off');
SELECT data FROM pg_logical_slot_get_changes('custom_slot', NULL, NULL, 'include_transaction', 'on');
DROP TABLE aa;

-- Special value handling for various data types
-- boolean, with true and false values correctly shaped
CREATE TABLE aa (a boolean);
INSERT INTO aa VALUES (true);
INSERT INTO aa VALUES (false);
SELECT data FROM pg_logical_slot_get_changes('custom_slot', NULL, NULL, 'include_transaction', 'off');
DROP TABLE aa;
-- numeric and flost with Nan and infinity - quotes should be correctly placed
CREATE TABLE aa (a numeric, b float4, c float8);
INSERT INTO aa VALUES ('Nan'::numeric, 'Nan'::float4, 'Nan'::float8);
INSERT INTO aa VALUES (1.0, '+Infinity'::float4, '+Infinity'::float8);
INSERT INTO aa VALUES (2.0, '-Infinity'::float4, '-Infinity'::float8);
INSERT INTO aa VALUES (3.0, 4.0, 5.0);
SELECT data FROM pg_logical_slot_get_changes('custom_slot', NULL, NULL, 'include_transaction', 'off');
DROP TABLE aa;
-- Unchanged toast datum
CREATE TABLE tt (a int primary key, t text);
ALTER TABLE tt ALTER COLUMN t SET STORAGE EXTERNAL;
INSERT INTO tt VALUES (1, 'foo');
INSERT INTO tt VALUES (2, repeat('x', 3000));
UPDATE tt SET t=t WHERE a=1;
UPDATE tt SET t=t WHERE a=2;
SELECT substr(data, 1, 50), substr(data, 3000, 45)
  FROM pg_logical_slot_get_changes('custom_slot', NULL, NULL, 'include_transaction', 'off');
DROP TABLE tt;
-- Drop replication slot
SELECT pg_drop_replication_slot('custom_slot');
