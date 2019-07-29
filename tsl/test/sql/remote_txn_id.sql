-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE FUNCTION _timescaledb_internal.test_remote_txn_id()
RETURNS void
AS :TSL_MODULE_PATHNAME, 'tsl_test_remote_txn_id'
LANGUAGE C STRICT;

SELECT _timescaledb_internal.test_remote_txn_id();

SELECT 'ts-1-10-20-30'::rxid;

create table tbl_w_rxid(
    txn_id rxid
);

CREATE UNIQUE INDEX idx_name ON tbl_w_rxid ((txn_id::text));

INSERT INTO tbl_w_rxid VALUES ('ts-1-10-20-30'), ('ts-1-11-20-30'), ('ts-1-10-21-30');

SELECT txn_id, _timescaledb_internal.rxid_in(_timescaledb_internal.rxid_out(txn_id))::text = txn_id::text FROM tbl_w_rxid;

\set ON_ERROR_STOP 0
INSERT INTO tbl_w_rxid VALUES ('ts-1-10-20-30');

SELECT ''::rxid;
SELECT '---'::rxid;
SELECT '----'::rxid;
SELECT 'ts---'::rxid;
SELECT 'ts----'::rxid;
SELECT 'ts-1-10-20a'::rxid;
SELECT 'ts-2-10-20-40'::rxid;
SELECT 'ts-1-10-20.0'::rxid;
SELECT 'ts-1-10.0-20'::rxid;
SELECT 'ts-a1-10-20-8'::rxid;
