-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--
-- The general compressed_data type;
--
CREATE TYPE _timescaledb_internal.compressed_data (
    INTERNALLENGTH = VARIABLE,
    STORAGE = EXTERNAL,
    ALIGNMENT = DOUBLE, --needed for alignment in ARRAY type compression
    INPUT = _timescaledb_internal.compressed_data_in,
    OUTPUT = _timescaledb_internal.compressed_data_out,
    RECEIVE = _timescaledb_internal.compressed_data_recv,
    SEND = _timescaledb_internal.compressed_data_send
);

-- Complete T-Digest type

CREATE TYPE @extschema@.tdigest (
  input = _timescaledb_internal.tdigest_in,
  output = _timescaledb_internal.tdigest_out,
  receive = _timescaledb_internal.tdigest_recv,
  send = _timescaledb_internal.tdigest_send,
  alignment = int4,
  storage = extended
);
