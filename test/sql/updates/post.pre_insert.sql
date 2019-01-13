-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\d+ _timescaledb_catalog.hypertable
\d+ _timescaledb_catalog.chunk
\d+ _timescaledb_catalog.dimension
\d+ _timescaledb_catalog.dimension_slice
\d+ _timescaledb_catalog.chunk_constraint
\d+ _timescaledb_catalog.chunk_index
\d+ _timescaledb_catalog.tablespace

\z _timescaledb_cache.*
\z _timescaledb_catalog.*
\z _timescaledb_config.*
\z _timescaledb_internal.*

\di+ _timescaledb_catalog.*
-- Do not list sequence details because of potentially different state
-- of the sequence between updated and restored versions of a database
\ds _timescaledb_catalog.*;
\df _timescaledb_internal.*;
\df+ _timescaledb_internal.*;
\df public.*;
\df+ public.*;

\dy
\d+ PUBLIC.*

\dx+ timescaledb
SELECT count(*)
  FROM pg_depend
 WHERE refclassid = 'pg_extension'::regclass
     AND refobjid = (SELECT oid FROM pg_extension WHERE extname = 'timescaledb');

-- The list of tables configured to be dumped.
SELECT obj::regclass::text
FROM (SELECT unnest(extconfig) AS obj FROM pg_extension WHERE extname='timescaledb') AS objects
ORDER BY obj::regclass::text;

SELECT * FROM _timescaledb_catalog.chunk_constraint ORDER BY chunk_id, dimension_slice_id, constraint_name;
SELECT index_name FROM _timescaledb_catalog.chunk_index ORDER BY index_name;

\d+ _timescaledb_internal._hyper*
