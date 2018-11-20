-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License, see LICENSE-APACHE
-- at the top level directory of the TimescaleDB distribution.


-- Convenience view to list all hypertables and their space usage
CREATE OR REPLACE VIEW information_schema._timescaledb_hypertables AS
  SELECT ht.schema_name,
    ht.table_name,
    ht.num_dimensions,
    (SELECT count(1)
     FROM _timescaledb_catalog.chunk ch
     WHERE ch.hypertable_id=ht.id
    ) AS num_chunks,
    size.table_size,
    size.index_size,
    size.toast_size,
    size.total_size
  FROM _timescaledb_catalog.hypertable ht,
    @extschema@.hypertable_relation_size_pretty(
      format('%I.%I',ht.schema_name,ht.table_name)
    ) size
  WHERE
    has_schema_privilege(ht.schema_name, 'USAGE');

