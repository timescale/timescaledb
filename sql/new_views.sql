-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE SCHEMA IF NOT EXISTS timescaledb_new;
drop view timescaledb_new.hypertables cascade;
drop view timescaledb_new.chunks cascade;

---hypertable metadata vew ---
CREATE OR REPLACE VIEW timescaledb_new.hypertables AS
WITH ht as (
  SELECT srcht.*, t.tableowner, srchtbs.tablespace_list
  FROM _timescaledb_catalog.hypertable srcht
        INNER JOIN pg_tables t
        ON srcht.table_name=t.tablename 
           AND srcht.schema_name=t.schemaname
        LEFT OUTER JOIN (
            SELECT hypertable_id, 
            array_agg(tablespace_name ORDER BY id) as tablespace_list 
            FROM _timescaledb_catalog.tablespace
            GROUP BY hypertable_id ) srchtbs
       ON srcht.id = srchtbs.hypertable_id
  WHERE srcht.compressed is false --> no intrenal compression tables
)
  SELECT 
    format('%1$I.%2$I', ht.schema_name, ht.table_name)::regclass as hypertable,
    ht.schema_name AS schema_name,
    ht.table_name as table_name,
    ht.tableowner AS owner,
    ht.num_dimensions,
    (SELECT count(1)
     FROM _timescaledb_catalog.chunk ch
     WHERE ch.hypertable_id=ht.id
    ) AS num_chunks,
    (CASE WHEN ht.compressed_hypertable_id IS NULL THEN true ELSE false END) AS
  compression_enabled,
    (CASE WHEN ht.replication_factor > 0 THEN true ELSE false END) AS is_distributed,
    ht.replication_factor ,
    dn.node_list as data_nodes,
    ht.tablespace_list as tablespaces
  FROM ht 
    LEFT OUTER JOIN ( SELECT hypertable_id, array_agg(node_name ORDER BY node_name) as node_list
                      FROM _timescaledb_catalog.hypertable_data_node
                      GROUP BY hypertable_id) dn
    ON ht.id = dn.hypertable_id
;

CREATE OR REPLACE VIEW  timescaledb_new.dimensions
AS
SELECT 
    format('%1$I.%2$I', ht.schema_name, ht.table_name)::regclass as hypertable,
       --dim.id as dimension_id,
    rank() over(partition by hypertable_id order by dim.id) as dimension_order,
    dim.column_name,
    dim.column_type,
    CASE WHEN dim.interval_length is NULL 
         THEN 'Space' 
         ELSE 'Time' 
    END as dimension_type,
    CASE WHEN dim.interval_length is NOT NULL THEN
              CASE 
              WHEN dim.column_type = 'TIMESTAMP'::regtype OR 
                   dim.column_type = 'TIMESTAMPTZ'::regtype OR 
                   dim.column_type = 'DATE'::regtype 
              THEN _timescaledb_internal.to_interval(dim.interval_length) 
              ELSE NULL 
              END
    END as time_interval, 
    CASE WHEN dim.interval_length is NOT NULL THEN
              CASE 
              WHEN dim.column_type = 'TIMESTAMP'::regtype OR 
                   dim.column_type = 'TIMESTAMPTZ'::regtype OR 
                   dim.column_type = 'DATE'::regtype 
              THEN  NULL
              ELSE dim.interval_length 
              END
    END as integer_interval, 
    dim.integer_now_func,
    dim.num_slices as num_partitions
FROM _timescaledb_catalog.hypertable ht, _timescaledb_catalog.dimension dim
where dim.hypertable_id = ht.id
;

-- chunks metadata view, shows information about the primary dimension column
CREATE OR REPLACE VIEW  timescaledb_new.chunks
AS
WITH chtab as
(
  SELECT srcch.hypertable_id as hypertable_id,
         srcch.id as chunk_id,
         srcch.schema_name,
         srcch.table_name as chunk_name,
         srcch.compressed_chunk_id, 
         pgtab.spcname as table_space
 FROM _timescaledb_catalog.chunk srcch INNER JOIN
       ( SELECT relname, reltablespace, nspname as tablespace_name 
         FROM pg_class , pg_namespace where
                pg_class.relnamespace = pg_namespace.oid) cl 
       ON srcch.table_name = cl.relname and srcch.schema_name = cl.tablespace_name
       LEFT OUTER JOIN pg_tablespace pgtab
       ON pgtab.oid = reltablespace
 WHERE srcch.dropped is false ---->check WHEN is this true
),
chq as
(
SELECT
 --src.chunk_id,
 src.*, 
 CASE WHEN src.compressed_chunk_id is null THEN src.table_space
      else comp.table_space
 END as chunk_table_space,
 node_list
 FROM chtab src left outer join chtab comp
      on src.compressed_chunk_id = comp.chunk_id
      left outer join (
               SELECT chunk_id, array_agg(node_name ORDER BY node_name) as node_list
                            FROM _timescaledb_catalog.chunk_data_node
                            GROUP BY chunk_id) chdn
      on src.chunk_id = chdn.chunk_id 
),
finalq as
( 
SELECT
    format('%1$I.%2$I', ht.schema_name, ht.table_name)::regclass as hypertable,
    format('%1$I.%2$I', chq.schema_name, chq.chunk_name)::regclass as chunk,
    chq.schema_name as schema_name,
    chq.chunk_name as chunk_name,
--       chcons.constraint_name,
    dim.column_name as primary_dimension,
    dim.column_type as primary_dimension_type,
    row_number() over(partition by chcons.chunk_id order by chcons.dimension_slice_id) as chunk_dimension_num,
---chcons.dimension_slice_id as chunk_dimension_id,
    CASE
        WHEN ( dim.column_type = 'TIMESTAMP'::regtype OR
               dim.column_type = 'TIMESTAMPTZ'::regtype OR
               dim.column_type = 'DATE'::regtype )
        THEN _timescaledb_internal.to_timestamp(dimsl.range_start)
        ELSE NULL
    END as range_start,
    CASE
         WHEN ( dim.column_type = 'TIMESTAMP'::regtype OR
                dim.column_type = 'TIMESTAMPTZ'::regtype OR
                dim.column_type = 'DATE'::regtype )
         THEN _timescaledb_internal.to_timestamp(dimsl.range_end)
         ELSE NULL
    END as range_end,
    CASE
        WHEN ( dim.column_type = 'TIMESTAMP'::regtype OR
               dim.column_type = 'TIMESTAMPTZ'::regtype OR
               dim.column_type = 'DATE'::regtype )
        THEN NULL
        ELSE dimsl.range_start
    END as integer_range_start,
    CASE
        WHEN ( dim.column_type = 'TIMESTAMP'::regtype OR
               dim.column_type = 'TIMESTAMPTZ'::regtype OR
               dim.column_type = 'DATE'::regtype )
        THEN NULL
        ELSE dimsl.range_end
    END as integer_range_end,
    CASE WHEN chq.compressed_chunk_id is not null THEN 'true'
         ELSE 'false'
    END as is_compressed,
    chq.chunk_table_space,
    chq.node_list
FROM  chq,
     ( SELECT * FROM _timescaledb_catalog.hypertable
       where compressed = false) ht ,
     _timescaledb_catalog.chunk_constraint chcons,
     _timescaledb_catalog.dimension dim,
     _timescaledb_catalog.dimension_slice dimsl
WHERE   chq.chunk_id = chcons.chunk_id and ht.id = chq.hypertable_id
    and chq.hypertable_id = dim.hypertable_id
    and chcons.dimension_slice_id = dimsl.id
    and dim.id = dimsl.dimension_id
)
SELECT hypertable, chunk, schema_name, chunk_name,
       primary_dimension, primary_dimension_type,
       range_start, range_end,
       integer_range_start, integer_range_end,
       is_compressed,
       chunk_table_space,
       node_list
FROM finalq
WHERE chunk_dimension_num = 1;
 ;
