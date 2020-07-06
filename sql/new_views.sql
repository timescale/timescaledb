-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE SCHEMA IF NOT EXISTS timescaledb_new;
drop view timescaledb_new.hypertables cascade;
drop view timescaledb_new.chunks cascade;

-- this does not work ---
---CREATE UNIQUE INDEX hypertable_oid ON _timescaledb_catalog.hypertable( format('%1$I.%2$I', schema_name, table_name)::regclass );
---hypertable metadata vew ---

CREATE OR REPLACE VIEW timescaledb_new.hypertables AS
  SELECT 
    format('%1$I.%2$I', ht.schema_name, ht.table_name)::regclass as hypertable,
    ht.schema_name AS schema_name,
    ht.table_name as table_name,
    t.tableowner AS owner,
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
    srchtbs.tablespace_list as tablespaces
  FROM _timescaledb_catalog.hypertable ht
        INNER JOIN pg_tables t
        ON ht.table_name=t.tablename 
           AND ht.schema_name=t.schemaname
        LEFT OUTER JOIN (
            SELECT hypertable_id, 
            array_agg(tablespace_name ORDER BY id) as tablespace_list 
            FROM _timescaledb_catalog.tablespace
            GROUP BY hypertable_id ) srchtbs
       ON ht.id = srchtbs.hypertable_id
       LEFT OUTER JOIN ( 
            SELECT hypertable_id, 
            array_agg(node_name ORDER BY node_name) as node_list
                      FROM _timescaledb_catalog.hypertable_data_node
                      GROUP BY hypertable_id) dn
    ON ht.id = dn.hypertable_id
    WHERE ht.compressed is false --> no intrenal compression tables
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
WHERE dim.hypertable_id = ht.id
;

-- chunks metadata view, shows information about the primary dimension column
CREATE OR REPLACE VIEW  timescaledb_new.chunks
AS
SELECT * from
(
SELECT
    format('%1$I.%2$I', ht.schema_name, ht.table_name)::regclass as hypertable,
    format('%1$I.%2$I', srcch.schema_name, srcch.table_name)::regclass as chunk,
    ht.schema_name as hypertable_schema,
    ht.table_name as hypertable_name,
    srcch.schema_name as schema_name,
    srcch.table_name as chunk_name,
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
    CASE WHEN srcch.compressed_chunk_id is not null THEN 'true'
         ELSE 'false'
    END as is_compressed,
    pgtab.spcname as chunk_table_space,
    chdn.node_list
FROM _timescaledb_catalog.chunk srcch 
      INNER JOIN _timescaledb_catalog.hypertable ht ON ht.id = srcch.hypertable_id
      INNER JOIN _timescaledb_catalog.chunk_constraint chcons ON srcch.id = chcons.chunk_id
      INNER JOIN _timescaledb_catalog.dimension dim ON srcch.hypertable_id = dim.hypertable_id
      INNER JOIN _timescaledb_catalog.dimension_slice dimsl ON dim.id = dimsl.dimension_id
                                and chcons.dimension_slice_id = dimsl.id
      INNER JOIN
       ( SELECT relname, reltablespace, nspname as schema_name 
                FROM pg_class , pg_namespace WHERE
                pg_class.relnamespace = pg_namespace.oid) cl 
       ON srcch.table_name = cl.relname and srcch.schema_name = cl.schema_name
       LEFT OUTER JOIN pg_tablespace pgtab ON pgtab.oid = reltablespace
       left outer join (
               SELECT chunk_id, array_agg(node_name ORDER BY node_name) as node_list
                            FROM _timescaledb_catalog.chunk_data_node
                            GROUP BY chunk_id) chdn
       ON srcch.id = chdn.chunk_id 
 WHERE srcch.dropped is false ---->check WHEN is this true
      and ht.compressed = false ) finalq
WHERE chunk_dimension_num = 1;
 ;


---variant of chunks --check which is more performant ---
--- this variation explicitly fetches the primary dimension and joins that
-- against the other tables
CREATE OR REPLACE VIEW  timescaledb_new.chunks2
AS
WITH chq as
(
  SELECT srcch.hypertable_id as hypertable_id,
         srcch.id as chunk_id,
         srcch.schema_name,
         srcch.table_name as chunk_name,
         srcch.compressed_chunk_id, 
         pgtab.spcname as table_space,
         node_list
 FROM _timescaledb_catalog.chunk srcch INNER JOIN
       ( SELECT relname, reltablespace, nspname as tablespace_name 
         FROM pg_class , pg_namespace WHERE
                pg_class.relnamespace = pg_namespace.oid) cl 
       ON srcch.table_name = cl.relname and srcch.schema_name = cl.tablespace_name
       LEFT OUTER JOIN pg_tablespace pgtab
       ON pgtab.oid = reltablespace
      left outer join (
               SELECT chunk_id, array_agg(node_name ORDER BY node_name) as node_list
                            FROM _timescaledb_catalog.chunk_data_node
                            GROUP BY chunk_id) chdn
      on srcch.id = chdn.chunk_id 
 WHERE srcch.dropped is false ---->check WHEN is this true
),
primdimq as
(
--get primary dimension information --
   SELECT hypertable_id, primdim, column_name, column_type
   FROM (
         SELECT hypertable_id, first_value(id) over(partition by hypertable_id order by id) as primdim,
                id, column_name, column_type 
         FROM _timescaledb_catalog.dimension
         ) pq 
  WHERE pq.primdim = id 
)
SELECT
    format('%1$I.%2$I', ht.schema_name, ht.table_name)::regclass as hypertable,
    format('%1$I.%2$I', chq.schema_name, chq.chunk_name)::regclass as chunk,
    ht.schema_name as hypertable_schema,
    ht.table_name as hypertable_name,
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
    chq.table_space,
    chq.node_list
FROM  chq,
      _timescaledb_catalog.hypertable ht,
     _timescaledb_catalog.chunk_constraint chcons,
     primdimq dim,
     _timescaledb_catalog.dimension_slice dimsl
WHERE   chq.chunk_id = chcons.chunk_id and ht.id = chq.hypertable_id
    and chq.hypertable_id = dim.hypertable_id
    and chcons.dimension_slice_id = dimsl.id
    and dim.primdim = dimsl.dimension_id and dim.hypertable_id = ht.id
   and ht.compressed = false;
;

