-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE SCHEMA IF NOT EXISTS timescaledb_debug;
drop view timescaledb_new.hypertables cascade;
drop view timescaledb_new.chunks cascade;

-- chunks metadata view, shows information about all the chunk dimensions
-- multiple rows for a chunk : 1 row per dimension  
CREATE OR REPLACE VIEW  timescaledb_debug.chunk_dimensions
AS
SELECT 
    format('%1$I.%2$I', ht.schema_name, ht.table_name)::regclass as hypertable,
    format('%1$I.%2$I', chq.schema_name, chq.table_name)::regclass as chunk,
    chq.schema_name as schema_name,
    chq.table_name as chunk_name,
--       chcons.constraint_name,
    dim.column_name as dimension_column,
    dim.column_type as dimension_column_type,
    row_number() over(partition by chcons.chunk_id order by dim.id) as chunk_dimension_num,
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
    CASE WHEN chq.compressed_chunk_id is not null then 'true'
         ELSE 'false'
    END as is_compressed
FROM _timescaledb_catalog.chunk chq, 
     _timescaledb_catalog.hypertable ht,
     _timescaledb_catalog.chunk_constraint chcons,
     _timescaledb_catalog.dimension dim,
         _timescaledb_catalog.dimension_slice dimsl
WHERE   chq.id = chcons.chunk_id and ht.id = chq.hypertable_id
    and chq.hypertable_id = dim.hypertable_id
    and chcons.dimension_slice_id = dimsl.id
    and dim.id = dimsl.dimension_id
   and chq.dropped is false
;

--difference from chunk_dimensions is that hypertable is represented as
-- schema_name + table_name
CREATE OR REPLACE VIEW  timescaledb_debug.chunk_dimensions2
AS
SELECT 
    ht.schema_name as hypertable_schema, ht.table_name as hypertable_name,
    format('%1$I.%2$I', chq.schema_name, chq.table_name)::regclass as chunk,
    chq.schema_name as schema_name,
    chq.table_name as chunk_name,
--       chcons.constraint_name,
    dim.column_name as dimension_column,
    dim.column_type as dimension_column_type,
    row_number() over(partition by chcons.chunk_id order by dim.id) as chunk_dimension_num,
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
    CASE WHEN chq.compressed_chunk_id is not null then 'true'
         ELSE 'false'
    END as is_compressed
FROM  
     _timescaledb_catalog.chunk chq, 
     _timescaledb_catalog.hypertable ht,
     _timescaledb_catalog.chunk_constraint chcons,
     _timescaledb_catalog.dimension dim,
         _timescaledb_catalog.dimension_slice dimsl
WHERE   chq.id = chcons.chunk_id and ht.id = chq.hypertable_id
    and chq.hypertable_id = dim.hypertable_id
    and chcons.dimension_slice_id = dimsl.id
    and dim.id = dimsl.dimension_id
   and chq.dropped is false
;

