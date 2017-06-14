-- Add a new partition epoch with equally sized partitions
CREATE OR REPLACE FUNCTION _timescaledb_internal.find_chunk(
    time_dimension_id           INTEGER,
    time_value                  BIGINT,
    space_dimension_id          INTEGER,
    space_dimension_hash        BIGINT
)
    RETURNS _timescaledb_catalog.chunk LANGUAGE SQL STABLE AS
$BODY$
SELECT * 
FROM _timescaledb_catalog.chunk
WHERE 
id = (
SELECT cc.chunk_id
FROM _timescaledb_catalog.dimension_slice ds 
INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (ds.id = cc.dimension_slice_id) 
WHERE ds.dimension_id = time_dimension_id and ds.range_start <= time_value and ds.range_end >= time_value

INTERSECT

SELECT cc.chunk_id
FROM _timescaledb_catalog.dimension_slice ds 
INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (ds.id = cc.dimension_slice_id) 
WHERE ds.dimension_id =  space_dimension_id and ds.range_start <= space_dimension_hash and ds.range_end >= space_dimension_hash
)
$BODY$;
