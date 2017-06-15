\ir include/create_single_db.sql

CREATE TABLE chunk_test(
        time       BIGINT,
        metric     INTEGER,
        device_id  TEXT
    );

-- Test chunk closing/creation
SELECT * FROM create_hypertable('chunk_test', 'time', 'device_id', 2, chunk_time_interval => 10);
SELECT * FROM _timescaledb_catalog.hypertable;

INSERT INTO chunk_test VALUES (1, 1, 'dev1'),
                              (2, 2, 'dev2'),
                              (45, 2, 'dev2'),
                              (46, 2, 'dev2');

SELECT * FROM set_chunk_time_interval('chunk_test', 40::bigint);

INSERT INTO chunk_test VALUES(23, 3, 'dev3');

SELECT * FROM chunk_test order by time, metric, device_id;
SELECT * FROM _timescaledb_catalog.chunk;
SELECT * FROM _timescaledb_catalog.hypertable;

SELECT * FROM ONLY chunk_test;
SELECT * FROM _timescaledb_catalog.chunk c
    LEFT JOIN _timescaledb_catalog.partition p ON (p.id = c.partition_id)
    LEFT JOIN _timescaledb_catalog.partition_epoch pe ON (pe.id = p.epoch_id)
    LEFT JOIN _timescaledb_catalog.hypertable h ON (pe.hypertable_id = h.id)
    WHERE h.schema_name = 'public' AND h.table_name = 'chunk_test'
    ORDER BY c.id;


-- Test chunk aligning between partitions
CREATE TABLE chunk_align_test(
        time       BIGINT,
        metric     INTEGER,
        device_id  TEXT
    );
SELECT * FROM create_hypertable('chunk_align_test', 'time', 'device_id', 2, chunk_time_interval => 10);

INSERT INTO chunk_align_test VALUES (1, 1, 'dev1'); -- this should create a 10 wide chunk

SELECT * FROM _timescaledb_catalog.chunk c
    LEFT JOIN _timescaledb_catalog.chunk_constraint cc ON (c.id = cc.chunk_id)
    LEFT JOIN _timescaledb_catalog.dimension_slice ds ON (ds.id = cc.dimension_slice_id)
    LEFT JOIN _timescaledb_catalog.dimension d ON (d.id = ds.dimension_id)
    LEFT JOIN _timescaledb_catalog.hypertable h ON (d.hypertable_id = h.id)
    WHERE h.schema_name = 'public' AND h.table_name = 'chunk_align_test'
          AND d.column_name = 'time'
    ORDER BY c.id, d.id;
    
SELECT * FROM set_chunk_time_interval('chunk_align_test', 40::bigint);
INSERT INTO chunk_align_test VALUES (5, 1, 'dev2'); -- this should still create a 10 wide chunk
INSERT INTO chunk_align_test VALUES (45, 1, 'dev2'); -- this should create a 40 wide chunk

SELECT * FROM _timescaledb_catalog.chunk c
    LEFT JOIN _timescaledb_catalog.chunk_constraint cc ON (c.id = cc.chunk_id)
    LEFT JOIN _timescaledb_catalog.dimension_slice ds ON (ds.id = cc.dimension_slice_id)
    LEFT JOIN _timescaledb_catalog.dimension d ON (d.id = ds.dimension_id)
    LEFT JOIN _timescaledb_catalog.hypertable h ON (d.hypertable_id = h.id)
    WHERE h.schema_name = 'public' AND h.table_name = 'chunk_align_test'
          AND d.column_name = 'time'
    ORDER BY c.id, d.id;
