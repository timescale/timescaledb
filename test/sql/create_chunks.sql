\ir include/create_single_db.sql

CREATE TABLE chunk_closing_test(
        time       BIGINT,
        metric     INTEGER,
        device_id  TEXT
    );

-- Test chunk closing/creation
SELECT * FROM create_hypertable('chunk_closing_test', 'time', 'device_id', 2, chunk_size_bytes => 10000);
INSERT INTO chunk_closing_test VALUES(1, 1, 'dev1');
INSERT INTO chunk_closing_test VALUES(2, 2, 'dev2');
INSERT INTO chunk_closing_test VALUES(3, 3, 'dev3');
SELECT * FROM chunk_closing_test;
SELECT * FROM ONLY chunk_closing_test;
SELECT * FROM _timescaledb_catalog.chunk c
    LEFT JOIN _timescaledb_catalog.chunk_replica_node crn ON (c.id = crn.chunk_id)
    LEFT JOIN _timescaledb_catalog.partition_replica pr ON (crn.partition_replica_id = pr.id)
    LEFT JOIN _timescaledb_catalog.hypertable h ON (pr.hypertable_id = h.id)
    WHERE h.schema_name = 'public' AND h.table_name = 'chunk_closing_test';


