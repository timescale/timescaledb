set log_min_messages to DEBUG1;
set client_min_messages to DEBUG1;
\COPY "33_testNs" FROM 'import_data/batch1_dev1.tsv' NULL AS '';
\COPY "33_testNs" FROM 'import_data/batch1_dev2.tsv' NULL AS '';
\COPY "33_testNs" FROM 'import_data/batch2_dev1.tsv' NULL AS '';
CREATE TABLE "public"."chunk_test"(
        time       BIGINT,
        metric     INTEGER,
        device_id  TEXT
    );

SELECT * FROM create_hypertable('"public"."chunk_test"', 'time', 'device_id', chunk_size_bytes => 10000);
INSERT INTO chunk_test VALUES(1, 1, 'dev1');
