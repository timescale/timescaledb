\o /dev/null
\ir include/insert_two_partitions.sql
\o

--old chunks
COPY "two_Partitions"("timeCustom", device_id, series_0, series_1) FROM STDIN DELIMITER ',';
1257894000000000000,dev3,1.5,2
\.
\copy "two_Partitions"("timeCustom", device_id, series_0, series_1) FROM STDIN DELIMITER ',';
1257894000000000000,dev3,1.5,2
\.

--new chunks
COPY "two_Partitions"("timeCustom", device_id, series_0, series_1) FROM STDIN DELIMITER ',';
2257894000000000000,dev3,1.5,2
\.
\copy "two_Partitions"("timeCustom", device_id, series_0, series_1) FROM STDIN DELIMITER ',';
2257894000000000000,dev3,1.5,2
\.

COPY (SELECT * FROM "two_Partitions" ORDER BY "timeCustom", device_id) TO STDOUT;


---test hypertable with FK
CREATE TABLE "meta" ("id" serial PRIMARY KEY);
CREATE TABLE "hyper" (
    "meta_id" integer NOT NULL REFERENCES meta(id),
    "time" bigint NOT NULL,
    "value" double precision NOT NULL
);
SELECT create_hypertable('hyper', 'time', chunk_time_interval => 100);

INSERT INTO "meta" ("id") values (1);
\copy hyper (time, meta_id, value) FROM STDIN DELIMITER ',';
1,1,1
\.

COPY hyper (time, meta_id, value) FROM STDIN DELIMITER ',';
2,1,1
\.

\set ON_ERROR_STOP 0
\copy hyper (time, meta_id, value) FROM STDIN DELIMITER ',';
1,2,1
\.
COPY hyper (time, meta_id, value) FROM STDIN DELIMITER ',';
2,2,1
\.
\set ON_ERROR_STOP 1

COPY (SELECT * FROM hyper ORDER BY time, meta_id) TO STDOUT;
