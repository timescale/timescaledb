ALTER TABLE PUBLIC."Hypertable_1" ADD COLUMN temp_f INTEGER NOT NULL DEFAULT 31;
ALTER TABLE PUBLIC."Hypertable_1" DROP COLUMN temp_c;
ALTER TABLE PUBLIC."Hypertable_1" DROP COLUMN sensor_4;
ALTER TABLE PUBLIC."Hypertable_1" ALTER COLUMN humidity SET DEFAULT 100;
ALTER TABLE PUBLIC."Hypertable_1" ALTER COLUMN sensor_1 DROP DEFAULT;
ALTER TABLE PUBLIC."Hypertable_1" ALTER COLUMN sensor_2 SET DEFAULT NULL;
ALTER TABLE PUBLIC."Hypertable_1" ALTER COLUMN sensor_1 SET NOT NULL;
ALTER TABLE PUBLIC."Hypertable_1" ALTER COLUMN sensor_2 DROP NOT NULL;
ALTER TABLE PUBLIC."Hypertable_1" RENAME COLUMN sensor_2 TO sensor_2_renamed;
ALTER TABLE PUBLIC."Hypertable_1" RENAME COLUMN sensor_3 TO sensor_3_renamed;
DROP INDEX "ind_sensor_1";


CREATE OR REPLACE FUNCTION empty_trigger_func()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
END
$BODY$;

CREATE TRIGGER test_trigger BEFORE UPDATE OR DELETE ON PUBLIC."Hypertable_1"
FOR EACH STATEMENT EXECUTE PROCEDURE empty_trigger_func();

ALTER TABLE PUBLIC."Hypertable_1" ALTER COLUMN sensor_2_renamed SET DATA TYPE int;
ALTER INDEX "ind_humidity" RENAME TO "ind_humdity2";

-- Change should be reflected here
SELECT * FROM _timescaledb_catalog.chunk_index;

--create column with same name as previously renamed one
ALTER TABLE PUBLIC."Hypertable_1" ADD COLUMN sensor_3 BIGINT NOT NULL DEFAULT 131;
--create column with same name as previously dropped one
ALTER TABLE PUBLIC."Hypertable_1" ADD COLUMN sensor_4 BIGINT NOT NULL DEFAULT 131;
