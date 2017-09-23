\o /dev/null
\ir include/insert_two_partitions.sql
\o

SELECT * FROM _timescaledb_catalog.hypertable;
SELECT * FROM _timescaledb_catalog.chunk;
SELECT * FROM test.show_subtables('"two_Partitions"');
SELECT * FROM "two_Partitions";

SET client_min_messages = WARNING;
TRUNCATE "two_Partitions";

SELECT * FROM _timescaledb_catalog.hypertable;
SELECT * FROM _timescaledb_catalog.chunk;

-- should be empty
SELECT * FROM test.show_subtables('"two_Partitions"');
SELECT * FROM "two_Partitions";

INSERT INTO public."two_Partitions"("timeCustom", device_id, series_0, series_1) VALUES
(1257987600000000000, 'dev1', 1.5, 1),
(1257987600000000000, 'dev1', 1.5, 2),
(1257894000000000000, 'dev2', 1.5, 1),
(1257894002000000000, 'dev1', 2.5, 3);

SELECT * FROM _timescaledb_catalog.chunk;

CREATE VIEW dependent_view AS SELECT * FROM _timescaledb_internal._hyper_1_5_chunk;

CREATE OR REPLACE FUNCTION test_trigger()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    cnt INTEGER;
BEGIN
    RAISE WARNING 'FIRING trigger when: % level: % op: % cnt: % trigger_name %',
          tg_when, tg_level, tg_op, cnt, tg_name;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END
$BODY$;

-- test truncate on a chunk
CREATE TRIGGER _test_truncate_before
    BEFORE TRUNCATE ON _timescaledb_internal._hyper_1_5_chunk
    FOR EACH STATEMENT EXECUTE PROCEDURE test_trigger();

CREATE TRIGGER _test_truncate_after
    AFTER TRUNCATE ON _timescaledb_internal._hyper_1_5_chunk
    FOR EACH STATEMENT EXECUTE PROCEDURE test_trigger();

\set ON_ERROR_STOP 0
TRUNCATE "two_Partitions";
\set ON_ERROR_STOP 1

TRUNCATE "two_Partitions" CASCADE;
-- should be empty
SELECT * FROM test.show_subtables('"two_Partitions"');
SELECT * FROM "two_Partitions";
