\o /dev/null
\ir include/create_single_db.sql
\o

CREATE TABLE hyper (
  time BIGINT NOT NULL,
  device_id TEXT NOT NULL,
  sensor_1 NUMERIC NULL DEFAULT 1
);

CREATE OR REPLACE FUNCTION test_trigger()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    cnt INTEGER;
BEGIN
    SELECT count(*) INTO cnt FROM hyper;
    RAISE WARNING 'FIRING trigger when: % level: % op: % cnt: %', tg_when, tg_level, tg_op, cnt;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END
$BODY$;

CREATE TRIGGER _0_test_trigger
    BEFORE INSERT OR UPDATE ON hyper
    FOR EACH ROW EXECUTE PROCEDURE test_trigger();

\set ON_ERROR_STOP 0
SELECT * FROM create_hypertable('hyper', 'time');
\set ON_ERROR_STOP 1
DROP TRIGGER _0_test_trigger ON hyper;

CREATE TRIGGER _0_test_trigger
    AFTER INSERT ON hyper
    FOR EACH ROW EXECUTE PROCEDURE test_trigger();
\set ON_ERROR_STOP 0
SELECT * FROM create_hypertable('hyper', 'time');
\set ON_ERROR_STOP 1
DROP TRIGGER _0_test_trigger ON hyper;


CREATE TRIGGER _0_test_trigger
    AFTER INSERT ON hyper
    FOR EACH STATEMENT EXECUTE PROCEDURE test_trigger();
\set ON_ERROR_STOP 0
SELECT * FROM create_hypertable('hyper', 'time');
\set ON_ERROR_STOP 1
DROP TRIGGER _0_test_trigger ON hyper;

CREATE TRIGGER _0_test_trigger_insert
    BEFORE INSERT ON hyper
    FOR EACH ROW EXECUTE PROCEDURE test_trigger();

CREATE TRIGGER _0_test_trigger_insert_s_before
    BEFORE INSERT ON hyper
    FOR EACH STATEMENT EXECUTE PROCEDURE test_trigger();

CREATE TRIGGER _0_test_trigger_update
    BEFORE UPDATE ON hyper
    FOR EACH ROW EXECUTE PROCEDURE test_trigger();

CREATE TRIGGER _0_test_trigger_delete
    BEFORE delete ON hyper
    FOR EACH ROW EXECUTE PROCEDURE test_trigger();

CREATE TRIGGER _0_test_trigger_update_s_before
    BEFORE UPDATE ON hyper
    FOR EACH STATEMENT EXECUTE PROCEDURE test_trigger();

CREATE TRIGGER _0_test_trigger_delete_s_before
    BEFORE DELETE ON hyper
    FOR EACH STATEMENT EXECUTE PROCEDURE test_trigger();


CREATE TRIGGER _0_test_trigger_update_after
    AFTER UPDATE ON hyper
    FOR EACH ROW EXECUTE PROCEDURE test_trigger();

CREATE TRIGGER _0_test_trigger_delete_after
    AFTER delete ON hyper
    FOR EACH ROW EXECUTE PROCEDURE test_trigger();

CREATE TRIGGER _0_test_trigger_update_s_after
    AFTER UPDATE ON hyper
    FOR EACH STATEMENT EXECUTE PROCEDURE test_trigger();

CREATE TRIGGER _0_test_trigger_delete_s_after
    AFTER DELETE ON hyper
    FOR EACH STATEMENT EXECUTE PROCEDURE test_trigger();


SELECT * FROM create_hypertable('hyper', 'time', chunk_time_interval => 10);

--test triggers before create_hypertable
INSERT INTO hyper(time, device_id,sensor_1) VALUES
(1257987600000000000, 'dev1', 1);

INSERT INTO hyper(time, device_id,sensor_1) VALUES
(1257987700000000000, 'dev2', 1);

UPDATE hyper SET sensor_1 = 2;

DELETE FROM hyper;

--test drop trigger
DROP TRIGGER _0_test_trigger_insert ON hyper;
DROP TRIGGER _0_test_trigger_insert_s_before ON hyper;
INSERT INTO hyper(time, device_id,sensor_1) VALUES
(1257987600000000000, 'dev1', 1);
INSERT INTO hyper(time, device_id,sensor_1) VALUES
(1257987700000000000, 'dev2', 1);

DROP TRIGGER _0_test_trigger_update ON hyper;
DROP TRIGGER _0_test_trigger_update_s_before ON hyper;
DROP TRIGGER _0_test_trigger_update_after ON hyper;
DROP TRIGGER _0_test_trigger_update_s_after ON hyper;
UPDATE hyper SET sensor_1 = 2;

DROP TRIGGER _0_test_trigger_delete ON hyper;
DROP TRIGGER _0_test_trigger_delete_s_before ON hyper;
DROP TRIGGER _0_test_trigger_delete_after ON hyper;
DROP TRIGGER _0_test_trigger_delete_s_after ON hyper;
DELETE FROM hyper;

--test create trigger on hypertable
CREATE TRIGGER _0_test_trigger_insert
    BEFORE INSERT ON hyper
    FOR EACH ROW EXECUTE PROCEDURE test_trigger();

CREATE TRIGGER _0_test_trigger_insert_s_before
    BEFORE INSERT ON hyper
    FOR EACH STATEMENT EXECUTE PROCEDURE test_trigger();

CREATE TRIGGER _0_test_trigger_update
    BEFORE UPDATE ON hyper
    FOR EACH ROW EXECUTE PROCEDURE test_trigger();

CREATE TRIGGER _0_test_trigger_delete
    BEFORE delete ON hyper
    FOR EACH ROW EXECUTE PROCEDURE test_trigger();

CREATE TRIGGER _0_test_trigger_update_s_before
    BEFORE UPDATE ON hyper
    FOR EACH STATEMENT EXECUTE PROCEDURE test_trigger();

CREATE TRIGGER _0_test_trigger_delete_s_before
    BEFORE DELETE ON hyper
    FOR EACH STATEMENT EXECUTE PROCEDURE test_trigger();

CREATE TRIGGER _0_test_trigger_update_after
    AFTER UPDATE ON hyper
    FOR EACH ROW EXECUTE PROCEDURE test_trigger();

CREATE TRIGGER _0_test_trigger_delete_after
    AFTER delete ON hyper
    FOR EACH ROW EXECUTE PROCEDURE test_trigger();

CREATE TRIGGER _0_test_trigger_update_s_after
    AFTER UPDATE ON hyper
    FOR EACH STATEMENT EXECUTE PROCEDURE test_trigger();

CREATE TRIGGER _0_test_trigger_delete_s_after
    AFTER DELETE ON hyper
    FOR EACH STATEMENT EXECUTE PROCEDURE test_trigger();


INSERT INTO hyper(time, device_id,sensor_1) VALUES
(1257987600000000000, 'dev1', 1);

INSERT INTO hyper(time, device_id,sensor_1) VALUES
(1257987700000000000, 'dev2', 1);

UPDATE hyper SET sensor_1 = 2;

DELETE FROM hyper;

