DO
$BODY$
DECLARE
    hypertable_name TEXT;
BEGIN
    SELECT first_dim.schema_name || '.' || first_dim.table_name
    FROM _timescaledb_catalog.continuous_agg ca
    INNER JOIN LATERAL (
        SELECT dim.*, h.*
        FROM _timescaledb_catalog.hypertable h
        INNER JOIN _timescaledb_catalog.dimension dim ON (dim.hypertable_id = h.id)
        WHERE ca.raw_hypertable_id = h.id
        ORDER by dim.id ASC
        LIMIT 1
    ) first_dim ON true
    WHERE first_dim.column_type IN (REGTYPE 'int2', REGTYPE 'int4', REGTYPE 'int8')
    AND (first_dim.integer_now_func_schema IS NULL OR first_dim.integer_now_func IS NULL)
    INTO hypertable_name;

    IF hypertable_name is not null AND (current_setting('timescaledb.ignore_update_errors', true) is null OR current_setting('timescaledb.ignore_update_errors', true) != 'on') THEN
        RAISE 'The continuous aggregate on hypertable "%" will break unless an integer_now func is set using set_integer_now_func().', hypertable_name;
    END IF;
END
$BODY$;


ALTER TABLE  _timescaledb_catalog.continuous_agg
    ADD COLUMN  ignore_invalidation_older_than BIGINT NOT NULL DEFAULT BIGINT '9223372036854775807';
UPDATE _timescaledb_catalog.continuous_agg SET ignore_invalidation_older_than = BIGINT '9223372036854775807';

CLUSTER  _timescaledb_catalog.continuous_agg USING continuous_agg_pkey;
ALTER TABLE _timescaledb_catalog.continuous_agg SET WITHOUT CLUSTER;

CREATE INDEX IF NOT EXISTS continuous_agg_raw_hypertable_id_idx
      ON _timescaledb_catalog.continuous_agg(raw_hypertable_id);


--Add modification_time column
CREATE TABLE _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log_tmp AS SELECT * FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;
DROP TABLE _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
(
    hypertable_id              INTEGER NOT NULL,
    modification_time BIGINT  NOT NULL, --time at which the raw table was modified
    lowest_modified_value      BIGINT  NOT NULL,
    greatest_modified_value    BIGINT  NOT NULL
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_hypertable_invalidation_log', '');
--modification_time == INT_MIN to cause these invalidations to be processed
INSERT INTO _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
    SELECT hypertable_id, BIGINT '-9223372036854775808', lowest_modified_value, greatest_modified_value
    FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log_tmp;
DROP TABLE _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log_tmp;
CREATE INDEX continuous_aggs_hypertable_invalidation_log_idx
    ON _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log (hypertable_id, lowest_modified_value ASC);
GRANT SELECT ON  _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log TO PUBLIC;

--Add modification_time column
CREATE TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log_tmp AS SELECT * FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log;
DROP TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log;
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
(
    materialization_id         INTEGER
        REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id)
            ON DELETE CASCADE,
    modification_time BIGINT NOT NULL, --time at which the raw table was modified
    lowest_modified_value      BIGINT NOT NULL,
    greatest_modified_value    BIGINT NOT NULL
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_materialization_invalidation_log', '');
--modification_time == INT_MIN to cause these invalidations to be processed
INSERT INTO _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
    SELECT materialization_id, BIGINT '-9223372036854775808', lowest_modified_value, greatest_modified_value
    FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log_tmp;
DROP TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log_tmp;
CREATE INDEX continuous_aggs_materialization_invalidation_log_idx
    ON _timescaledb_catalog.continuous_aggs_materialization_invalidation_log (materialization_id, lowest_modified_value ASC);
GRANT SELECT ON  _timescaledb_catalog.continuous_aggs_materialization_invalidation_log TO PUBLIC;

ALTER TABLE _timescaledb_config.bgw_policy_drop_chunks ALTER COLUMN cascade_to_materializations DROP NOT NULL;

UPDATE _timescaledb_config.bgw_policy_drop_chunks SET cascade_to_materializations = NULL WHERE cascade_to_materializations = false;

ALTER TABLE  _timescaledb_catalog.chunk ADD COLUMN dropped BOOLEAN DEFAULT false;
UPDATE _timescaledb_catalog.chunk SET dropped = false;
ALTER TABLE _timescaledb_catalog.chunk ALTER COLUMN dropped SET NOT NULL;

CLUSTER  _timescaledb_catalog.chunk USING chunk_pkey;
ALTER TABLE _timescaledb_catalog.chunk SET WITHOUT CLUSTER;
