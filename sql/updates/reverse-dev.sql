DROP VIEW _timescaledb_internal.hypertable_chunk_local_size;
DROP FUNCTION _timescaledb_internal.relation_size(relation REGCLASS);
DROP INDEX _timescaledb_catalog.chunk_constraint_dimension_slice_id_idx;
CREATE INDEX chunk_constraint_chunk_id_dimension_slice_id_idx ON _timescaledb_catalog.chunk_constraint (chunk_id, dimension_slice_id);

DO
$$
DECLARE
    caggs_finalized TEXT;
    caggs_count INTEGER;
BEGIN
    SELECT
        string_agg(format('%I.%I', user_view_schema, user_view_name), ', '),
        count(*)
    INTO
        caggs_finalized,
        caggs_count
    FROM
        _timescaledb_catalog.continuous_agg
    WHERE
        finalized IS TRUE;

    IF caggs_count > 0 THEN
        RAISE EXCEPTION 'Downgrade is not possible because there are % continuous aggregates using the finalized form: %', caggs_count, caggs_finalized
            USING HINT = 'Remove the corresponding continuous aggregates manually before downgrading';
    END IF;
END;
$$
LANGUAGE 'plpgsql';

--
-- Rebuild the catalog table `_timescaledb_catalog.continuous_agg`
--
-- We need to recreate the catalog from scratch because when we drop a column
-- Postgres mark the `pg_attribute.attisdropped=TRUE` instead of removing it from
-- the `pg_catalog.pg_attribute` table.
--
-- If we downgrade and upgrade the extension without rebuild the catalog table it
-- will mess with `pg_attribute.attnum` and we will end up with issues when trying
-- to update data in those catalog tables.
--
DROP VIEW IF EXISTS timescaledb_information.hypertables;
DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;

ALTER EXTENSION timescaledb
    DROP TABLE _timescaledb_catalog.continuous_agg;

ALTER TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
    DROP CONSTRAINT continuous_aggs_materialization_invalid_materialization_id_fkey;

ALTER TABLE _timescaledb_catalog.continuous_agg
    DROP COLUMN finalized;

CREATE TABLE _timescaledb_catalog._tmp_continuous_agg (
    LIKE _timescaledb_catalog.continuous_agg
    INCLUDING ALL
    -- indexes and constraintes will be created later to keep original names
    EXCLUDING INDEXES
    EXCLUDING CONSTRAINTS
);

INSERT INTO _timescaledb_catalog._tmp_continuous_agg
    SELECT
        mat_hypertable_id,
        raw_hypertable_id,
        user_view_schema,
        user_view_name,
        partial_view_schema,
        partial_view_name,
        bucket_width,
        direct_view_schema,
        direct_view_name,
        materialized_only
    FROM
        _timescaledb_catalog.continuous_agg
    ORDER BY
        mat_hypertable_id;

DROP TABLE _timescaledb_catalog.continuous_agg;

ALTER TABLE _timescaledb_catalog._tmp_continuous_agg
    RENAME TO continuous_agg;

ALTER TABLE _timescaledb_catalog.continuous_agg
    ADD CONSTRAINT continuous_agg_pkey PRIMARY KEY (mat_hypertable_id),
    ADD CONSTRAINT continuous_agg_partial_view_schema_partial_view_name_key UNIQUE (partial_view_schema, partial_view_name),
    ADD CONSTRAINT continuous_agg_user_view_schema_user_view_name_key UNIQUE (user_view_schema, user_view_name),
    ADD CONSTRAINT continuous_agg_mat_hypertable_id_fkey FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    ADD CONSTRAINT continuous_agg_raw_hypertable_id_fkey FOREIGN KEY (raw_hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE;

CREATE INDEX continuous_agg_raw_hypertable_id_idx ON _timescaledb_catalog.continuous_agg (raw_hypertable_id);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_agg', '');

GRANT SELECT ON TABLE _timescaledb_catalog.continuous_agg TO PUBLIC;

ALTER TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
    ADD CONSTRAINT continuous_aggs_materialization_invalid_materialization_id_fkey
        FOREIGN KEY (materialization_id)
        REFERENCES _timescaledb_catalog.continuous_agg(mat_hypertable_id) ON DELETE CASCADE;

ANALYZE _timescaledb_catalog.continuous_agg;
