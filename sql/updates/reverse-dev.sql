DROP VIEW _timescaledb_internal.hypertable_chunk_local_size;
DROP FUNCTION _timescaledb_internal.relation_size(relation REGCLASS);
DROP INDEX _timescaledb_catalog.chunk_constraint_dimension_slice_id_idx;
CREATE INDEX chunk_constraint_chunk_id_dimension_slice_id_idx ON _timescaledb_catalog.chunk_constraint (chunk_id, dimension_slice_id);

CREATE OR REPLACE FUNCTION timescaledb_experimental.refresh_continuous_aggregate(
    continuous_aggregate     REGCLASS,
    hypertable_chunk         REGCLASS
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_continuous_agg_refresh_chunk' LANGUAGE C VOLATILE;

DO
$$
DECLARE
    caggs_finalized TEXT[];
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

    IF caggs_finalized > 0 THEN
        RAISE EXCEPTION 'Downgrade is not possible because there are % continuous aggregates using the finalized form: %', caggs_count, caggs_finalized
            USING HINT = 'Remove the corresponding continuous aggregates manually before downgrading';
    END IF;
END;
$$
LANGUAGE 'plpgsql';

ALTER TABLE _timescaledb_catalog.continuous_agg
    DROP COLUMN finalized;
