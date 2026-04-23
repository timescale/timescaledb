-- Migration: refresh orderby sparse index entries in compression_settings
UPDATE _timescaledb_catalog.compression_settings
SET index = (
    SELECT COALESCE(jsonb_agg(elem), '[]'::jsonb)
    FROM jsonb_array_elements(index) AS elem
    WHERE elem->>'source' != 'orderby'
)
WHERE index IS NOT NULL
AND index @> '[{"source": "orderby"}]';

UPDATE _timescaledb_catalog.compression_settings cs
SET index = COALESCE(index, '[]'::jsonb) ||
            (
            SELECT jsonb_agg(jsonb_build_object(
                                'type', 'minmax',
                                'source', 'orderby',
                                'column', elem))
            FROM unnest(cs.orderby) AS elem
            )
WHERE cs.orderby IS NOT NULL;
