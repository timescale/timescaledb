DO $$
DECLARE 
    r record;
    cnt INTEGER;
BEGIN
    FOR r IN 
        SELECT * 
        FROM _timescaledb_catalog.chunk_constraint cc
        INNER JOIN _timescaledb_catalog.chunk c ON (c.id = cc.chunk_id)
    LOOP
        SELECT count(*) INTO cnt
        FROM pg_constraint
        WHERE conname = r.constraint_name
        AND conrelid = format('%I.%I', r.schema_name, r.table_name)::regclass;

        IF cnt = 0 THEN
            DELETE FROM _timescaledb_catalog.chunk_constraint
            WHERE chunk_id = r.chunk_id AND constraint_name = r.constraint_name;
        END IF;
    END LOOP;
END$$;

DO $$
DECLARE 
    r record;
    cnt INTEGER;
BEGIN
    FOR r IN 
        SELECT * 
        FROM _timescaledb_catalog.chunk_index cc
        INNER JOIN _timescaledb_catalog.chunk c ON (c.id = cc.chunk_id)
    LOOP
        SELECT count(*) INTO cnt
        FROM pg_class
        WHERE relname = r.index_name;

        IF cnt = 0 THEN
            DELETE FROM _timescaledb_catalog.chunk_index
            WHERE chunk_id = r.chunk_id AND index_name = r.index_name;
        END IF;
    END LOOP;
END$$;
