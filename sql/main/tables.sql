CREATE SCHEMA IF NOT EXISTS "cluster";

CREATE UNLOGGED TABLE IF NOT EXISTS public.version (
    versionNo INT
);

CREATE TABLE IF NOT EXISTS public.cluster_name (
    name TEXT
);

BEGIN WORK;
LOCK TABLE public.cluster_name IN EXCLUSIVE MODE;
DO $$
BEGIN
    IF (SELECT count(*)
        FROM public.cluster_name) = 0
    THEN EXECUTE 'INSERT INTO public.cluster_name VALUES (''none'')';
    END IF;
END $$;
COMMIT WORK;

CREATE TABLE IF NOT EXISTS data_tables (
    table_name       REGCLASS PRIMARY KEY NOT NULL,
    project_id       BIGINT               NOT NULL,
    namespace        TEXT                 NOT NULL,
    replica_no       SMALLINT             NOT NULL CHECK (replica_no >= 0),
    partition        SMALLINT             NOT NULL CHECK (partition >= 0),
    total_partitions SMALLINT             NOT NULL CHECK (total_partitions > 0),
    start_time       BIGINT,
    end_time         BIGINT,
    UNIQUE (project_id, namespace, replica_no, partition, start_time, end_time), --creates an index
    CHECK (start_time IS NOT NULL OR end_time IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS distinct_tables (
    table_name REGCLASS PRIMARY KEY NOT NULL,
    project_id BIGINT               NOT NULL,
    namespace  TEXT                 NOT NULL,
    replica_no SMALLINT             NOT NULL CHECK (replica_no >= 0),
    --partition smallint NOT NULL CHECK(partition >= 0), --should this be partitioned
    UNIQUE (project_id, namespace, replica_no) --creates an index
);


CREATE TABLE IF NOT EXISTS data_fields (
    table_name      REGCLASS NOT NULL REFERENCES data_tables (table_name) ON DELETE CASCADE,
    field_name      TEXT     NOT NULL,
    field_type      REGTYPE  NOT NULL CHECK (field_type IN
                                             ('double precision' :: REGTYPE, 'text' :: REGTYPE, 'boolean' :: REGTYPE, 'bigint' :: REGTYPE)),
    is_distinct     BOOLEAN  NOT NULL DEFAULT FALSE,
    is_partitioning BOOLEAN  NOT NULL DEFAULT FALSE,
    PRIMARY KEY (table_name, field_name)
);

CREATE TABLE IF NOT EXISTS data_field_idx (
    table_name REGCLASS         NOT NULL REFERENCES data_tables (table_name) ON DELETE CASCADE,
    field_name TEXT             NOT NULL,
    idx_name   TEXT             NOT NULL, --not regclass since regclass create problems with database backup/restore (indexes created after data load)
    idx_type   field_index_type NOT NULL,
    PRIMARY KEY (table_name, field_name, idx_name),
    FOREIGN KEY (table_name, field_name) REFERENCES data_fields (table_name, field_name)
);


