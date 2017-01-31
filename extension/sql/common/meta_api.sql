CREATE OR REPLACE FUNCTION _iobeamdb_meta_api.create_hypertable(
    main_schema_name        NAME,
    main_table_name         NAME,
    time_column_name        NAME,
    time_column_type        REGTYPE,
    partitioning_column     NAME,
    replication_factor      SMALLINT,
    number_partitions       SMALLINT,
    associated_schema_name  NAME,
    associated_table_prefix NAME,
    placement               _iobeamdb_catalog.chunk_placement_type,
    chunk_size_bytes        BIGINT
)
    RETURNS _iobeamdb_catalog.hypertable LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    hypertable_row _iobeamdb_catalog.hypertable;
BEGIN
    SELECT (res::_iobeamdb_catalog.hypertable).*
        INTO hypertable_row
        FROM _iobeamdb_internal.meta_transaction_exec_with_return(
            format('SELECT t FROM _iobeamdb_meta.create_hypertable(%L, %L, %L, %L, %L, %L, %L, %L, %L, %L, %L, %L) t ',
                main_schema_name,
                main_table_name,
                time_column_name,
                time_column_type,
                partitioning_column,
                replication_factor,
                number_partitions,
                associated_schema_name,
                associated_table_prefix,
                placement,
                chunk_size_bytes,
                current_database()
            )
        ) AS res;

    RETURN hypertable_row;
END
$BODY$;

CREATE OR REPLACE FUNCTION _iobeamdb_meta_api.drop_hypertable(
    schema_name NAME,
    table_name NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    PERFORM
    _iobeamdb_internal.meta_transaction_exec(
        format('SELECT _iobeamdb_meta.drop_hypertable(%L, %L, %L)',
            schema_name,
            table_name,
            current_database()
        )
    );
END
$BODY$;

CREATE OR REPLACE FUNCTION _iobeamdb_meta_api.add_column(
    hypertable_id   INTEGER,
    column_name     NAME,
    attnum          INT2,
    data_type       REGTYPE,
    default_value   TEXT,
    not_null        BOOLEAN
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    PERFORM
    _iobeamdb_internal.meta_transaction_exec(
        format('SELECT _iobeamdb_meta.add_column(%L, %L, %L, %L, %L, %L, %L)',
            hypertable_id,
            column_name,
            attnum,
            data_type,
            default_value,
            not_null,
            current_database()
        )
    );
END
$BODY$;

CREATE OR REPLACE FUNCTION _iobeamdb_meta_api.drop_column(
    hypertable_id   INTEGER,
    column_name     NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    PERFORM
    _iobeamdb_internal.meta_transaction_exec(
        format('SELECT _iobeamdb_meta.drop_column(%L, %L, %L)',
            hypertable_id,
            column_name,
            current_database()
        )
    );
END
$BODY$;

CREATE OR REPLACE FUNCTION _iobeamdb_meta_api.add_index(
    hypertable_id    INTEGER,
    main_schema_name NAME,
    main_index_name  NAME,
    definition       TEXT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    PERFORM
    _iobeamdb_internal.meta_transaction_exec(
        format('SELECT _iobeamdb_meta.add_index(%L, %L, %L, %L, %L)',
            hypertable_id,
            main_schema_name,
            main_index_name,
            definition,
            current_database()
        )
    );
END
$BODY$;

CREATE OR REPLACE FUNCTION _iobeamdb_meta_api.drop_index(
    main_schema_name NAME,
    main_index_name  NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    PERFORM
    _iobeamdb_internal.meta_transaction_exec(
        format('SELECT _iobeamdb_meta.drop_index(%L, %L, %L)',
            main_schema_name,
            main_index_name,
            current_database()
        )
    );
END
$BODY$;


CREATE OR REPLACE FUNCTION _iobeamdb_meta_api.alter_table_rename_column(
    hypertable_id   INTEGER,
    old_column_name NAME,
    new_column_name NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    PERFORM
    _iobeamdb_internal.meta_transaction_exec(
        format('SELECT _iobeamdb_meta.alter_table_rename_column(%L, %L, %L, %L)',
            hypertable_id,
            old_column_name,
            new_column_name,
            current_database()
        )
    );
END
$BODY$;

CREATE OR REPLACE FUNCTION _iobeamdb_meta_api.alter_column_set_default(
    hypertable_id     INTEGER,
    column_name       NAME,
    new_default_value TEXT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    PERFORM
    _iobeamdb_internal.meta_transaction_exec(
        format('SELECT _iobeamdb_meta.alter_column_set_default(%L, %L, %L, %L)',
            hypertable_id,
            column_name,
            new_default_value,
            current_database()
        )
    );
END
$BODY$;

CREATE OR REPLACE FUNCTION _iobeamdb_meta_api.alter_column_set_not_null(
    hypertable_id   INTEGER,
    column_name     NAME,
    new_not_null    BOOLEAN
)
RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    PERFORM
    _iobeamdb_internal.meta_transaction_exec(
        format('SELECT _iobeamdb_meta.alter_column_set_not_null(%L, %L, %L, %L)',
            hypertable_id,
            column_name,
            new_not_null,
            current_database()
        )
    );
END
$BODY$;

-- *immediate functions are not transactional.
CREATE OR REPLACE FUNCTION _iobeamdb_meta_api.close_chunk_end_immediate(
    chunk_id INT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    --This should use the non-transactional rpc because this needs to commit before we can take a lock
    --for writing on the closed chunk. That means this operation is not transactional with the insert and will not be rolled back.
    PERFORM _iobeamdb_internal.meta_immediate_commit_exec_with_return(
        format('SELECT * FROM _iobeamdb_meta.close_chunk_end(%L)', chunk_id)
    );
END
$BODY$;

CREATE OR REPLACE FUNCTION _iobeamdb_meta_api.get_or_create_chunk_immediate(
    partition_id INT,
    time_point   BIGINT
)
    RETURNS _iobeamdb_catalog.chunk LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_row _iobeamdb_catalog.chunk;
BEGIN
    --This should use the non-transactional rpc because this needs to see the results of this call
    --to make progress.
    SELECT (res::_iobeamdb_catalog.chunk).* INTO chunk_row
    FROM _iobeamdb_internal.meta_immediate_commit_exec_with_return(
        format('SELECT t FROM _iobeamdb_meta.get_or_create_chunk(%L, %L) t ', partition_id, time_point)
    ) AS res;

    RETURN chunk_row;
END
$BODY$;
