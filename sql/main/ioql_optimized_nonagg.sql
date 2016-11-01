--------------- PARTITION FUNCTIONS ------------

-- The quey plan for this query is as follows:
--   1) Scan the top 10000 rows of table and try to fulfil the query with those items.
--   2) Then for every by_every item not fulfilled, try to scan for it, using its index.
CREATE OR REPLACE FUNCTION ioql_query_local_partition_rows_limit_by_every(query ioql_query,
                                                                          part  namespace_partition_type)
    RETURNS SETOF RECORD LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    cnt                       INT;
    total_cnt                 INT := 0;
    distinct_cnt              INT := 0;
    data_table_row            data_table;
    query_sql_scan_base_table TEXT;
    query_sql_scan            TEXT;
    query_sql_jump            TEXT;
    distinct_value_cnt        JSONB;
    distinct_value_original   JSONB;
BEGIN
    IF get_partitioning_field_name(query.namespace_name) = (query.limit_by_field).field THEN
        SELECT jsonb_object_agg(value, (query.limit_by_field).count)
        INTO distinct_value_cnt
        FROM get_distinct_values_local(query.namespace_name, (query.limit_by_field).field) AS value
        WHERE get_partition_for_key(value, part.total_partitions) = part.partition_number;
    ELSE
        SELECT jsonb_object_agg(value, (query.limit_by_field).count)
        INTO distinct_value_cnt
        FROM get_distinct_values_local(query.namespace_name, (query.limit_by_field).field) AS value;
    END IF;

    distinct_value_original := distinct_value_cnt;

    EXECUTE format(
        $$
            CREATE TEMP TABLE results (%s)
        $$,
        get_result_column_def_list_nonagg(query));

    SELECT count(*)
    INTO distinct_cnt
    FROM jsonb_object_keys(distinct_value_original);

    query_sql_scan_base_table :=
    -- query for easy limit 10000
    base_query_raw(
        get_full_select_clause_nonagg(query),
        'FROM %1$s',
        get_where_clause(
            get_time_predicate(query.time_condition)
        ),
        NULL,
        'ORDER BY time DESC NULLS LAST',
        'LIMIT ' || (distinct_cnt * (query.limit_by_field).count * 2));


    query_sql_scan :=
    format(
        $$
            %s
            FROM (%s) simple_scan
            %s
        $$,
        get_full_select_clause_nonagg(query),
        query_sql_scan_base_table,
        get_where_clause(
            get_field_predicate_clause(query.field_condition),
            get_select_field_predicate(query.select_items),
            format('%s IS NOT NULL', (query.limit_by_field).field)
        )
    );

    query_sql_jump :=
    base_query_raw(
        get_full_select_clause_nonagg(query),
        'FROM %1$s',
        get_where_clause(
            default_predicates(query, part.total_partitions),
            format('%1$I::text = %2$s AND %1$I IS NOT NULL', (query.limit_by_field).field,
                   'distinct_value.key'),
            '(time < min_table.min_time OR min_table.min_time IS NULL)'
        ),
        get_groupby_clause(query.aggregate),
        'ORDER BY time DESC NULLS LAST, ' || (query.limit_by_field).field,
        'LIMIT distinct_value.value::int');

    FOR data_table_row IN SELECT *
                          FROM get_data_tables_for_partitions_time_desc(part) LOOP
        RAISE NOTICE E'ioql_query_local_partition_rows_limit_by_every scan: \n % \n', format(query_sql_scan,
                                                                                             data_table_row.table_oid);
        EXECUTE format(
            $$
                WITH inserted as (
                    INSERT INTO results
                    %2$s
                    FROM
                    (
                        SELECT
                            ROW_NUMBER() OVER (PARTITION BY %3$s ORDER BY time DESC NULLS LAST) as rank,
                            (($1::jsonb)->>(%3$s::text))::int as cnt,
                            res.*
                        FROM (%1$s) as res
                    ) AS with_rank
                    WHERE rank <= cnt
                    RETURNING time
                )
                SELECT count(*)
                FROM inserted
            $$,
            format(query_sql_scan, data_table_row.table_oid),
            get_full_select_clause_nonagg(query),
            (query.limit_by_field).field
        )
        USING distinct_value_cnt INTO cnt;

        total_cnt := total_cnt + cnt;
        IF total_cnt >= query.limit_rows THEN
            EXIT;
        END IF;

        IF cnt > 0 THEN
            EXECUTE format(
                $$
                    SELECT jsonb_object_agg(original.key, original.value::int - coalesce(existing.cnt, 0))
                    FROM jsonb_each_text($1) as original
                    LEFT JOIN (
                        SELECT %1$s grp, count(*) cnt
                        FROM results
                        GROUP BY %1$s
                    ) as existing on (original.key = existing.grp)
                    WHERE  (original.value::int - coalesce(existing.cnt, 0)) > 0
                $$,
                (query.limit_by_field).field)
            USING distinct_value_original INTO distinct_value_cnt;

            SELECT count(*)
            INTO distinct_cnt
            FROM jsonb_object_keys(distinct_value_cnt);

            IF distinct_cnt = 0 THEN
                EXIT;
            END IF;
        END IF;


        RAISE NOTICE E'ioql_query_local_partition_rows_limit_by_every jump %:\n %\n',
        total_cnt, format(query_sql_jump, data_table_row.table_oid);

        EXECUTE format(
            $$
                INSERT INTO results
                SELECT every_jump.*
                FROM jsonb_each_text($1) as distinct_value
                LEFT JOIN (
                    SELECT %2$s::text as gp, min(time) as min_time
                    FROM results
                    GROUP BY %2$s
                ) min_table on (distinct_value.key = min_table.gp),
                LATERAL(%1$s) every_jump
            $$,
            format(query_sql_jump, data_table_row.table_oid),
            (query.limit_by_field).field)
        USING distinct_value_cnt;

        GET DIAGNOSTICS cnt := ROW_COUNT;
        total_cnt := total_cnt + cnt;
        IF total_cnt >= query.limit_rows THEN
            EXIT;
        END IF;

        IF cnt > 0 THEN
            EXECUTE format(
                $$
                    SELECT jsonb_object_agg(original.key, original.value::int - coalesce(existing.cnt, 0))
                    FROM jsonb_each_text($1) as original
                    LEFT JOIN (
                        SELECT %1$s grp, count(*) cnt
                        FROM results
                        GROUP BY %1$s
                    ) as existing on (original.key = existing.grp)
                    WHERE  (original.value::int - coalesce(existing.cnt, 0)) > 0
                $$,
                (query.limit_by_field).field)
            USING distinct_value_original INTO distinct_value_cnt;

            SELECT count(*)
            INTO distinct_cnt
            FROM jsonb_object_keys(distinct_value_cnt);

            IF distinct_cnt = 0 THEN
                EXIT;
            END IF;
        END IF;

    END LOOP;

    RETURN QUERY
    SELECT *
    FROM results
    LIMIT query.limit_rows;
    DROP TABLE results;
END
$BODY$
SET constraint_exclusion = ON;

CREATE OR REPLACE FUNCTION ioql_query_local_partition_rows_regular_limit(query ioql_query,
                                                                         part  namespace_partition_type)
    RETURNS SETOF RECORD LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    data_table_row data_table;
    cnt            INT;
    total_cnt      INT := 0;
    query_sql      TEXT;
BEGIN
    FOR data_table_row IN
    SELECT *
    FROM get_data_tables_for_partitions_time_desc(part) LOOP

        query_sql :=  base_query_raw(
            get_full_select_clause_nonagg(query),
            get_from_clause(data_table_row),
            get_where_clause(
                default_predicates(query, part.total_partitions)
            ),
            get_groupby_clause(query.aggregate),
            get_orderby_clause_nonagg(query),
            get_limit_clause(query.limit_rows - total_cnt)
        );

        RAISE NOTICE E'ioql_query_local_partition_rows_regular_limit table: %, SQL:\n% ', data_table_row, query_sql;
        RETURN QUERY EXECUTE query_sql;

        GET DIAGNOSTICS cnt := ROW_COUNT;
        total_cnt := total_cnt + cnt;
        IF total_cnt >= query.limit_rows THEN
            EXIT;
        END IF;
    END LOOP;
END
$BODY$
SET constraint_exclusion = ON;

CREATE OR REPLACE FUNCTION ioql_query_local_partition_rows(query ioql_query, part namespace_partition_type)
    RETURNS SETOF RECORD LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
BEGIN
    RAISE NOTICE E'ioql_query_local_partition_rows SQL:\n%', query;
    IF query.limit_by_field IS NULL THEN
        RETURN QUERY EXECUTE format(
            $$
                SELECT *
                FROM ioql_query_local_partition_rows_regular_limit($1, $2) AS res(%s)
            $$,
            get_result_column_def_list_nonagg(query))
        USING query, part;
    ELSE
        RETURN QUERY EXECUTE format(
            $$
                SELECT *
                FROM ioql_query_local_partition_rows_limit_by_every($1, $2) AS res(%s)
            $$,
            get_result_column_def_list_nonagg(query))
        USING query, part;
    END IF;
END
$BODY$;

--------------- NODE FUNCTIONS ------------

CREATE OR REPLACE FUNCTION ioql_query_local_node_nonagg(query ioql_query)
    RETURNS SETOF RECORD LANGUAGE PLPGSQL STABLE AS
$BODY$
BEGIN
    --each partition has limited correctly, but the union of partitions needs to be re-limited.
    IF NOT (query.limit_by_field IS NULL) THEN
        RETURN QUERY EXECUTE format(
            $$
                SELECT %3$s
                FROM
                (
                    SELECT
                        ROW_NUMBER() OVER (PARTITION BY %2$s ORDER BY time DESC NULLS LAST) as rank,
                        partition_results.*
                    FROM get_partitions_for_namespace($1.namespace_name) parts,
                    ioql_query_local_partition_rows($1, parts) as partition_results(%1$s)
                ) AS across_partitions
                WHERE rank <= ($1.limit_by_field).count
                ORDER BY time DESC NULLS LAST, %2$s
                LIMIT $1.limit_rows
            $$,
            get_result_column_def_list_nonagg(query),
            (query.limit_by_field).field,
            get_result_column_list_nonagg(query)
        )
        USING query;
    ELSE
        RETURN QUERY EXECUTE format(
            $$
                SELECT partition_results.*
                FROM get_partitions_for_namespace($1.namespace_name) parts,
                ioql_query_local_partition_rows($1, parts) as partition_results(%1$s)
                ORDER BY time DESC NULLS LAST
                LIMIT $1.limit_rows
            $$,
            get_result_column_def_list_nonagg(query))
        USING query;
    END IF;
END
$BODY$;

--------------- CLUSTER FUNCTIONS ------------

CREATE OR REPLACE FUNCTION ioql_exec_query_nonagg_without_limit(query ioql_query)
    RETURNS SETOF RECORD LANGUAGE PLPGSQL STABLE AS $BODY$
--function aggregates partials across nodes.
BEGIN
    RETURN QUERY EXECUTE FORMAT(
        $$
            SELECT *
            FROM
              ioql_query_nodes_individually (
                get_cluster_table($1.namespace_name), $1, 'ioql_query_local_node_nonagg', get_result_column_def_list_nonagg($1)
              ) as res(%1$s)
            ORDER BY time DESC NULLS LAST
            LIMIT $1.limit_rows
        $$,
        get_result_column_def_list_nonagg(query))
    USING query;
END
$BODY$;

CREATE OR REPLACE FUNCTION ioql_exec_query_nonagg(query ioql_query)
    RETURNS SETOF RECORD LANGUAGE PLPGSQL STABLE AS $BODY$
DECLARE
BEGIN
    IF query.limit_by_field IS NOT NULL THEN
        RETURN QUERY EXECUTE format(
            $$
                SELECT %4$s
                FROM (
                    SELECT
                        ROW_NUMBER() OVER (PARTITION BY %2$s ORDER BY time DESC NULLS LAST) AS rank,
                         *
                    FROM  ioql_exec_query_nonagg_without_limit($1) as ieq(%1$s)
                ) as ranked
                WHERE rank <= $2 OR time IS NULL
                ORDER BY time DESC NULLS LAST, %2$s
                LIMIT $1.limit_rows
            $$,
            get_result_column_def_list_nonagg(query),
            (query.limit_by_field).field,
            query,
            get_result_column_list_nonagg(query)
        )
        USING query, (query.limit_by_field).count;
    ELSE
        RETURN QUERY EXECUTE format(
            $$
                SELECT *
                FROM  ioql_exec_query_nonagg_without_limit($1) as ieq(%1$s)
                ORDER BY time DESC NULLS LAST
                LIMIT $1.limit_rows
            $$,
            get_result_column_def_list_nonagg(query))
        USING query;

    END IF;
END;
$BODY$;



