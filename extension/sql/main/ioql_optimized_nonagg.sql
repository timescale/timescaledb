--------------- PARTITION FUNCTIONS ------------

-- The quey plan for this query is as follows:
--   1) Scan the top 10000 rows of table and try to fulfil the query with those items.
--   2) Then for every by_every item not fulfilled, try to scan for it, using its index.
CREATE OR REPLACE FUNCTION ioql_query_local_partition_rows_limit_by_every_sql(
    query ioql_query,
    epoch _iobeamdb_catalog.partition_epoch,
    pr    _iobeamdb_catalog.partition_replica
)
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    distinct_value_sql        TEXT;
    index                     INT = 0;
    previous_tables           TEXT = '';
    code                      TEXT = '';
    time_col_name             NAME;
    time_col_type             REGTYPE;

    query_sql_scan_base_table TEXT = '';
    query_sql_scan            TEXT = '';
    query_sql_jump            TEXT = '';
    crn_row                   _iobeamdb_catalog.chunk_replica_node;
    partition_row             _iobeamdb_catalog.partition;
BEGIN
    time_col_name := get_time_column(query.hypertable_name);
    time_col_type := get_time_column_type(query.hypertable_name);

    IF epoch.partitioning_column = (query.limit_by_column).column_name THEN
        SELECT *
        INTO STRICT partition_row
        FROM _iobeamdb_catalog.partition p
        WHERE p.id = pr.partition_id;

        distinct_value_sql = format(
            $$
              SELECT value AS value, %2$L::bigint AS cnt
              FROM get_distinct_values_local(%3$L, %4$L, %1$L) AS value
              WHERE get_partition_for_key(value, %5$L) BETWEEN %6$L AND %7$L
            $$,
            (query.limit_by_column).column_name,
            (query.limit_by_column).count,
            pr.hypertable_name,
            pr.replica_id,
            epoch.partitioning_mod,
            partition_row.keyspace_start,
            partition_row.keyspace_end
        );
    ELSE
        distinct_value_sql = format(
            $$
              SELECT value, %2$L::bigint as cnt
              FROM get_distinct_values_local(%3$L, %4$L, %1$L) AS value
            $$,
            (query.limit_by_column).column_name,
            (query.limit_by_column).count,
            pr.hypertable_name,
            pr.replica_id
        );
    END IF;


    code = format(
        $$
        WITH distinct_value AS (%s)
        $$, distinct_value_sql);

    query_sql_scan_base_table :=
    -- query for easy limit 10000
    base_query_raw(
        get_full_select_clause_nonagg(query),
        'FROM %1$s',
        get_where_clause(
            get_time_predicate(time_col_name, time_col_type, query.time_condition)
        ),
        NULL,
        format('ORDER BY %I DESC NULLS LAST', time_col_name),
        format('LIMIT (SELECT count(*) * %L FROM distinct_value)', (query.limit_by_column).count * 2)
    );


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
            get_column_predicate_clause(query.column_condition),
            get_select_column_predicate(query.select_items),
            format('%s IS NOT NULL', (query.limit_by_column).column_name)
        )
    );

    query_sql_jump :=
    base_query_raw(
        get_full_select_clause_nonagg(query),
        'FROM %1$s',
        get_where_clause(
            default_predicates(query, epoch),
            format('%1$I::text = dv_counts_min_time.value AND %1$I IS NOT NULL', (query.limit_by_column).column_name),
            format('(%I < dv_counts_min_time.min_time OR dv_counts_min_time.min_time IS NULL)', time_col_name)
        ),
        get_groupby_clause(query.aggregate),
        'ORDER BY '||quote_ident(time_col_name)||' DESC NULLS LAST, ' || (query.limit_by_column).column_name,
        'LIMIT dv_counts_min_time.remaining_cnt');

    FOR crn_row IN SELECT *
                   FROM get_local_chunk_replica_node_for_pr_time_desc(pr)
    LOOP

        IF index = 0 THEN
            code := code || format(
                $$
                        , result_scan AS (
                            %1$s
                            FROM
                                (
                                    SELECT
                                        ROW_NUMBER() OVER (PARTITION BY %2$I ORDER BY %5$I DESC NULLS LAST) as rank,
                                    res.*
                                    FROM (%3$s) as res
                                ) AS with_rank
                                WHERE rank <= %4$L
                        )
                   $$,
                get_full_select_clause_nonagg(query),
                (query.limit_by_column).column_name,
                format(query_sql_scan, format('%I.%I', crn_row.schema_name, crn_row.table_name)),
                (query.limit_by_column).count,
				time_col_name
            );

            previous_tables := 'SELECT * FROM result_scan';
        END IF;

        code := code || format(
            $$  , results_%3$s AS (
                    WITH dv_counts_min_time AS (
                        SELECT original.value AS value, original.cnt - coalesce(existing.cnt, 0) as remaining_cnt, min_time as min_time
                        FROM distinct_value as original
                        LEFT JOIN (
                            SELECT %1$s::text AS value, count(*) AS cnt, min(%5$I) AS min_time
                            FROM (%2$s) AS previous_results
                            GROUP BY %1$s
                        ) as existing on (original.value = existing.value)
                        WHERE  (original.cnt - coalesce(existing.cnt, 0)) > 0
                    )
                    SELECT every_jump.*
                    FROM dv_counts_min_time,
                     LATERAL (%4$s) AS every_jump
                    )
                $$,

            (query.limit_by_column).column_name,
            previous_tables,
            index,
            format(query_sql_jump, format('%I.%I', crn_row.schema_name, crn_row.table_name)),
			time_col_name
        );

        previous_tables := previous_tables || format(' UNION ALL SELECT * FROM results_%s', index);

        index := index + 1;
    END LOOP;
    code := code || previous_tables;
    RETURN code;
END
$BODY$;

CREATE OR REPLACE FUNCTION ioql_query_local_partition_rows_regular_limit_sql(
    query ioql_query,
    epoch _iobeamdb_catalog.partition_epoch,
    pr    _iobeamdb_catalog.partition_replica
)
    RETURNS TEXT LANGUAGE SQL STABLE AS
$BODY$
SELECT format(
    $$
        SELECT *
        FROM (%s) combined_partition
        %s
    $$,
    string_agg(code, ' UNION ALL '),
    get_limit_clause(query.limit_rows)
)
FROM (
         SELECT '(' || base_query_raw(
             get_full_select_clause_nonagg(query),
             get_from_clause(crn),
             get_where_clause(
                 default_predicates(query, epoch)
             ),
             get_groupby_clause(query.aggregate),
             get_orderby_clause_nonagg(query),
             get_limit_clause(query.limit_rows)
         ) || ')' AS code
         FROM get_local_chunk_replica_node_for_pr_time_desc(pr) AS crn
     ) AS code_per_crn
HAVING string_agg(code, ' UNION ALL ') IS NOT NULL
$BODY$
SET constraint_exclusion = ON;

CREATE OR REPLACE FUNCTION ioql_query_local_partition_rows_sql(
    query ioql_query,
    epoch _iobeamdb_catalog.partition_epoch,
    pr    _iobeamdb_catalog.partition_replica
)
    RETURNS TEXT LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
BEGIN
    IF query.limit_by_column IS NULL THEN
        RETURN ioql_query_local_partition_rows_regular_limit_sql(query, epoch, pr);
    ELSE
        RETURN ioql_query_local_partition_rows_limit_by_every_sql(query, epoch, pr);
    END IF;
END
$BODY$;

--------------- NODE FUNCTIONS ------------

CREATE OR REPLACE FUNCTION ioql_query_local_node_nonagg_sql(
    query      ioql_query,
    epoch      _iobeamdb_catalog.partition_epoch,
    replica_id SMALLINT
)
    RETURNS TEXT LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    code TEXT;
BEGIN
    --each partition has limited correctly, but the union of partitions needs to be re-limited.
    IF NOT (query.limit_by_column IS NULL) THEN
        SELECT format(
            $$
                SELECT %3$s
                FROM
                (
                    SELECT
                        ROW_NUMBER() OVER (PARTITION BY %2$s ORDER BY %7$I DESC NULLS LAST) as rank,
                        combined_node.*
                    FROM (%4$s) as combined_node
                ) AS node_result
                WHERE rank <= %5$L
                ORDER BY %7$I DESC NULLS LAST, %2$s
                %6$s
            $$,
            get_result_column_def_list_nonagg(query),
            (query.limit_by_column).column_name,
            get_result_column_list_nonagg(query),
            string_agg(code_part, ' UNION ALL '),
            (query.limit_by_column).count,
            get_limit_clause(query.limit_rows),
			get_time_column(query.hypertable_name)
        )
        INTO STRICT code
        FROM
            (
                SELECT '(' || ioql_query_local_partition_rows_sql(query, epoch, pr) || ')' AS code_part
                FROM get_partition_replicas(epoch, replica_id) pr
            ) AS code_per_partition;
    ELSE
        SELECT format(
            $$
                SELECT *
                FROM  (%1$s) AS combined_node
                ORDER BY %3$I DESC NULLS LAST
                %2$s
            $$,
            string_agg(code_part, ' UNION ALL '),
            get_limit_clause(query.limit_rows),
			get_time_column(query.hypertable_name)
        )
        INTO STRICT code
        FROM
            (
                SELECT '(' || ioql_query_local_partition_rows_sql(query, epoch, pr) || ')' AS code_part
                FROM get_partition_replicas(epoch, replica_id) pr
            ) AS code_per_partition;
    END IF;
    RETURN code;
END
$BODY$;
