--------------------------------------
------------- TYPES ------------------
--------------------------------------
DO $CREATETYPES$
BEGIN
    LOCK pg_type;
    IF NOT EXISTS(SELECT 1
                  FROM pg_type
                  WHERE typname = 'column_predicate_op') THEN
        CREATE TYPE column_predicate_op AS ENUM ('=', '!=', '<=', '>=', '<', '>');

        CREATE TYPE predicate_conjunctive AS ENUM ('AND', 'OR');

        CREATE TYPE column_predicate AS (column_name TEXT, op column_predicate_op, constant TEXT);

        CREATE TYPE aggregate_function_type AS ENUM ('AVG', 'SUM', 'COUNT', 'MAX', 'MIN');

        CREATE TYPE select_item AS (column_name NAME, func aggregate_function_type); --func is null for non aggregates

        CREATE TYPE limit_by_column_type AS (column_name TEXT, count INT);

        --Grouptime is in usec if timestamp type used for time column in the data.
        --If time is numeric, unit used by grouptime needs to match units in the data inserted
        --(this is determined by user: inserted data can be in sec, usec, nanosec, etc.).
        CREATE TYPE aggregate_type AS (group_time BIGINT, group_column TEXT);

        CREATE TYPE column_condition_type AS (conjunctive predicate_conjunctive, predicates column_predicate []);

        --From_time/to_time is in usec if timestamp type used for time column in the data.
        --If time is numeric, unit used by these columns needs to match units in the data inserted
        --(this is determined by user: inserted data can be in sec, usec, nanosec, etc.).
        CREATE TYPE time_condition_type AS (from_time BIGINT, to_time BIGINT); --from_time inclusive; to_time exclusive

        CREATE TYPE ioql_query AS (
            hypertable_name     TEXT, -- NOT NULL
            select_items        select_item [], -- NULL to return row, not null for aggregating
            aggregate           aggregate_type, --op, group_column and group_time
            time_condition      time_condition_type, --time limits, from and to
            column_condition    column_condition_type, -- column predicates combined with a conjunctive (AND/OR)
            limit_rows          INT, --regular limit (number of rows)
            limit_time_periods  INT, --limit # of time periods, only aggregates
            limit_by_column      limit_by_column_type, -- limit by every column value, only non-aggregate; column must be distinct
            timezone            TEXT
        );

        CREATE TYPE time_range AS (start_time BIGINT, end_time BIGINT);

        CREATE TYPE hypertable_partition_type AS (hypertable_name NAME, partition_number SMALLINT, total_partitions SMALLINT);
    END IF;
END
$CREATETYPES$;
---------------------------------------------
------------- CONSTRUCTORS ------------------
---------------------------------------------

CREATE OR REPLACE FUNCTION new_ioql_query(
    hypertable_name     TEXT, -- NOT NULL
    select_items        select_item [] = NULL,
    aggregate           aggregate_type = NULL, --op, group_column and group_time for aggregation, if NULL, not aggregating
    time_condition      time_condition_type = NULL, --time limits, from and to
    column_condition    column_condition_type = NULL, -- column predicates combined with a conjunctive (AND/OR)
    limit_rows          INT = NULL, --regular limit (number of rows)
    limit_time_periods  INT = NULL, --limit # of time periods, only aggregates
    limit_by_column     limit_by_column_type = NULL-- limit by every column value, only non-aggregate; column must be distinct
)
    RETURNS ioql_query AS $BODY$
--TODO convert empty select_item to NULL?
SELECT ROW (
       hypertable_name,
       select_items,
       aggregate,
       time_condition,
       column_condition,
       limit_rows,
       limit_time_periods,
       limit_by_column,
       current_setting('timezone')
) :: ioql_query;
$BODY$ LANGUAGE 'sql' STABLE;


CREATE OR REPLACE FUNCTION new_select_item(
    column_name TEXT,
    func  aggregate_function_type = NULL
)
    RETURNS select_item AS $BODY$
SELECT ROW (column_name, func) :: select_item
$BODY$ LANGUAGE 'sql' IMMUTABLE;

CREATE OR REPLACE FUNCTION new_aggregate(
    group_time  BIGINT,
    group_column TEXT = NULL
)
    RETURNS aggregate_type AS $BODY$
SELECT ROW (group_time, group_column) :: aggregate_type
$BODY$ LANGUAGE 'sql' STABLE;

CREATE OR REPLACE FUNCTION new_time_condition(
    from_time BIGINT = NULL,
    to_time   BIGINT = NULL
)
    RETURNS time_condition_type AS $BODY$
SELECT ROW (from_time, to_time) :: time_condition_type
$BODY$ LANGUAGE 'sql' STABLE;

CREATE OR REPLACE FUNCTION new_column_predicate(
    column_name TEXT, -- name of column
    op          column_predicate_op, -- enum of '=', '<=', '>=', '<', '>'
    constant    TEXT --constant to compare to
)
    RETURNS column_predicate AS $BODY$
SELECT ROW (column_name, op, constant) :: column_predicate
$BODY$ LANGUAGE 'sql' STABLE;

CREATE OR REPLACE FUNCTION new_column_condition(
    conjunctive predicate_conjunctive,
    predicates  column_predicate []
)
    RETURNS column_condition_type AS $BODY$
SELECT ROW (conjunctive, predicates) :: column_condition_type
$BODY$ LANGUAGE 'sql' STABLE;

CREATE OR REPLACE FUNCTION new_limit_by_column(
    column_name TEXT, -- name of column
    count       INT
)
    RETURNS limit_by_column_type AS $BODY$
SELECT ROW (column_name, count) :: limit_by_column_type
$BODY$ LANGUAGE 'sql' STABLE;
