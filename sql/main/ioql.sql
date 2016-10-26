--------------------------------------
------------- TYPES ------------------
--------------------------------------
DROP TYPE IF EXISTS field_predicate_op CASCADE;
CREATE TYPE field_predicate_op AS ENUM ('=', '!=', '<=', '>=', '<', '>');

DROP TYPE IF EXISTS predicate_conjunctive CASCADE;
CREATE TYPE predicate_conjunctive AS ENUM ('AND', 'OR');

DROP TYPE IF EXISTS field_predicate CASCADE;
CREATE TYPE field_predicate AS (field TEXT, op field_predicate_op, constant TEXT);

DROP TYPE IF EXISTS namespace_type CASCADE;
CREATE TYPE namespace_type AS (project_id BIGINT, name TEXT, replica_no SMALLINT);

DROP TYPE IF EXISTS aggregate_function_type CASCADE;
CREATE TYPE aggregate_function_type AS ENUM ('AVG', 'SUM', 'COUNT', 'MAX', 'MIN');

DROP TYPE IF EXISTS select_item CASCADE;
CREATE TYPE select_item AS (field TEXT, func aggregate_function_type); --func is null for non aggregates

DROP TYPE IF EXISTS limit_by_field_type CASCADE;
CREATE TYPE limit_by_field_type AS (field TEXT, count INT);

DROP TYPE IF EXISTS aggregate_type CASCADE;
CREATE TYPE aggregate_type AS (group_time BIGINT, group_field TEXT);

DROP TYPE IF EXISTS field_condition_type CASCADE;
CREATE TYPE field_condition_type AS (conjunctive predicate_conjunctive, predicates field_predicate []);

DROP TYPE IF EXISTS time_condition_type CASCADE;
CREATE TYPE time_condition_type AS (from_time BIGINT, to_time BIGINT); --from_time inclusive; to_time exclusive\


DROP TYPE IF EXISTS ioql_query CASCADE;
CREATE TYPE ioql_query AS (
    project_id         BIGINT, -- NOT NULL
    namespace_name     TEXT, -- NOT NULL
    select_field       select_item [], -- NULL to return row, not null for aggregating
    aggregate          aggregate_type, --op, group_field and group_time
    time_condition     time_condition_type, --time limits, from and to
    field_condition    field_condition_type, -- field predicates combined with a conjunctive (AND/OR)
    limit_rows         INT, --regular limit (number of rows)
    limit_time_periods INT, --limit # of time periods, only aggregates
    limit_by_field     limit_by_field_type -- limit by every field value, only non-aggregate; field must be distinct
);

DROP TYPE IF EXISTS ioql_return CASCADE;
CREATE TYPE ioql_return AS (
    namespace         TEXT, --name of namespace
    field_name        TEXT, --name of field or NULL if returning a row
    group_field       TEXT, --the value of the field if grouping by field or if limit_by_field
    time              BIGINT, -- for non-aggregate, observation time; for aggregate, start of window
    return_value      TEXT, --the value of the field, row, or aggregate
    return_value_type REGTYPE --type of value: TEXT, DOUBLE PRECISION, BOOLEAN
);

---------------------------------------------
------------- CONSTRUCTORS ------------------
---------------------------------------------

CREATE OR REPLACE FUNCTION new_ioql_query(
    project_id         BIGINT, -- NOT NULL
    namespace_name     TEXT, -- NOT NULL
    select_field       select_item [] = NULL,
    aggregate          aggregate_type = NULL, --op, group_field and group_time for aggregation, if NULL, not aggregating
    time_condition     time_condition_type = NULL, --time limits, from and to
    field_condition    field_condition_type = NULL, -- field predicates combined with a conjunctive (AND/OR)
    limit_rows         INT = NULL, --regular limit (number of rows)
    limit_time_periods INT = NULL, --limit # of time periods, only aggregates
    limit_by_field     limit_by_field_type = NULL-- limit by every field value, only non-aggregate; field must be distinct
)
    RETURNS ioql_query AS $BODY$
--TODO convert empty select_item to NULL?
SELECT ROW (
       project_id,
       namespace_name,
       select_field,
       aggregate,
       time_condition,
       field_condition,
       limit_rows,
       limit_time_periods,
       limit_by_field
) :: ioql_query;
$BODY$ LANGUAGE 'sql' STABLE;


CREATE OR REPLACE FUNCTION new_select_item(
    field TEXT,
    func  aggregate_function_type = NULL
)
    RETURNS select_item AS $BODY$
SELECT ROW (field, func) :: select_item
$BODY$ LANGUAGE 'sql' IMMUTABLE;

CREATE OR REPLACE FUNCTION new_aggregate(
    group_time  BIGINT,
    group_field TEXT = NULL
)
    RETURNS aggregate_type AS $BODY$
SELECT ROW (group_time, group_field) :: aggregate_type
$BODY$ LANGUAGE 'sql' STABLE;

CREATE OR REPLACE FUNCTION new_time_condition(
    from_time BIGINT = NULL,
    to_time   BIGINT = NULL
)
    RETURNS time_condition_type AS $BODY$
SELECT ROW (from_time, to_time) :: time_condition_type
$BODY$ LANGUAGE 'sql' STABLE;

CREATE OR REPLACE FUNCTION new_field_predicate(
    field    TEXT, -- name of field
    op       field_predicate_op, -- enum of '=', '<=', '>=', '<', '>'
    constant TEXT --constant to compare to
)
    RETURNS field_predicate AS $BODY$
SELECT ROW (field, op, constant) :: field_predicate
$BODY$ LANGUAGE 'sql' STABLE;

CREATE OR REPLACE FUNCTION new_field_condition(
    conjunctive predicate_conjunctive,
    predicates  field_predicate []
)
    RETURNS field_condition_type AS $BODY$
SELECT ROW (conjunctive, predicates) :: field_condition_type
$BODY$ LANGUAGE 'sql' STABLE;

CREATE OR REPLACE FUNCTION new_limit_by_field(
    field TEXT, -- name of field
    count INT
)
    RETURNS limit_by_field_type AS $BODY$
SELECT ROW (field, count) :: limit_by_field_type
$BODY$ LANGUAGE 'sql' STABLE;


CREATE OR REPLACE FUNCTION get_field_list(query ioql_query)
    RETURNS TEXT AS
$BODY$
SELECT CASE
       WHEN query.select_field IS NULL THEN
           get_field_list(query.namespace_name)
       ELSE
           array_to_string(ARRAY(
                               (SELECT format('%I', field) AS field_name
                                FROM unnest(query.select_field))
                               UNION
                               (SELECT CASE WHEN (query.limit_by_field).field IS NOT NULL THEN format('%I',
                                                                                                      (query.limit_by_field).field) END)
                               ORDER BY field_name
                           ), ', ')
       END;
$BODY$
LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION get_fields_def(
    namespace_name NAME
)
    RETURNS TEXT [] LANGUAGE SQL STABLE AS
$BODY$
SELECT ARRAY(
    SELECT format($$%s %s$$, field_name, data_type)
    FROM (
             SELECT
                 f.name AS field_name,
                 f.data_type
             FROM field AS f
             WHERE f.namespace_name = get_fields_def.namespace_name
             ORDER BY f.name
         ) AS info
);
$BODY$;

CREATE OR REPLACE FUNCTION get_field_def(
    namespace_name NAME,
    field_name     NAME
)
    RETURNS TEXT [] LANGUAGE SQL STABLE AS
$BODY$
SELECT ARRAY(
    SELECT format($$%s %s$$, field_name, data_type)
    FROM (
             SELECT
                 f.name AS field_name,
                 f.data_type
             FROM field AS f
             WHERE f.namespace_name = get_field_def.namespace_name AND
                 f.name = field_name
             ORDER BY f.name
         ) AS info
);
$BODY$;

CREATE OR REPLACE FUNCTION get_column_def_list(query ioql_query)
    RETURNS TEXT AS
$BODY$
SELECT CASE
       WHEN query.select_field IS NULL THEN
           array_to_string(ARRAY ['time bigint'] || get_fields_def(query.namespace_name), ', ')
       ELSE
           array_to_string(
               ARRAY ['time bigint'] ||
               ARRAY(
                   (
                       SELECT get_field_def(query.namespace_name, field) AS field_name
                       FROM unnest(query.select_field)
                   )
                   UNION
                   (
                       SELECT CASE
                              WHEN (query.limit_by_field).field IS NOT NULL THEN
                                  get_field_def(query.namespace_name,
                                                (query.limit_by_field).field)
                              END
                   )
                   ORDER BY field_name
               ), ', ')
       END;
$BODY$
LANGUAGE SQL STABLE;

