--the columns (w/o time, which isn't a column) the sql will return
--should return unquoted names
CREATE OR REPLACE FUNCTION get_result_column_array_nonagg(query ioql_query)
    RETURNS NAME [] LANGUAGE SQL STABLE AS
$BODY$
SELECT CASE
       WHEN query.select_items IS NULL THEN
           get_column_names(query.hypertable_name)
       ELSE
           ARRAY(
               SELECT *
               FROM
                   (
                       (
                          SELECT time_column AS column_name
                          FROM get_time_column(query.hypertable_name) time_column
                          WHERE time_column NOT IN (
                            SELECT column_name
                            FROM unnest(query.select_items)
                          )
                       )
                      UNION ALL
                       (
                           SELECT column_name
                           FROM unnest(query.select_items)
                       )
                       UNION ALL
                       (
                           SELECT CASE
                                  WHEN
                                      (query.limit_by_column).column_name IS NOT NULL AND
                                      NOT EXISTS(
                                          SELECT 1
                                          FROM unnest(query.select_items)
                                          WHERE column_name = (query.limit_by_column).column_name
                                      ) THEN
                                      (query.limit_by_column).column_name
                                  END
                       )
                   ) AS _x
               WHERE column_name IS NOT NULL
           )
       END;
$BODY$;

CREATE OR REPLACE FUNCTION quote_names(names NAME [])
    RETURNS TEXT [] LANGUAGE SQL STABLE AS
$BODY$
SELECT array_agg(format('%I', name)
ORDER BY ordinality)
FROM unnest(names) WITH ORDINALITY AS name;
$BODY$;

--all the columns the sql will return as a comma-delim string;
CREATE OR REPLACE FUNCTION get_result_column_list_nonagg(query ioql_query)
    RETURNS TEXT LANGUAGE SQL STABLE AS
$BODY$
SELECT array_to_string(quote_names(get_result_column_array_nonagg(query)), ', ')
$BODY$;

--all columns and their types
CREATE OR REPLACE FUNCTION get_result_column_def_array_nonagg(query ioql_query)
    RETURNS TEXT [] LANGUAGE SQL STABLE AS
$BODY$
SELECT ARRAY(
    SELECT format('%I %s', column_name, data_type)
    FROM get_column_names_and_types(query.hypertable_name,
                                   get_result_column_array_nonagg(query)) AS ft(column_name, data_type)
)
$BODY$;

--all columns and their types as a comma-delim list
CREATE OR REPLACE FUNCTION get_result_column_def_list_nonagg(query ioql_query)
    RETURNS TEXT LANGUAGE SQL STABLE AS
$BODY$
SELECT array_to_string(get_result_column_def_array_nonagg(query), ', ')
$BODY$;
