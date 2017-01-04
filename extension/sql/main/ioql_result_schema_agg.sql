--the final name of the aggregate column
--ex avg(temperature)
--collisions here shouldn't cause errors
CREATE OR REPLACE FUNCTION get_result_aggregate_column_name(field_name NAME, func aggregate_function_type)
    RETURNS NAME AS $BODY$
SELECT format('%s(%s)', lower(func :: TEXT), substr(field_name, 1, (63 - 2) - (char_length(func :: TEXT)))) :: NAME;
$BODY$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION get_result_field_def_item_agg(item select_item)
    RETURNS TEXT AS $BODY$
SELECT CASE
       WHEN item.func = 'AVG' THEN
           format('%I double precision',
                  get_result_aggregate_column_name(item.field, item.func))
       WHEN item.func = 'COUNT' THEN
           format('%I numeric',
                  get_result_aggregate_column_name(item.field, item.func))
       ELSE
           format('%I double precision',
                  get_result_aggregate_column_name(item.field, item.func))
       END;
$BODY$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION get_result_column_def_array_agg(query ioql_query)
    RETURNS TEXT [] AS
$BODY$
SELECT CASE
       WHEN (query.aggregate).group_field IS NOT NULL THEN
           ARRAY [
           format('%I %s', (query.aggregate).group_field,
                  get_field_type(query.namespace_name, (query.aggregate).group_field)),
           'time '|| get_time_field_type(query.namespace_name) ] ||
           field_array
       ELSE
           ARRAY ['time '|| get_time_field_type(query.namespace_name)] ||
           field_array
       END
FROM
    (
        SELECT ARRAY(
                   SELECT get_result_field_def_item_agg(item)
                   FROM unnest(query.select_items) AS item
               ) AS field_array
    ) AS x
$BODY$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION get_result_column_def_list_agg(query ioql_query)
    RETURNS TEXT AS
$BODY$
SELECT array_to_string(get_result_column_def_array_agg(query), ', ')
$BODY$ LANGUAGE SQL IMMUTABLE STRICT;
