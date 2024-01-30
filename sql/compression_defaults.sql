-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.


-- This function return a jsonb with the following keys:
-- - columns: an array of column names that shold be used for segment by
-- - confidence: a number between 0 and 10 (most confident) indicating how sure we are.
-- - message: a message that should be displayed to the user to evaluate the result.
CREATE OR REPLACE FUNCTION _timescaledb_functions.get_segmentby_defaults(
    relation regclass
)
    RETURNS JSONB LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    _table_name NAME;
    _schema_name NAME;
    _hypertable_row _timescaledb_catalog.hypertable;
    _segmentby NAME;
    _cnt int;
BEGIN
    SELECT n.nspname, c.relname INTO STRICT _schema_name, _table_name
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.oid = c.relnamespace)
    WHERE c.oid = relation;

    SELECT * INTO STRICT _hypertable_row FROM _timescaledb_catalog.hypertable h WHERE h.table_name = _table_name AND h.schema_name = schema_name;

    --STEP 1 if column stats exist use unique indexes. Pick the column that comes first in any such indexes. Ties are broken arbitrarily.
    --Note: this will only pick a column that is NOT unique in a multi-column unique index.
    with index_attr as (
        SELECT
        a.attnum, min(a.pos) as pos
        FROM
            (select indkey, indnkeyatts from pg_catalog.pg_index where indisunique and indrelid = relation) i
        INNER JOIN LATERAL
            (select * from unnest(i.indkey) with ordinality) a(attnum, pos) ON (TRUE)
        WHERE a.pos <= i.indnkeyatts
        GROUP BY 1
    )
    SELECT
      a.attname INTO _segmentby
    FROM
      index_attr i
    INNER JOIN
      pg_attribute a on (a.attnum = i.attnum AND a.attrelid = relation)
    --right now stats are from the hypertable itself. Use chunks in the future.
    INNER JOIN pg_statistic s ON (s.staattnum = a.attnum and s.starelid = relation)
    WHERE
      a.attname NOT IN (SELECT column_name FROM _timescaledb_catalog.dimension d WHERE d.hypertable_id = _hypertable_row.id)
      AND s.stadistinct > 1
    ORDER BY i.pos
    LIMIT 1;

    IF FOUND THEN
        return json_build_object('columns', json_build_array(_segmentby), 'confidence', 10);
    END IF;


    --STEP 2 if column stats exist and no unique indexes use non-unique indexes. Pick the column that comes first in any such indexes. Ties are broken arbitrarily.
    with index_attr as (
        SELECT
        a.attnum, min(a.pos) as pos
        FROM
            (select indkey, indnkeyatts from pg_catalog.pg_index where NOT indisunique and indrelid = relation) i
        INNER JOIN LATERAL
            (select * from unnest(i.indkey) with ordinality) a(attnum, pos) ON (TRUE)
        WHERE a.pos <= i.indnkeyatts
        GROUP BY 1
    )
    SELECT
      a.attname INTO _segmentby
    FROM
      index_attr i
    INNER JOIN
      pg_attribute a on (a.attnum = i.attnum AND a.attrelid = relation)
    --right now stats are from the hypertable itself. Use chunks in the future.
    INNER JOIN pg_statistic s ON (s.staattnum = a.attnum and s.starelid = relation)
    WHERE
      a.attname NOT IN (SELECT column_name FROM _timescaledb_catalog.dimension d WHERE d.hypertable_id = _hypertable_row.id)
      AND s.stadistinct > 1
    ORDER BY i.pos
    LIMIT 1;

    IF FOUND THEN
        return json_build_object('columns', json_build_array(_segmentby), 'confidence', 8);
    END IF;

    --STEP 3 if column stats do not exist use non-unique indexes. Pick the column that comes first in any such indexes. Ties are broken arbitrarily.
    with index_attr as (
        SELECT
        a.attnum, min(a.pos) as pos
        FROM
            (select indkey, indnkeyatts from pg_catalog.pg_index where NOT indisunique and indrelid = relation) i
        INNER JOIN LATERAL
            (select * from unnest(i.indkey) with ordinality) a(attnum, pos) ON (TRUE)
        WHERE a.pos <= i.indnkeyatts
        GROUP BY 1
    )
    SELECT
      a.attname INTO _segmentby
    FROM
      index_attr i
    INNER JOIN
      pg_attribute a on (a.attnum = i.attnum AND a.attrelid = relation)
    LEFT JOIN
      pg_catalog.pg_attrdef ad ON (ad.adrelid = relation AND ad.adnum = a.attnum)
    LEFT JOIN
      pg_statistic s ON (s.staattnum = a.attnum and s.starelid = relation)
    WHERE
      a.attname NOT IN (SELECT column_name FROM _timescaledb_catalog.dimension d WHERE d.hypertable_id = _hypertable_row.id)
      AND s.stadistinct is null
      AND a.attidentity = '' AND (ad.adbin IS NULL OR pg_get_expr(adbin, adrelid) not like 'nextval%')
    ORDER BY i.pos
    LIMIT 1;

    IF FOUND THEN
        return json_build_object(
            'columns', json_build_array(_segmentby),
            'confidence', 5,
            'message',  'Please make sure '|| _segmentby||' is not a unique column and appropriate for a segment by');
    END IF;

    --STEP 4 if column stats do not exist and no non-unique indexes, use unique indexes. Pick the column that comes first in any such indexes. Ties are broken arbitrarily.
    with index_attr as (
        SELECT
        a.attnum, min(a.pos) as pos
        FROM
            (select indkey, indnkeyatts from pg_catalog.pg_index where indisunique and indrelid = relation) i
        INNER JOIN LATERAL
            (select * from unnest(i.indkey) with ordinality) a(attnum, pos) ON (TRUE)
        WHERE a.pos <= i.indnkeyatts
        GROUP BY 1
    )
    SELECT
      a.attname INTO _segmentby
    FROM
      index_attr i
    INNER JOIN
      pg_attribute a on (a.attnum = i.attnum AND a.attrelid = relation)
    LEFT JOIN
      pg_catalog.pg_attrdef ad ON (ad.adrelid = relation AND ad.adnum = a.attnum)
    LEFT JOIN
      pg_statistic s ON (s.staattnum = a.attnum and s.starelid = relation)
    WHERE
      a.attname NOT IN (SELECT column_name FROM _timescaledb_catalog.dimension d WHERE d.hypertable_id = _hypertable_row.id)
      AND s.stadistinct is null
      AND a.attidentity = '' AND (ad.adbin IS NULL OR pg_get_expr(adbin, adrelid) not like 'nextval%')
    ORDER BY i.pos
    LIMIT 1;

    IF FOUND THEN
            return json_build_object(
            'columns', json_build_array(_segmentby),
            'confidence', 5,
            'message',  'Please make sure '|| _segmentby||' is not a unique column and appropriate for a segment by');
    END IF;


    --are there any indexed columns that are not dimemsions and are not serial/identity?
    with index_attr as (
        SELECT
        a.attnum, min(a.pos) as pos
        FROM
            (select indkey, indnkeyatts from pg_catalog.pg_index where indisunique and indrelid = relation) i
        INNER JOIN LATERAL
            (select * from unnest(i.indkey) with ordinality) a(attnum, pos) ON (TRUE)
        WHERE a.pos <= i.indnkeyatts
        GROUP BY 1
    )
    SELECT
      count(*) INTO STRICT _cnt
    FROM
      index_attr i
    INNER JOIN
      pg_attribute a on (a.attnum = i.attnum AND a.attrelid = relation)
    LEFT JOIN
      pg_catalog.pg_attrdef ad ON (ad.adrelid = relation AND ad.adnum = a.attnum)
    WHERE
      a.attname NOT IN (SELECT column_name FROM _timescaledb_catalog.dimension d WHERE d.hypertable_id = _hypertable_row.id)
      AND a.attidentity = '' AND (ad.adbin IS NULL OR pg_get_expr(adbin, adrelid) not like 'nextval%');

    IF _cnt > 0 THEN
        --there are many potential candidates. We do not have enough information to choose one.
        return json_build_object(
            'columns', json_build_array(),
            'confidence', 0,
            'message',  'Several columns are potential segment by candidates and we do not have enough information to choose one. Please use the segment_by option to explicitly specify the segment_by column');
    ELSE
        --there are no potential candidates. There is a good chance no segment by is the correct choice.
        return json_build_object(
            'columns', json_build_array(),
            'confidence', 5,
            'message',  'You do not have any indexes on columns that can be used for segment_by and thus we are not using segment_by for compression. Please make sure you are not missing any indexes');
    END IF;
END
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- This function return a jsonb with the following keys:
-- - clauses: an array of column names and sort order key words that shold be used for order by.
-- - confidence: a number between 0 and 10 (most confident) indicating how sure we are.
-- - message: a message that should be shown to the user to evaluate the result.
CREATE OR REPLACE FUNCTION _timescaledb_functions.get_orderby_defaults(
    relation regclass, segment_by_cols text[]
)
    RETURNS JSONB LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    _table_name NAME;
    _schema_name NAME;
    _hypertable_row _timescaledb_catalog.hypertable;
    _orderby_names NAME[];
    _dimension_names NAME[];
    _first_index_attrs NAME[];
    _orderby_clauses text[];
    _confidence int;
BEGIN
    SELECT n.nspname, c.relname INTO STRICT _schema_name, _table_name
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.oid = c.relnamespace)
    WHERE c.oid = relation;

    SELECT * INTO STRICT _hypertable_row FROM _timescaledb_catalog.hypertable h WHERE h.table_name = _table_name AND h.schema_name = schema_name;

    --start with the unique index columns minus the segment by columns
    with index_attr as (
        SELECT
        a.attnum, min(a.pos) as pos
        FROM
             --is there a better way to pick the right unique index if there are multiple?
            (select indkey, indnkeyatts from pg_catalog.pg_index where indisunique and indrelid = relation limit 1) i
        INNER JOIN LATERAL
            (select * from unnest(i.indkey) with ordinality) a(attnum, pos) ON (TRUE)
        WHERE a.pos <= i.indnkeyatts
        GROUP BY 1
    )
    SELECT
      array_agg(a.attname ORDER BY i.pos) INTO _orderby_names
    FROM
      index_attr i
    INNER JOIN
      pg_attribute a on (a.attnum = i.attnum AND a.attrelid = relation)
    WHERE
      NOT(a.attname::text = ANY (segment_by_cols));

    if _orderby_names is null then
        _orderby_names := array[]::name[];
        _confidence := 5;
    else
        _confidence := 8;
    end if;

    --add dimension colomns to the end. A dimension column like time should probably always be part of the order by.
    SELECT
      array_agg(d.column_name) INTO _dimension_names
    FROM _timescaledb_catalog.dimension d
    WHERE
      d.hypertable_id = _hypertable_row.id
      AND NOT(d.column_name::text = ANY (_orderby_names))
      AND NOT(d.column_name::text = ANY (segment_by_cols));
    _orderby_names := _orderby_names || _dimension_names;

    --add the first attribute of any index
    with index_attr as (
        SELECT
        a.attnum, min(a.pos) as pos
        FROM
            (select indkey, indnkeyatts from pg_catalog.pg_index where indrelid = relation) i
        INNER JOIN LATERAL
            (select * from unnest(i.indkey) with ordinality) a(attnum, pos) ON (TRUE)
        WHERE a.pos = 1
        GROUP BY 1
    )
    SELECT
      array_agg(a.attname ORDER BY i.pos) INTO _first_index_attrs
    FROM
      index_attr i
    INNER JOIN
      pg_attribute a on (a.attnum = i.attnum AND a.attrelid = relation)
    WHERE
          NOT(a.attname::text = ANY (_orderby_names))
      AND NOT(a.attname::text = ANY (segment_by_cols));

    _orderby_names := _orderby_names || _first_index_attrs;

    --add DESC to any dimensions
    SELECT
      array_agg(
      CASE WHEN d.column_name IS NULL THEN
        a.colname
      ELSE
        a.colname || ' DESC'
      END ORDER BY pos) INTO STRICT _orderby_clauses
    FROM unnest(_orderby_names) WITH ORDINALITY as a(colname, pos)
    LEFT JOIN _timescaledb_catalog.dimension d ON (d.column_name = a.colname AND d.hypertable_id = _hypertable_row.id);


    return json_build_object('clauses', _orderby_clauses, 'confidence', _confidence);
END
$BODY$ SET search_path TO pg_catalog, pg_temp;