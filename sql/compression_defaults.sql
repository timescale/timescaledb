-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.


-- This function returns the column names that should be used for segment by.
CREATE OR REPLACE FUNCTION _timescaledb_functions.get_segmentby_defaults(
    relation regclass
)
    RETURNS TEXT[] LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    _table_name NAME;
    _schema_name NAME;
    _hypertable_row _timescaledb_catalog.hypertable;
    _segmentby NAME;
BEGIN
    SELECT n.nspname, c.relname INTO STRICT _schema_name, _table_name
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.oid = c.relnamespace)
    WHERE c.oid = relation;

    SELECT * INTO STRICT _hypertable_row FROM _timescaledb_catalog.hypertable h WHERE h.table_name = _table_name AND h.schema_name = _schema_name;

    --STEP 1 if column stats exist use unique indexes.
    --Pick the column that comes first in any such indexes
    --Select the column such that tuples are segmented evenly across distinct values.
    --Note: this will only pick a column that is NOT unique in a multi-column unique index.
    with index_attr as (
      SELECT
        a.attnum, min(a.pos) as pos
      FROM (
        SELECT indkey, indnkeyatts
        FROM pg_catalog.pg_index
        WHERE indisunique AND indrelid = relation
      ) i
      INNER JOIN LATERAL (
        SELECT * FROM unnest(i.indkey) WITH ORDINALITY
      ) a(attnum, pos) ON TRUE
      WHERE a.pos <= i.indnkeyatts
      GROUP BY a.attnum
    ),
    stats_with_stddev as (
      SELECT
        a.attname,
        i.pos,
        ROUND(stddev_pop(freqs)::numeric, 5) as freq_stddev
      FROM index_attr i
      INNER JOIN pg_attribute a ON a.attnum = i.attnum AND a.attrelid = relation
      INNER JOIN pg_type t ON t.oid = a.atttypid
      INNER JOIN pg_stats s ON s.attname = a.attname
                            AND s.schemaname = _schema_name
                            AND s.tablename = _table_name
                            AND s.inherited = true
      LEFT JOIN LATERAL unnest(s.most_common_freqs) as freqs ON TRUE
      WHERE a.attname NOT IN (
        SELECT column_name
        FROM _timescaledb_catalog.dimension d
        WHERE d.hypertable_id = _hypertable_row.id
      )
      AND s.n_distinct > 1
      -- exclude date/time type category
      AND t.typcategory NOT IN ('D')
      GROUP BY a.attname, i.pos
    )
    SELECT attname
    INTO _segmentby
    FROM stats_with_stddev
    ORDER BY pos ASC, freq_stddev ASC NULLS LAST
    LIMIT 1;

    IF FOUND THEN
        return array[_segmentby::text];
    END IF;


    --STEP 2 if column stats exist and no unique indexes use non-unique indexes.
    --Pick the column that comes first in any such indexes
    --Select the column such that tuples are segmented evenly across distinct values.
    with index_attr as (
        SELECT
        a.attnum, min(a.pos) as pos
        FROM
            (select indkey, indnkeyatts from pg_catalog.pg_index where NOT indisunique and indrelid = relation) i
        INNER JOIN LATERAL
            (select * from unnest(i.indkey) with ordinality) a(attnum, pos) ON (TRUE)
        WHERE a.pos <= i.indnkeyatts
        GROUP BY 1
    ),
    stats_with_stddev as (
      SELECT
        a.attname,
        i.pos,
        ROUND(stddev_pop(freqs)::numeric, 5) as freq_stddev
      FROM index_attr i
      INNER JOIN pg_attribute a ON a.attnum = i.attnum AND a.attrelid = relation
      INNER JOIN pg_type t ON t.oid = a.atttypid
      INNER JOIN pg_stats s ON s.attname = a.attname
                            AND s.schemaname = _schema_name
                            AND s.tablename = _table_name
                            AND s.inherited = true
      LEFT JOIN LATERAL unnest(s.most_common_freqs) as freqs ON TRUE
      WHERE a.attname NOT IN (
        SELECT column_name
        FROM _timescaledb_catalog.dimension d
        WHERE d.hypertable_id = _hypertable_row.id
      )
      AND s.n_distinct > 1
      AND t.typcategory NOT IN ('D')
      GROUP BY a.attname, i.pos
    )
    SELECT attname
    INTO _segmentby
    FROM stats_with_stddev
    ORDER BY pos ASC, freq_stddev ASC NULLS LAST
    LIMIT 1;

    IF FOUND THEN
        return array[_segmentby::text];
    END IF;

    --STEP 3 if column stats exist but there are no indexes
    --Select the column such that tuples are segmented evenly across distinct values.
    with stats_with_stddev as (
      SELECT
        a.attname,
        ROUND(stddev_pop(freqs)::numeric, 5) as freq_stddev
      FROM pg_attribute a
      INNER JOIN pg_type t ON t.oid = a.atttypid
      INNER JOIN pg_stats s ON s.attname = a.attname
                            AND s.schemaname = _schema_name
                            AND s.tablename = _table_name
                            AND s.inherited = true
      LEFT JOIN LATERAL unnest(s.most_common_freqs) as freqs ON TRUE
      WHERE a.attrelid = relation
        AND a.attname NOT IN (
          SELECT column_name
          FROM _timescaledb_catalog.dimension d
          WHERE d.hypertable_id = _hypertable_row.id
        )
      AND s.n_distinct > 1
      AND t.typcategory NOT IN ('D')
      GROUP BY a.attname
    )
    SELECT attname
    INTO _segmentby
    FROM stats_with_stddev
    ORDER BY freq_stddev ASC NULLS LAST
    LIMIT 1;

    IF FOUND THEN
        return array[_segmentby::text];
    END IF;

    --STEP 4 if column stats do not exist use non-unique indexes. Pick the column that comes first in any such indexes. Ties are broken arbitrarily.
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
    INNER JOIN
      pg_type t ON t.oid = a.atttypid
    LEFT JOIN
      pg_catalog.pg_attrdef ad ON (ad.adrelid = relation AND ad.adnum = a.attnum)
    LEFT JOIN pg_stats s ON s.attname = a.attname
                          AND s.schemaname = _schema_name
                          AND s.tablename = _table_name
                          AND s.inherited = true
    WHERE
      a.attname NOT IN (SELECT column_name FROM _timescaledb_catalog.dimension d WHERE d.hypertable_id = _hypertable_row.id)
      AND s.n_distinct is null
      AND a.attidentity = '' AND (ad.adbin IS NULL OR pg_get_expr(adbin, adrelid) not like 'nextval%')
      AND t.typcategory NOT IN ('D')
    ORDER BY i.pos
    LIMIT 1;

    IF FOUND THEN
        return array[_segmentby::text];
    END IF;

    --STEP 5 if column stats do not exist and no non-unique indexes, use unique indexes. Pick the column that comes first in any such indexes. Ties are broken arbitrarily.
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
    INNER JOIN
      pg_type t ON t.oid = a.atttypid
    LEFT JOIN
      pg_catalog.pg_attrdef ad ON (ad.adrelid = relation AND ad.adnum = a.attnum)
    LEFT JOIN pg_stats s ON s.attname = a.attname
                          AND s.schemaname = _schema_name
                          AND s.tablename = _table_name
                          AND s.inherited = true
    WHERE
      a.attname NOT IN (SELECT column_name FROM _timescaledb_catalog.dimension d WHERE d.hypertable_id = _hypertable_row.id)
      AND s.n_distinct is null
      AND a.attidentity = '' AND (ad.adbin IS NULL OR pg_get_expr(adbin, adrelid) not like 'nextval%')
      AND t.typcategory NOT IN ('D')
    ORDER BY i.pos
    LIMIT 1;

    IF FOUND THEN
            return array[_segmentby::text];
    END IF;


    return array[]::text[];
END
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- This function returns the column names and sort order key words that should be used for order by.
CREATE OR REPLACE FUNCTION _timescaledb_functions.get_orderby_defaults(
    relation regclass, segment_by_cols text[]
)
    RETURNS TEXT[] LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    _table_name NAME;
    _schema_name NAME;
    _hypertable_row _timescaledb_catalog.hypertable;
    _orderby_names NAME[];
    _dimension_names NAME[];
    _first_index_attrs NAME[];
    _orderby_clauses text[];
BEGIN
    SELECT n.nspname, c.relname INTO STRICT _schema_name, _table_name
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.oid = c.relnamespace)
    WHERE c.oid = relation;

    SELECT * INTO STRICT _hypertable_row FROM _timescaledb_catalog.hypertable h WHERE h.table_name = _table_name AND h.schema_name = _schema_name;

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
    end if;

    --add dimension columns to the end. A dimension column like time should probably always be part of the order by.
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
      coalesce(array_agg(
      CASE WHEN d.column_name IS NULL THEN
        pg_catalog.format('%I', a.colname)
      ELSE
        pg_catalog.format('%I DESC', a.colname)
      END ORDER BY pos), array[]::text[]) INTO STRICT _orderby_clauses
    FROM unnest(_orderby_names) WITH ORDINALITY as a(colname, pos)
    LEFT JOIN _timescaledb_catalog.dimension d ON (d.column_name = a.colname AND d.hypertable_id = _hypertable_row.id);


    return _orderby_clauses;
END
$BODY$ SET search_path TO pg_catalog, pg_temp;
