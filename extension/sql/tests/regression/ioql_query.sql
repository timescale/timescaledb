\set ON_ERROR_STOP 1

\o /dev/null
\ir include/insert.sql

\o
\set ECHO ALL
\c Test1

SELECT *
FROM ioql_exec_query(new_ioql_query(hypertable_name => 'testNs'));

\set ON_ERROR_STOP 0
SELECT *
FROM ioql_exec_query(new_ioql_query(hypertable_name => 'DoesNotExist'));
\set ON_ERROR_STOP 1

SELECT *
FROM ioql_exec_query(new_ioql_query(hypertable_name => 'testNs',
                                    select_items => ARRAY [new_select_item('series_0', 'SUM')],
                                    aggregate => new_aggregate((100 * 60 * 60 * 1e9) :: BIGINT)
                     ));

SELECT *
FROM ioql_exec_query(new_ioql_query(hypertable_name => 'testNs',
                                    select_items => ARRAY [new_select_item('series_0', 'SUM')],
                                    aggregate => new_aggregate((100 * 60 * 60 * 1e9) :: BIGINT),
                                    limit_rows => 1
                     ));

SELECT *
FROM ioql_exec_query(new_ioql_query(hypertable_name => 'testNs',
                                    select_items => ARRAY [new_select_item('series_0', 'SUM')],
                                    aggregate => new_aggregate((100 * 60 * 60 * 1e9) :: BIGINT, 'series_1')
                     ));

SELECT *
FROM ioql_exec_query(new_ioql_query(hypertable_name => 'testNs',
                                    select_items => ARRAY [new_select_item('series_0', 'SUM')],
                                    aggregate => new_aggregate((100 * 60 * 60 * 1e9) :: BIGINT, 'series_1'),
                                    limit_rows => 1
                     ));

SELECT *
FROM ioql_exec_query(new_ioql_query(hypertable_name => 'testNs',
                                    select_items => ARRAY [new_select_item('series_0', 'SUM')],
                                    aggregate => new_aggregate((100 * 60 * 60 * 1e9) :: BIGINT, 'series_1'),
                                    limit_time_periods => 1
                     ));

SELECT *
FROM ioql_exec_query(new_ioql_query(hypertable_name => 'testNs',
                                    select_items => ARRAY [new_select_item('series_0')],
                                    limit_by_column => new_limit_by_column('device_id', 1)
                     ));

SELECT *
FROM ioql_exec_query(new_ioql_query(hypertable_name => 'testNs',
                                    select_items => ARRAY [new_select_item('series_0')],
                                    limit_rows => 1
                     ));
