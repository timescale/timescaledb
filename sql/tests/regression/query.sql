\set ON_ERROR_STOP 1

\ir insert.sql

\c Test1

SELECT *
FROM ioql_exec_query(new_ioql_query(namespace_name => 'testNs'));


SELECT *
FROM ioql_exec_query(new_ioql_query(namespace_name => 'testNs',
                                    select_items => ARRAY [new_select_item('series_0', 'SUM')],
                                    aggregate => new_aggregate((100 * 60 * 60 * 1e9) :: BIGINT)
                     ));

SELECT *
FROM ioql_exec_query(new_ioql_query(namespace_name => 'testNs',
                                    select_items => ARRAY [new_select_item('series_0', 'SUM')],
                                    aggregate => new_aggregate((100 * 60 * 60 * 1e9) :: BIGINT),
                                    limit_rows => 1
                     ));

SELECT *
FROM ioql_exec_query(new_ioql_query(namespace_name => 'testNs',
                                    select_items => ARRAY [new_select_item('series_0', 'SUM')],
                                    aggregate => new_aggregate((100 * 60 * 60 * 1e9) :: BIGINT, 'series_1')
                     ));

SELECT *
FROM ioql_exec_query(new_ioql_query(namespace_name => 'testNs',
                                    select_items => ARRAY [new_select_item('series_0', 'SUM')],
                                    aggregate => new_aggregate((100 * 60 * 60 * 1e9) :: BIGINT, 'series_1'),
                                    limit_rows => 1
                     ));

SELECT *
FROM ioql_exec_query(new_ioql_query(namespace_name => 'testNs',
                                    select_items => ARRAY [new_select_item('series_0')],
                                    limit_by_field => new_limit_by_field('device_id', 1)
                     ));

SELECT *
FROM ioql_exec_query(new_ioql_query(namespace_name => 'testNs',
                                    select_items => ARRAY [new_select_item('series_0')],
                                    limit_rows => 1
                     ));