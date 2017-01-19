CREATE OR REPLACE FUNCTION unit_tests.test_ioql_queries()
RETURNS test_result
AS
$$
DECLARE
message test_result;
success boolean;
BEGIN
    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs'), 'test_outputs', 'output_0');
    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs'), 'test_outputs', 'output_0');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,NULL::aggregate_function_type)]), 'test_outputs', 'output_1');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('bool_1'::text,NULL::aggregate_function_type)]), 'test_outputs', 'output_2');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('string_1'::text,NULL::aggregate_function_type)]), 'test_outputs', 'output_3');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('field_only_dev2'::text,NULL::aggregate_function_type)]), 'test_outputs', 'output_4');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('field_only_ref2'::text,NULL::aggregate_function_type)]), 'test_outputs', 'output_5');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',limit_rows=>2), 'test_outputs', 'output_7');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    limit_by_column=>new_limit_by_column('device_id',1)), 'test_outputs', 'output_8');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    limit_by_column=>new_limit_by_column('string_1',2)), 'test_outputs', 'output_9');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    limit_by_column=>new_limit_by_column('string_2',1)), 'test_outputs', 'output_10');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,NULL::aggregate_function_type)],
    limit_by_column=>new_limit_by_column('string_2',1)), 'test_outputs', 'output_11');

    /*
        Should throw error as field_only_dev2 is not distinct, bug filed

        PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
        limit_by_column=>new_limit_by_column('field_only_dev2',1)), 'test_outputs', 'output_12');
    */
    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    time_condition=>new_time_condition(NULL,910738800000000000),
    limit_by_column=>new_limit_by_column('device_id',1)), 'test_outputs', 'output_13');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,NULL::aggregate_function_type)],
    time_condition=>new_time_condition(NULL,910738800000000000),
    limit_by_column=>new_limit_by_column('device_id',1)), 'test_outputs', 'output_14');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    time_condition=>new_time_condition(NULL,1257987600000000000)), 'test_outputs', 'output_15');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    time_condition=>new_time_condition(NULL,1257987601000000000)), 'test_outputs', 'output_16');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    time_condition=>new_time_condition(1257894000000000000,NULL)), 'test_outputs', 'output_17');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    time_condition=>new_time_condition(1257987600000000000,NULL)), 'test_outputs', 'output_18');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    time_condition=>new_time_condition(1257894000000000000,1257987600000000000)),
    'test_outputs', 'output_19');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    time_condition=>new_time_condition(1257894000000000000,1257987601000000000)),
    'test_outputs', 'output_20');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    column_condition=>new_column_condition('AND',ARRAY[new_column_predicate('nUm_1','=',1.5::text)])),
    'test_outputs', 'output_21');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    column_condition=>new_column_condition('AND',ARRAY[new_column_predicate('nUm_1','=',1.6::text)])),
    'test_outputs', 'output_22');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    column_condition=>new_column_condition('AND',ARRAY[new_column_predicate('nUm_1','!=',1.6::text)])),
    'test_outputs', 'output_23');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    column_condition=>new_column_condition('AND',ARRAY[new_column_predicate('nUm_1','!=',1.5::text)])),
    'test_outputs', 'output_24');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    column_condition=>new_column_condition('AND',ARRAY[new_column_predicate('nUm_1','>',1.4::text)])),
    'test_outputs', 'output_25');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    column_condition=>new_column_condition('AND',ARRAY[new_column_predicate('nUm_1','>',1.5::text)])),
    'test_outputs', 'output_26');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    column_condition=>new_column_condition('AND',ARRAY[new_column_predicate('nUm_1','>=',1.5::text)])),
    'test_outputs', 'output_27');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    column_condition=>new_column_condition('AND',ARRAY[new_column_predicate('nUm_1','>=',1.51::text)])),
    'test_outputs', 'output_28');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    column_condition=>new_column_condition('AND',ARRAY[new_column_predicate('nUm_1','<',1.6::text)])),
    'test_outputs', 'output_29');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    column_condition=>new_column_condition('AND',ARRAY[new_column_predicate('nUm_1','<',1.5::text)])),
    'test_outputs', 'output_30');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    column_condition=>new_column_condition('AND',ARRAY[new_column_predicate('nUm_1','<=',1.5::text)])),
    'test_outputs', 'output_31');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    column_condition=>new_column_condition('AND',ARRAY[new_column_predicate('nUm_1','<=',1.49::text)])),
    'test_outputs', 'output_32');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    column_condition=>new_column_condition('AND',ARRAY[new_column_predicate('device_id','=','dev1'::text)])),
    'test_outputs', 'output_33');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    column_condition=>new_column_condition('AND',ARRAY[new_column_predicate('device_id','=','dev2'::text)])),
    'test_outputs', 'output_34');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    column_condition=>new_column_condition('AND',ARRAY[new_column_predicate('device_id','=','dev3'::text)])),
    'test_outputs', 'output_35');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    column_condition=>new_column_condition('AND',ARRAY[new_column_predicate('device_id','!=','dev2'::text)])),
    'test_outputs', 'output_36');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    column_condition=>new_column_condition('AND',ARRAY[new_column_predicate('nUm_1','=',1.5::text),
    new_column_predicate('num_2','=',0.0::text)])), 'test_outputs', 'output_37');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    column_condition=>new_column_condition('AND',ARRAY[new_column_predicate('nUm_1','=',1.5::text),
    new_column_predicate('num_2','=',2.0::text)])), 'test_outputs', 'output_38');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    column_condition=>new_column_condition('AND',ARRAY[new_column_predicate('num_2','=',1.0::text),
    new_column_predicate('num_2','=',2.0::text)])), 'test_outputs', 'output_39');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    column_condition=>new_column_condition('OR',ARRAY[new_column_predicate('nUm_1','=',1.5::text),
    new_column_predicate('num_2','=',0.0::text)])), 'test_outputs', 'output_40');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    column_condition=>new_column_condition('OR',ARRAY[new_column_predicate('nUm_1','=',1.5::text),
    new_column_predicate('num_2','=',2.0::text)])), 'test_outputs', 'output_41');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    column_condition=>new_column_condition('OR',ARRAY[new_column_predicate('num_2','=',1.0::text),
    new_column_predicate('num_2','=',2.0::text)])), 'test_outputs', 'output_42');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'SUM'::aggregate_function_type)],
    aggregate=>new_aggregate(2592000000000000,NULL)), 'test_outputs', 'output_43');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'SUM'::aggregate_function_type)],
    aggregate=>new_aggregate(3600000000000,NULL)), 'test_outputs', 'output_44');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'MAX'::aggregate_function_type)],
    aggregate=>new_aggregate(2592000000000000,NULL)), 'test_outputs', 'output_45');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'MAX'::aggregate_function_type)],
    aggregate=>new_aggregate(3600000000000,NULL)), 'test_outputs', 'output_46');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'MIN'::aggregate_function_type)],
    aggregate=>new_aggregate(2592000000000000,NULL)), 'test_outputs', 'output_47');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'MIN'::aggregate_function_type)],
    aggregate=>new_aggregate(3600000000000,NULL)), 'test_outputs', 'output_48');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'AVG'::aggregate_function_type)],
    aggregate=>new_aggregate(2592000000000000,NULL)), 'test_outputs', 'output_49');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'AVG'::aggregate_function_type)],
    aggregate=>new_aggregate(3600000000000,NULL)), 'test_outputs', 'output_50');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'COUNT'::aggregate_function_type)],
    aggregate=>new_aggregate(2592000000000000,NULL)), 'test_outputs', 'output_51');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'COUNT'::aggregate_function_type)],
    aggregate=>new_aggregate(3600000000000,NULL)), 'test_outputs', 'output_52');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'COUNT'::aggregate_function_type)],
    aggregate=>new_aggregate(2592000000000000,NULL),
    column_condition=>new_column_condition('AND',ARRAY[new_column_predicate('device_id','=','dev1'::text)])),
    'test_outputs', 'output_53');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'COUNT'::aggregate_function_type)],
    aggregate=>new_aggregate(2592000000000000,NULL),
    column_condition=>new_column_condition('AND',ARRAY[new_column_predicate('device_id','=','dev2'::text)])),
    'test_outputs', 'output_54');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'COUNT'::aggregate_function_type)],
    aggregate=>new_aggregate(2592000000000000,NULL),
    column_condition=>new_column_condition('AND',ARRAY[new_column_predicate('device_id','=','dev3'::text)])),
    'test_outputs', 'output_55');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'COUNT'::aggregate_function_type)],
    aggregate=>new_aggregate(2592000000000000,NULL),
    column_condition=>new_column_condition('AND',ARRAY[new_column_predicate('device_id','!=','dev2'::text)])),
    'test_outputs', 'output_56');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'MAX'::aggregate_function_type)],
    aggregate=>new_aggregate(2592000000000000,NULL),
    time_condition=>new_time_condition(1257894000000000000,1257987600000000000),
    column_condition=>new_column_condition('AND',ARRAY[new_column_predicate('device_id','=','dev1'::text)]),
    limit_rows=>100), 'test_outputs', 'output_57');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'SUM'::aggregate_function_type)],
    aggregate=>new_aggregate(3600000000000,NULL),limit_time_periods=>2), 'test_outputs', 'output_58');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'SUM'::aggregate_function_type)],
    aggregate=>new_aggregate(3600000000000,NULL),limit_time_periods=>200), 'test_outputs', 'output_59');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'SUM'::aggregate_function_type)],
    aggregate=>new_aggregate(3600000000000,NULL),limit_rows=>1), 'test_outputs', 'output_60');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'SUM'::aggregate_function_type)],
    aggregate=>new_aggregate(3600000000000,NULL),limit_rows=>200), 'test_outputs', 'output_61');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'SUM'::aggregate_function_type)],
    aggregate=>new_aggregate(3600000000000,NULL),limit_rows=>200,limit_time_periods=>2),
    'test_outputs', 'output_62');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'SUM'::aggregate_function_type)],
    aggregate=>new_aggregate(3600000000000,NULL),limit_rows=>200,limit_time_periods=>200),
    'test_outputs', 'output_63');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('field_only_dev2'::text,'SUM'::aggregate_function_type)],
    aggregate=>new_aggregate(3600000000000,NULL),limit_time_periods=>1), 'test_outputs', 'output_64');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'COUNT'::aggregate_function_type)],
    aggregate=>new_aggregate(3600000000000,'num_2')), 'test_outputs', 'output_65');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'COUNT'::aggregate_function_type)],
    aggregate=>new_aggregate(3600000000000,'string_2')), 'test_outputs', 'output_66');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'COUNT'::aggregate_function_type)],
    aggregate=>new_aggregate(3600000000000,'num_2'),limit_time_periods=>1), 'test_outputs', 'output_67');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'COUNT'::aggregate_function_type)],
    aggregate=>new_aggregate(3600000000000,'string_2'),limit_time_periods=>1), 'test_outputs', 'output_68');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'COUNT'::aggregate_function_type)],
    aggregate=>new_aggregate(2592000000000000,'num_2')), 'test_outputs', 'output_69');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'COUNT'::aggregate_function_type)],
    aggregate=>new_aggregate(2592000000000000,'string_2')), 'test_outputs', 'output_70');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'COUNT'::aggregate_function_type)],
    aggregate=>new_aggregate(2592000000000000,'field_only_dev2')), 'test_outputs', 'output_71');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('nUm_1'::text,'COUNT'::aggregate_function_type)],
    aggregate=>new_aggregate(3600000000000,'field_only_dev2'),limit_time_periods=>1),
    'test_outputs', 'output_72');

    PERFORM test_utils.test_query(new_ioql_query(hypertable_name=>'33_testNs',
    select_items=>ARRAY[new_select_item('device_id'::text,'COUNT'::aggregate_function_type)],
    aggregate=>new_aggregate(3600000000000,'device_id'),limit_rows=>1), 'test_outputs', 'output_73');

    SELECT assert.ok('End of test.') INTO message;
    RETURN message;
END
$$
LANGUAGE plpgsql;
