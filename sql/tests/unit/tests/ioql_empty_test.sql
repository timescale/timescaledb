CREATE OR REPLACE FUNCTION unit_tests.test_ioql_queries_empty()
RETURNS test_result
AS
$$
DECLARE
message test_result;
success boolean;
BEGIN
    
	PERFORM test_utils.test_query(new_ioql_query(namespace_name => 'emptyNs',
	select_items => ARRAY[new_select_item('nUm_1'::text,
	NULL::aggregate_function_type)]), 'test_outputs', 'empty_result');
	
	PERFORM test_utils.test_query(new_ioql_query(namespace_name => 'emptyNs',
	select_items => ARRAY[new_select_item('nUm_1'::text, 'SUM'::aggregate_function_type)],
	aggregate => new_aggregate(2592000000000000, NULL)), 'test_outputs', 'empty_result');

	PERFORM test_utils.test_query(new_ioql_query(namespace_name => 'emptyNs',
	select_items => ARRAY[new_select_item('nUm_1'::text, 'AVG'::aggregate_function_type)],
	aggregate => new_aggregate(2592000000000000, NULL)), 'test_outputs', 'empty_result');

	PERFORM test_utils.test_query(new_ioql_query(namespace_name => 'emptyNs',
	select_items => ARRAY[new_select_item('nUm_1'::text, 'MAX'::aggregate_function_type)],
	aggregate => new_aggregate(2592000000000000, NULL)), 'test_outputs', 'empty_result');

	PERFORM test_utils.test_query(new_ioql_query(namespace_name => 'emptyNs',
	select_items => ARRAY[new_select_item('nUm_1'::text, 'MIN'::aggregate_function_type)],
	aggregate => new_aggregate(2592000000000000, NULL)), 'test_outputs', 'empty_result');
	
	PERFORM test_utils.test_query(new_ioql_query(namespace_name => 'emptyNs',
	select_items => ARRAY[new_select_item('nUm_1'::text, 'COUNT'::aggregate_function_type)],
	aggregate => new_aggregate(2592000000000000, NULL)), 'test_outputs', 'empty_result');
	
	PERFORM test_utils.test_query(new_ioql_query(namespace_name => 'emptyNs',
	select_items => ARRAY[new_select_item('nUm_1'::text, 'SUM'::aggregate_function_type)],
	aggregate => new_aggregate(2592000000000000, 'device_id')), 'test_outputs', 'empty_result');	

	SELECT assert.ok('End of test.') INTO message;
	RETURN message;
END
$$
LANGUAGE plpgsql;
