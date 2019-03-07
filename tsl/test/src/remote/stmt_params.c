/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_type.h>

#include "export.h"
#include "remote/stmt_params.h"

TS_FUNCTION_INFO_V1(tsl_test_stmt_params_format);

Datum
tsl_test_stmt_params_format(PG_FUNCTION_ARGS)
{
	StmtParams *params;
	const int *formats;
	int i;
	TupleDesc tuple_desc;
	List *target_attr_nums;
	bool binary = PG_GETARG_BOOL(0);
	Form_pg_attribute *attrs = palloc(sizeof(Form_pg_attribute) * 2);
	attrs[0] = &(FormData_pg_attribute){
		.atttypid = INT4OID,
	};
	attrs[1] = &(FormData_pg_attribute){
		.atttypid = BOOLOID,
	};
	tuple_desc = CreateTupleDesc(2, false, attrs);
	target_attr_nums = list_make2_int(1, 2);

	params = stmt_params_create(target_attr_nums, false, tuple_desc, 2);
	formats = stmt_params_formats(params);
	Assert(stmt_params_num_params(params) == 2);
	if (binary)
		for (i = 0; i < 4; i++)
			Assert(formats[i] == FORMAT_BINARY);
	else
		for (i = 0; i < 4; i++)
			Assert(formats[i] == FORMAT_TEXT);

	PG_RETURN_VOID();
}
