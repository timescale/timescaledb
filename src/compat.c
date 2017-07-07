#include <postgres.h>
#include <funcapi.h>

/* Old functions that are no longer used but are needed for compatibiliy when
 * updating the extension. */
PG_FUNCTION_INFO_V1(insert_main_table_trigger);

Datum
insert_main_table_trigger(PG_FUNCTION_ARGS)
{
	elog(ERROR, "Deprecated trigger function should not be invoked");
	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(insert_main_table_trigger_after);

Datum
insert_main_table_trigger_after(PG_FUNCTION_ARGS)
{
	elog(ERROR, "Deprecated trigger function should not be invoked");
	PG_RETURN_NULL();
}
