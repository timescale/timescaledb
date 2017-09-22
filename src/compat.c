#include <postgres.h>
#include <funcapi.h>

/* Old functions that are no longer used but are needed for compatibiliy when
 * updating the extension. */
PGDLLEXPORT Datum insert_main_table_trigger(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(insert_main_table_trigger);

Datum
insert_main_table_trigger(PG_FUNCTION_ARGS)
{
	elog(ERROR, "Deprecated trigger function should not be invoked");
	PG_RETURN_NULL();
}

PGDLLEXPORT Datum insert_main_table_trigger_after(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(insert_main_table_trigger_after);

Datum
insert_main_table_trigger_after(PG_FUNCTION_ARGS)
{
	elog(ERROR, "Deprecated trigger function should not be invoked");
	PG_RETURN_NULL();
}

PGDLLEXPORT Datum ddl_is_change_owner(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(ddl_is_change_owner);

Datum
ddl_is_change_owner(PG_FUNCTION_ARGS)
{
	elog(ERROR, "Deprecated function should not be invoked");
	PG_RETURN_NULL();
}

PGDLLEXPORT Datum ddl_change_owner_to(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(ddl_change_owner_to);

Datum
ddl_change_owner_to(PG_FUNCTION_ARGS)
{
	elog(ERROR, "Deprecated function should not be invoked");
	PG_RETURN_NULL();
}
