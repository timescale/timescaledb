#include <postgres.h>
#include <funcapi.h>

#include "compat.h"
#include "extension.h"

/* Old functions that are no longer used but are needed for compatibility when
 * updating the extension. */
TS_FUNCTION_INFO_V1(insert_main_table_trigger);

Datum
insert_main_table_trigger(PG_FUNCTION_ARGS)
{
	elog(ERROR, "Deprecated trigger function should not be invoked");
	PG_RETURN_NULL();
}

TS_FUNCTION_INFO_V1(insert_main_table_trigger_after);

Datum
insert_main_table_trigger_after(PG_FUNCTION_ARGS)
{
	elog(ERROR, "Deprecated trigger function should not be invoked");
	PG_RETURN_NULL();
}

TS_FUNCTION_INFO_V1(ddl_is_change_owner);

Datum
ddl_is_change_owner(PG_FUNCTION_ARGS)
{
	elog(ERROR, "Deprecated function should not be invoked");
	PG_RETURN_NULL();
}

TS_FUNCTION_INFO_V1(ddl_change_owner_to);

Datum
ddl_change_owner_to(PG_FUNCTION_ARGS)
{
	elog(ERROR, "Deprecated function should not be invoked");
	PG_RETURN_NULL();
}

TS_FUNCTION_INFO_V1(indexing_verify_hypertable_indexes);

Datum
indexing_verify_hypertable_indexes(PG_FUNCTION_ARGS)
{
	elog(ERROR, "Deprecated function should not be invoked");
	PG_RETURN_NULL();
}

TS_FUNCTION_INFO_V1(hypertable_validate_triggers);

Datum
hypertable_validate_triggers(PG_FUNCTION_ARGS)
{
	elog(ERROR, "Deprecated function should not be invoked");
	PG_RETURN_NULL();
}

TS_FUNCTION_INFO_V1(timescaledb_ddl_command_end);

Datum
timescaledb_ddl_command_end(PG_FUNCTION_ARGS)
{
	if (!extension_is_loaded())
		PG_RETURN_NULL();

	elog(ERROR, "Deprecated function should not be invoked");
	PG_RETURN_NULL();
}
