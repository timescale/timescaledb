#include <postgres.h>
#include <fmgr.h>
#include <utils/memutils.h>
#include <utils/builtins.h>
#include <access/xact.h>

#include "deps/dblink.h"

void		_xact_init(void);
void		_xact_fini(void);

static List *callbackConnections = NIL;

Datum		register_dblink_precommit_connection(PG_FUNCTION_ARGS);

/* Function to register dblink connections for remote commit/abort on local pre-commit/abort. */
PG_FUNCTION_INFO_V1(register_dblink_precommit_connection);
Datum
register_dblink_precommit_connection(PG_FUNCTION_ARGS)
{
	/*
	 * allocate this stuff in top-level transaction context, so that it
	 * survives till commit
	 */
	MemoryContext old = MemoryContextSwitchTo(TopTransactionContext);

	char	   *connectionName = text_to_cstring(PG_GETARG_TEXT_PP(0));

	callbackConnections = lappend(callbackConnections, connectionName);

	MemoryContextSwitchTo(old);
	PG_RETURN_VOID();
}

/*
 * Commits dblink connections registered with register_dblink_precommit_connection.
 * Look at meta_commands.sql for example usage. Remote commits happen in pre-commit.
 * Remote aborts happen on abort.
 * */
static void
io_xact_callback(XactEvent event, void *arg)
{
	ListCell   *cell;

	if (list_length(callbackConnections) == 0)
		return;

	switch (event)
	{
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_COMMIT:
			foreach(cell, callbackConnections)
			{
				char	   *connection = (char *) lfirst(cell);

				DirectFunctionCall3(dblink_exec,
								PointerGetDatum(cstring_to_text(connection)),
								  PointerGetDatum(cstring_to_text("COMMIT")),
									BoolGetDatum(true));		/* throw error */
				DirectFunctionCall1(dblink_disconnect, PointerGetDatum(cstring_to_text(connection)));
			}
			break;
		case XACT_EVENT_PARALLEL_ABORT:
		case XACT_EVENT_ABORT:

			/*
			 * Be quite careful here. Cannot throw any errors (or infinite
			 * loop) and cannot use PG_TRY either. Make sure to test with
			 * c-asserts on.
			 */
			foreach(cell, callbackConnections)
			{
				char	   *connection = (char *) lfirst(cell);

				DirectFunctionCall3(dblink_exec,
								PointerGetDatum(cstring_to_text(connection)),
									PointerGetDatum(cstring_to_text("ABORT")),
									BoolGetDatum(false));
				DirectFunctionCall1(dblink_disconnect, PointerGetDatum(cstring_to_text(connection)));
			}
			break;
		case XACT_EVENT_PRE_PREPARE:
		case XACT_EVENT_PREPARE:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot prepare a transaction that has open dblink constraint_exclusion_options")));
			break;
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_COMMIT:
			break;
		default:
			elog(ERROR, "unkown xact event: %d", event);
	}
	callbackConnections = NIL;
}

void
_xact_init(void)
{
	RegisterXactCallback(io_xact_callback, NULL);
}

void
_xact_fini(void)
{
	UnregisterXactCallback(io_xact_callback, NULL);
}
