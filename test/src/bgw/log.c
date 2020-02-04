/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/xact.h>
#include <catalog/namespace.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <postmaster/bgworker.h>
#include <storage/proc.h>

#include "log.h"
#include "scanner.h"
#include "params.h"
#include "catalog.h"

#include "compat.h"

static char *bgw_application_name = "unset";

void
ts_bgw_log_set_application_name(char *name)
{
	bgw_application_name = name;
}

static bool
bgw_log_insert_relation(Relation rel, char *msg)
{
	TupleDesc desc = RelationGetDescr(rel);
	static int32 msg_no = 0;
	Datum values[4];
	bool nulls[4] = { false, false, false };

	values[0] = Int32GetDatum(msg_no++);
	values[1] = Int64GetDatum((int64) ts_params_get()->current_time);
	values[2] = CStringGetTextDatum(bgw_application_name);
	values[3] = CStringGetTextDatum(msg);

	ts_catalog_insert_values(rel, desc, values, nulls);

	return true;
}

/* Insert a new entry into public.bgw_log
 * This table is used for testing as a way for mock background jobs
 * to insert messges into a log that could then be output into the golden file
 */
static void
bgw_log_insert(char *msg)
{
	Relation rel;
	Oid log_oid = get_relname_relid("bgw_log", get_namespace_oid("public", false));

	rel = table_open(log_oid, RowExclusiveLock);
	bgw_log_insert_relation(rel, msg);
	table_close(rel, RowExclusiveLock);
}

static emit_log_hook_type prev_emit_log_hook = NULL;

static void
emit_log_hook_callback(ErrorData *edata)
{
	bool started_txn = false;

	/*
	 * once proc_exit has started we may no longer be able to start transactions
	 */
	if (MyProc == NULL)
		return;

	/*
	 * Block signals so we don't lose messages generated during signal
	 * processing if they occur while we are saving this log message (since
	 * emit_log_hook is modified and restored below)
	 */
	BackgroundWorkerBlockSignals();
	PG_TRY();
	{
		/*
		 * If we do encounter some error writing to our log hook, remove the
		 * hook to prevent potentially infinite recursion where this callback
		 * keeps encountering an error, and it is its own logging callback. We
		 * reinstall the hook when we're successfully done with this function.
		 */
		emit_log_hook = NULL;

		if (!IsTransactionState())
		{
			StartTransactionCommand();
			started_txn = true;
		}

		bgw_log_insert(edata->message);

		if (started_txn)
			CommitTransactionCommand();

		if (prev_emit_log_hook != NULL)
			prev_emit_log_hook(edata);

		/* Reinstall the hook if log was successful. */
		emit_log_hook = emit_log_hook_callback;
	}
	PG_CATCH();
	{
		/*
		 * Reinstall the hook because we are out of the main body of the
		 * function.
		 */
		emit_log_hook = emit_log_hook_callback;
	}
	PG_END_TRY();
	BackgroundWorkerUnblockSignals();
}

void
ts_register_emit_log_hook()
{
	prev_emit_log_hook = emit_log_hook;
	emit_log_hook = emit_log_hook_callback;
}
