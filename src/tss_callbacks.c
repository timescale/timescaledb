/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * Optional callbacks let a provider explicitly record operations executed by
 * TimescaleDB, including hypertable COPY FROM and background job executions.
 * Utility hooks may also observe COPY FROM, depending on the loader version and
 * hook order; callback reporting is independent of that path.
 * Optional scope callbacks classify directly executed bodies as nested.
 *
 * Callback use is controlled by the enable_tss_callbacks GUC.
 */

#include <postgres.h>

#include <executor/instrument.h>
#include <fmgr.h>
#include <utils/elog.h>

#include "guc.h"
#include "tss_callbacks.h"

static TSSCallbacks *
ts_get_tss_callbacks(void)
{
	TSSCallbacks **ptr = (TSSCallbacks **) find_rendezvous_variable(TSS_CALLBACKS_VAR_NAME);

	return *ptr;
}

static TSSStatementScopeCallbacks *
ts_get_tss_statement_scope_callbacks(void)
{
	TSSStatementScopeCallbacks **ptr = (TSSStatementScopeCallbacks **) find_rendezvous_variable(
		TSS_STATEMENT_SCOPE_CALLBACKS_VAR_NAME);

	return *ptr;
}

void
ts_begin_tss_store_callback(volatile TsTssStatementScope *scope)
{
	TSSCallbacks *callbacks;

	Assert(scope->scope_token == TSS_STATEMENT_SCOPE_NOT_ENTERED);
	Assert(scope->exit == NULL);
	Assert(!scope->synthetic_enabled);

	if (!ts_guc_enable_tss_callbacks)
	{
		return;
	}

	callbacks = ts_get_tss_callbacks();
	if (callbacks == NULL)
	{
		return;
	}

	if (callbacks->version_num != TSS_CALLBACKS_VERSION)
	{
		ereport(WARNING,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("statement-tracking callback version mismatch"),
				 errdetail("Expected callback version " INT64_FORMAT
						   "; provider version " INT64_FORMAT ".",
						   (int64) TSS_CALLBACKS_VERSION,
						   (int64) callbacks->version_num)));
		return;
	}

	if (callbacks->tss_store_hook == NULL || callbacks->tss_enabled == NULL ||
		!callbacks->tss_enabled(0))
	{
		return;
	}

	scope->store = callbacks->tss_store_hook;
	scope->synthetic_enabled = true;
	scope->start_bufusage = pgBufferUsage;
	scope->start_walusage = pgWalUsage;
	INSTR_TIME_SET_CURRENT(scope->start_time);
}

bool
ts_tss_store_callback_started(const volatile TsTssStatementScope *scope)
{
	return scope->synthetic_enabled;
}

void
ts_tss_statement_scope_enter(volatile TsTssStatementScope *scope)
{
	TSSStatementScopeCallbacks *callbacks;
	TSSStatementScopeToken token;

	if (!scope->synthetic_enabled)
	{
		return;
	}

	Assert(scope->scope_token == TSS_STATEMENT_SCOPE_NOT_ENTERED);
	Assert(scope->exit == NULL);

	callbacks = ts_get_tss_statement_scope_callbacks();
	if (!tss_statement_scope_callbacks_usable(callbacks))
	{
		return;
	}

	token = callbacks->enter();
	if (token < 0)
	{
		return;
	}

	scope->exit = callbacks->exit;
	scope->scope_token = token;
}

void
ts_tss_statement_scope_exit(volatile TsTssStatementScope *scope)
{
	tss_statement_scope_exit_hook_type exit = scope->exit;
	TSSStatementScopeToken token = scope->scope_token;

	scope->exit = NULL;
	scope->scope_token = TSS_STATEMENT_SCOPE_NOT_ENTERED;

	if (exit != NULL && token >= 0)
	{
		exit(token);
	}
}

void
ts_end_tss_store_callback(volatile TsTssStatementScope *scope, const char *query,
						  int query_location, int query_len, uint64 query_id, uint64 rows)
{
	instr_time duration;
	instr_time start_time;
	BufferUsage bufusage;
	WalUsage walusage;
	BufferUsage start_bufusage;
	WalUsage start_walusage;
	tss_store_hook_type store;

	ts_tss_statement_scope_exit(scope);

	if (!scope->synthetic_enabled)
	{
		return;
	}

	store = scope->store;
	scope->store = NULL;
	scope->synthetic_enabled = false;
	if (store == NULL)
	{
		return;
	}

	start_time = scope->start_time;
	start_bufusage = scope->start_bufusage;
	start_walusage = scope->start_walusage;
	INSTR_TIME_SET_CURRENT(duration);
	INSTR_TIME_SUBTRACT(duration, start_time);

	/* calc differences of buffer counters. */
	memset(&bufusage, 0, sizeof(BufferUsage));
	BufferUsageAccumDiff(&bufusage, &pgBufferUsage, &start_bufusage);

	/* calc differences of WAL counters. */
	memset(&walusage, 0, sizeof(WalUsage));
	WalUsageAccumDiff(&walusage, &pgWalUsage, &start_walusage);

	store(query,
		  query_location,
		  query_len,
		  query_id,
		  INSTR_TIME_GET_MICROSEC(duration),
		  rows,
		  &bufusage,
		  &walusage);
}
