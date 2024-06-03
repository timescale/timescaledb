/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * Currently we finish the execution of some process utility statements
 * and don't execute other hooks in the chain.
 *
 * Because that reason neither ts_stat_statements and pg_stat_statements
 * are able to track some utility statements, for example COPY ... FROM.
 *
 * To be able to track it on ts_stat_statements here we introduce some
 * callbacks in order to hook pgss_store from TimescaleDB and store
 * information about the execution of those statements.
 *
 * Hooking ts_stat_statements from TimescaleDB is controlled by a new GUC
 * named `enable_tss_callbacks`.
 */

#include <postgres.h>

#include <executor/instrument.h>
#include <fmgr.h>
#include <utils/elog.h>

#include "guc.h"
#include "tss_callbacks.h"

static instr_time tss_callback_start_time;
static BufferUsage tss_callback_start_bufusage;
static WalUsage tss_callback_start_walusage;

static TSSCallbacks *
ts_get_tss_callbacks(void)
{
	TSSCallbacks **ptr = (TSSCallbacks **) find_rendezvous_variable(TSS_CALLBACKS_VAR_NAME);

	return *ptr;
}

static tss_store_hook_type
ts_get_tss_store_hook(void)
{
	TSSCallbacks *ptr = ts_get_tss_callbacks();

	if (ptr && ptr->version_num == TSS_CALLBACKS_VERSION)
		return ptr->tss_store_hook;

	return NULL;
}

static bool
is_tss_enabled(void)
{
	if (ts_guc_enable_tss_callbacks)
	{
		TSSCallbacks *ptr = ts_get_tss_callbacks();

		if (ptr)
		{
			if (ptr->version_num != TSS_CALLBACKS_VERSION)
			{
				ereport(WARNING,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("version mismatch between timescaledb and ts_stat_statements "
								"callbacks"),
						 errdetail("Callbacks versions: TimescaleDB (%d) and ts_stat_statements "
								   "(%d)",
								   TSS_CALLBACKS_VERSION,
								   ptr->version_num)));
				return false;
			}

			return ptr->tss_enabled_hook_type(0); /* consider top level statement */
		}
	}

	return false;
}

void
ts_begin_tss_store_callback(void)
{
	if (!is_tss_enabled())
		return;

	tss_callback_start_bufusage = pgBufferUsage;
	tss_callback_start_walusage = pgWalUsage;
	INSTR_TIME_SET_CURRENT(tss_callback_start_time);
}

void
ts_end_tss_store_callback(const char *query, int query_location, int query_len, uint64 query_id,
						  uint64 rows)
{
	instr_time duration;
	BufferUsage bufusage;
	WalUsage walusage;
	tss_store_hook_type hook;

	if (!is_tss_enabled())
		return;

	hook = ts_get_tss_store_hook();

	if (!hook)
		return;

	INSTR_TIME_SET_CURRENT(duration);
	INSTR_TIME_SUBTRACT(duration, tss_callback_start_time);

	/* calc differences of buffer counters. */
	memset(&bufusage, 0, sizeof(BufferUsage));
	BufferUsageAccumDiff(&bufusage, &pgBufferUsage, &tss_callback_start_bufusage);

	/* calc differences of WAL counters. */
	memset(&walusage, 0, sizeof(WalUsage));
	WalUsageAccumDiff(&walusage, &pgWalUsage, &tss_callback_start_walusage);

	hook(query,
		 query_location,
		 query_len,
		 query_id,
		 INSTR_TIME_GET_MICROSEC(duration),
		 rows,
		 &bufusage,
		 &walusage);
}
