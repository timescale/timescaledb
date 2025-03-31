/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <fmgr.h>

#include <catalog/namespace.h>
#include <executor/spi.h>
#include <utils/builtins.h>
#include <utils/guc.h>

#include "timescaledb.h"

static TimescaleDBCallbacks **plugin_ptr = NULL;

/* Using PGDLLEXPORT to avoid unecessary dependencies */
extern void PGDLLEXPORT _PG_init(void);

PG_MODULE_MAGIC;

static void
log_job_init(int job_id, Oid user_oid)
{
	elog(LOG, "%s: Job with id %d initializing", __func__, job_id);
}

static void
log_job_start(int job_id, Oid owner, const char *application_name, List *proc)
{
	elog(LOG,
		 "%s: Job with id %d started with function \"%s\"",
		 __func__,
		 job_id,
		 NameListToString(proc));
}

static void
log_job_exit(int job_id, int error_code)
{
	elog(LOG, "%s: Job with id %d exiting with error code %d", __func__, job_id, error_code);
}

/*
 * This is the plugin's idea about how big the structure is. The server will
 * honor that and just use callbacks that the plugin knows about.
 */
static TimescaleDBCallbacks plugin_callbacks = {
	.size = sizeof(TimescaleDBCallbacks),
	.bgw_job_init = log_job_init,
	.bgw_job_start = log_job_start,
	.bgw_job_exit = log_job_exit,
};

void
_PG_init(void)
{
	plugin_ptr = (TimescaleDBCallbacks **) find_rendezvous_variable(TIMESCALEDB_PLUGIN_NAME);
	*plugin_ptr = &plugin_callbacks;
}
