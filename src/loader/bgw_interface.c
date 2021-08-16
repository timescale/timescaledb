/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

#include <miscadmin.h>
#include <fmgr.h>

#include "bgw_interface.h"
#include "../compat/compat.h"
#include "bgw_counter.h"
#include "bgw_message_queue.h"
#include "../extension_constants.h"

/* This is where versioned-extension facing functions live. They shouldn't live anywhere else. */

const int32 ts_bgw_loader_api_version = 3;

TS_FUNCTION_INFO_V1(ts_bgw_worker_reserve);
TS_FUNCTION_INFO_V1(ts_bgw_worker_release);
TS_FUNCTION_INFO_V1(ts_bgw_num_unreserved);
TS_FUNCTION_INFO_V1(ts_bgw_db_workers_start);

TS_FUNCTION_INFO_V1(ts_bgw_db_workers_stop);

TS_FUNCTION_INFO_V1(ts_bgw_db_workers_restart);

void
ts_bgw_interface_register_api_version()
{
	void **versionptr = find_rendezvous_variable(RENDEZVOUS_BGW_LOADER_API_VERSION);

	/* Cast away the const to store in the rendezvous variable */
	*versionptr = (void *) &ts_bgw_loader_api_version;
}

Datum
ts_bgw_worker_reserve(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(ts_bgw_total_workers_increment());
}

Datum
ts_bgw_worker_release(PG_FUNCTION_ARGS)
{
	ts_bgw_total_workers_decrement();
	PG_RETURN_VOID();
}

Datum
ts_bgw_num_unreserved(PG_FUNCTION_ARGS)
{
	int unreserved_workers;

	unreserved_workers = ts_guc_max_background_workers - ts_bgw_total_workers_get();
	PG_RETURN_INT32(unreserved_workers);
}

Datum
ts_bgw_db_workers_start(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to start background workers"))));

	PG_RETURN_BOOL(ts_bgw_message_send_and_wait(START, MyDatabaseId));
}

Datum
ts_bgw_db_workers_stop(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to stop background workers"))));

	PG_RETURN_BOOL(ts_bgw_message_send_and_wait(STOP, MyDatabaseId));
}

Datum
ts_bgw_db_workers_restart(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to restart background workers"))));

	PG_RETURN_BOOL(ts_bgw_message_send_and_wait(RESTART, MyDatabaseId));
}
