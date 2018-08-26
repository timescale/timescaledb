#include <postgres.h>

#include <miscadmin.h>
#include <fmgr.h>
#include "../compat.h"
#include "bgw_counter.h"
#include "bgw_message_queue.h"
#include "bgw_interface.h"


/* This is where versioned-extension facing functions live. They shouldn't live anywhere else. */


TS_FUNCTION_INFO_V1(ts_bgw_db_workers_start);

TS_FUNCTION_INFO_V1(ts_bgw_db_workers_stop);

TS_FUNCTION_INFO_V1(ts_bgw_db_workers_restart);

Datum
ts_bgw_worker_reserve(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(bgw_total_workers_increment());
}

Datum
ts_bgw_worker_release(PG_FUNCTION_ARGS)
{
	bgw_total_workers_decrement();
	PG_RETURN_VOID();
}

Datum
ts_bgw_num_unreserved(PG_FUNCTION_ARGS)
{
	int			unreserved_workers;

	unreserved_workers = guc_max_background_workers - bgw_total_workers_get();
	PG_RETURN_INT32(unreserved_workers);
}

Datum
ts_bgw_db_workers_start(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(bgw_message_send_and_wait(START, MyDatabaseId));
}

Datum
ts_bgw_db_workers_stop(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(bgw_message_send_and_wait(STOP, MyDatabaseId));
}


Datum
ts_bgw_db_workers_restart(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(bgw_message_send_and_wait(RESTART, MyDatabaseId));
}
