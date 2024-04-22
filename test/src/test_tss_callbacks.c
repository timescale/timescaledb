/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

#include <fmgr.h>
#include <funcapi.h>

#include "export.h"
#include "tss_callbacks.h"

static void
test_tss_store_hook(const char *query, int query_location, int query_len, uint64 query_id,
					uint64 total_time, uint64 rows, const BufferUsage *bufusage,
					const WalUsage *walusage)
{
	elog(NOTICE,
		 "test_tss_callbacks (mock): query=%s, len=%d, id=" INT64_FORMAT ", rows=" INT64_FORMAT,
		 query,
		 query_len,
		 query_id,
		 rows);
}

static bool
test_tss_enabled_hook(int level)
{
	return true;
}

TSSCallbacks test_tss_callbacks_v1 = {
	.version_num = 1,
	.tss_store_hook = test_tss_store_hook,
	.tss_enabled_hook_type = test_tss_enabled_hook,
};

TS_FUNCTION_INFO_V1(ts_setup_tss_hook_v1);
Datum
ts_setup_tss_hook_v1(PG_FUNCTION_ARGS)
{
	TSSCallbacks **ptr = (TSSCallbacks **) find_rendezvous_variable(TSS_CALLBACKS_VAR_NAME);
	*ptr = &test_tss_callbacks_v1;

	PG_RETURN_NULL();
}

TS_FUNCTION_INFO_V1(ts_teardown_tss_hook_v1);
Datum
ts_teardown_tss_hook_v1(PG_FUNCTION_ARGS)
{
	TSSCallbacks **ptr = (TSSCallbacks **) find_rendezvous_variable(TSS_CALLBACKS_VAR_NAME);
	*ptr = NULL;

	PG_RETURN_NULL();
}

/* This version will mismatch with the current supported */
TSSCallbacks test_tss_callbacks_v0 = {
	.version_num = 0,
	.tss_store_hook = test_tss_store_hook,
	.tss_enabled_hook_type = test_tss_enabled_hook,
};

TS_FUNCTION_INFO_V1(ts_setup_tss_hook_v0);
Datum
ts_setup_tss_hook_v0(PG_FUNCTION_ARGS)
{
	TSSCallbacks **ptr = (TSSCallbacks **) find_rendezvous_variable(TSS_CALLBACKS_VAR_NAME);
	*ptr = &test_tss_callbacks_v0;

	PG_RETURN_NULL();
}
