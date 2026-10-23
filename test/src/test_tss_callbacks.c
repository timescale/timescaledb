/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

#include <executor/instrument.h>
#include <fmgr.h>
#include <funcapi.h>

#include "export.h"
#include "guc.h"
#include "tss_callbacks.h"

/* Exercise both callback directions above the 32-bit token range. */
static TSSStatementScopeToken test_tss_scope_next_token = INT64_C(1) << 40;
static TSSStatementScopeToken test_tss_scope_active_token = TSS_STATEMENT_SCOPE_NOT_ENTERED;
static bool test_tss_scope_disable_guc_on_enter;
static bool test_tss_scope_reject_entry;
static bool test_tss_tracking_enabled = true;

static void
test_tss_store_hook(const char *query, int query_location, int query_len, uint64_t query_id,
					uint64_t total_time, uint64_t rows, const BufferUsage *bufusage,
					const WalUsage *walusage)
{
	Assert(test_tss_scope_active_token == TSS_STATEMENT_SCOPE_NOT_ENTERED);
	elog(NOTICE,
		 "test_tss_callbacks (mock): query=%s, len=%d, id=" UINT64_FORMAT ", rows=" UINT64_FORMAT,
		 query,
		 query_len,
		 (uint64) query_id,
		 (uint64) rows);
}

static bool
test_tss_enabled_hook(int level)
{
	if (!test_tss_tracking_enabled)
	{
		elog(NOTICE, "test_tss_callbacks (mock): tracking declined");
	}
	return test_tss_tracking_enabled;
}

static TSSStatementScopeToken
test_tss_scope_enter_hook(void)
{
	TSSStatementScopeToken token;

	Assert(test_tss_scope_active_token == TSS_STATEMENT_SCOPE_NOT_ENTERED);
	if (test_tss_scope_reject_entry)
	{
		test_tss_scope_reject_entry = false;
		elog(NOTICE, "test_tss_callbacks (mock): scope entry declined");
		return TSS_STATEMENT_SCOPE_NOT_ENTERED;
	}

	token = ++test_tss_scope_next_token;
	test_tss_scope_active_token = token;
	if (test_tss_scope_disable_guc_on_enter)
	{
		test_tss_scope_disable_guc_on_enter = false;
		ts_guc_enable_tss_callbacks = false;
	}
	elog(NOTICE, "test_tss_callbacks (mock): enter scope " INT64_FORMAT, (int64) token);

	return token;
}

static void
test_tss_scope_exit_hook(TSSStatementScopeToken token)
{
	Assert(token == test_tss_scope_active_token);
	elog(NOTICE, "test_tss_callbacks (mock): exit scope " INT64_FORMAT, (int64) token);
	test_tss_scope_active_token = TSS_STATEMENT_SCOPE_NOT_ENTERED;
}

static bool
test_tss_scope_reset_hook(void)
{
	bool recovered = test_tss_scope_active_token != TSS_STATEMENT_SCOPE_NOT_ENTERED;

	test_tss_scope_active_token = TSS_STATEMENT_SCOPE_NOT_ENTERED;
	return recovered;
}

TSSCallbacks test_tss_callbacks_v1 = {
	.version_num = TSS_CALLBACKS_VERSION,
	.tss_store_hook = test_tss_store_hook,
	.tss_enabled = test_tss_enabled_hook,
};

static struct
{
	TSSStatementScopeCallbacks callbacks;
	uint64 appended_tail;
} test_tss_scope_callbacks_v1 = {
	.callbacks =
		{
			.version_num = TSS_STATEMENT_SCOPE_CALLBACKS_VERSION,
			.struct_size = sizeof(test_tss_scope_callbacks_v1),
			.enter = test_tss_scope_enter_hook,
			.exit = test_tss_scope_exit_hook,
			.reset = test_tss_scope_reset_hook,
		},
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

TS_FUNCTION_INFO_V1(ts_setup_tss_scope_hook_v1);
Datum
ts_setup_tss_scope_hook_v1(PG_FUNCTION_ARGS)
{
	TSSStatementScopeCallbacks **ptr = (TSSStatementScopeCallbacks **) find_rendezvous_variable(
		TSS_STATEMENT_SCOPE_CALLBACKS_VAR_NAME);
	*ptr = &test_tss_scope_callbacks_v1.callbacks;

	PG_RETURN_NULL();
}

TS_FUNCTION_INFO_V1(ts_teardown_tss_scope_hook_v1);
Datum
ts_teardown_tss_scope_hook_v1(PG_FUNCTION_ARGS)
{
	TSSStatementScopeCallbacks **ptr = (TSSStatementScopeCallbacks **) find_rendezvous_variable(
		TSS_STATEMENT_SCOPE_CALLBACKS_VAR_NAME);
	*ptr = NULL;

	PG_RETURN_NULL();
}

TS_FUNCTION_INFO_V1(ts_setup_tss_scope_hook_invalid);
Datum
ts_setup_tss_scope_hook_invalid(PG_FUNCTION_ARGS)
{
	static TSSStatementScopeCallbacks callbacks;
	TSSStatementScopeCallbacks **ptr = (TSSStatementScopeCallbacks **) find_rendezvous_variable(
		TSS_STATEMENT_SCOPE_CALLBACKS_VAR_NAME);

	callbacks = test_tss_scope_callbacks_v1.callbacks;
	callbacks.struct_size = sizeof(callbacks);
	switch (PG_GETARG_INT32(0))
	{
		case 0:
			callbacks.struct_size = TSS_STATEMENT_SCOPE_CALLBACKS_V1_MIN_SIZE - 1;
			break;
		case 1:
			callbacks.version_num = 0;
			break;
		case 2:
			callbacks.enter = NULL;
			break;
		case 3:
			callbacks.reset = NULL;
			break;
		case 4:
			callbacks.exit = NULL;
			break;
		default:
			elog(ERROR, "unknown invalid scope callback variant");
	}
	*ptr = &callbacks;

	PG_RETURN_NULL();
}

TS_FUNCTION_INFO_V1(ts_arm_tss_scope_disable_guc);
Datum
ts_arm_tss_scope_disable_guc(PG_FUNCTION_ARGS)
{
	test_tss_scope_disable_guc_on_enter = true;
	PG_RETURN_NULL();
}

TS_FUNCTION_INFO_V1(ts_arm_tss_scope_reject_entry);
Datum
ts_arm_tss_scope_reject_entry(PG_FUNCTION_ARGS)
{
	test_tss_scope_reject_entry = true;
	PG_RETURN_NULL();
}

TS_FUNCTION_INFO_V1(ts_set_tss_tracking_enabled);
Datum
ts_set_tss_tracking_enabled(PG_FUNCTION_ARGS)
{
	test_tss_tracking_enabled = PG_GETARG_BOOL(0);
	PG_RETURN_NULL();
}

/* This version will mismatch with the current supported */
TSSCallbacks test_tss_callbacks_v0 = {
	.version_num = 0,
	.tss_store_hook = test_tss_store_hook,
	.tss_enabled = test_tss_enabled_hook,
};

TS_FUNCTION_INFO_V1(ts_setup_tss_hook_v0);
Datum
ts_setup_tss_hook_v0(PG_FUNCTION_ARGS)
{
	TSSCallbacks **ptr = (TSSCallbacks **) find_rendezvous_variable(TSS_CALLBACKS_VAR_NAME);
	*ptr = &test_tss_callbacks_v0;

	PG_RETURN_NULL();
}

TS_FUNCTION_INFO_V1(ts_setup_tss_hook_version);
Datum
ts_setup_tss_hook_version(PG_FUNCTION_ARGS)
{
	static TSSCallbacks callbacks;
	TSSCallbacks **ptr = (TSSCallbacks **) find_rendezvous_variable(TSS_CALLBACKS_VAR_NAME);

	callbacks = test_tss_callbacks_v1;
	callbacks.version_num = PG_GETARG_INT64(0);
	*ptr = &callbacks;

	PG_RETURN_NULL();
}
