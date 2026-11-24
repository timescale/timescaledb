/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

#include <executor/instrument.h>
#include <executor/spi.h>
#include <fmgr.h>
#include <funcapi.h>
#include <miscadmin.h>
#include <postmaster/bgworker.h>
#include <storage/dsm.h>

#include "bgw/job.h"
#include "bgw/job_stat.h"
#include "bgw/worker.h"
#include "export.h"
#include "extension.h"
#include "guc.h"
#include "test_utils.h"
#include "tss_callbacks.h"

/* Exercise both callback directions above the 32-bit token range. */
static TSSStatementScopeToken test_tss_scope_next_token = INT64_C(1) << 40;
static TSSStatementScopeToken test_tss_scope_active_token = TSS_STATEMENT_SCOPE_NOT_ENTERED;
static bool test_tss_scope_disable_guc_on_enter;
static bool test_tss_scope_reject_entry;
static bool test_tss_tracking_enabled = true;

/* After initialization, only the worker writes; the caller reads after shutdown. */
typedef struct TestTssBgwState
{
	Oid database_id;
	int entries;
	int exits;
	int stores;
	int checkpoints;
	int sqlerrcode;
	bool scope_active;
	bool finished;
} TestTssBgwState;

static TestTssBgwState *test_tss_bgw_state;

static void
test_tss_store_hook(const char *query, int query_location, int query_len, uint64_t query_id,
					uint64_t total_time, uint64_t rows, const BufferUsage *bufusage,
					const WalUsage *walusage)
{
	Assert(test_tss_scope_active_token == TSS_STATEMENT_SCOPE_NOT_ENTERED);
	if (test_tss_bgw_state != NULL)
	{
		test_tss_bgw_state->stores++;
	}
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

	if (test_tss_scope_active_token > 0)
	{
		return TSS_STATEMENT_SCOPE_NOT_ENTERED;
	}
	if (test_tss_scope_reject_entry)
	{
		test_tss_scope_reject_entry = false;
		elog(NOTICE, "test_tss_callbacks (mock): scope entry declined");
		return TSS_STATEMENT_SCOPE_NOT_ENTERED;
	}

	token = ++test_tss_scope_next_token;
	test_tss_scope_active_token = token;
	if (test_tss_bgw_state != NULL)
	{
		test_tss_bgw_state->entries++;
	}
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
	if (test_tss_bgw_state != NULL)
	{
		test_tss_bgw_state->exits++;
	}
	elog(NOTICE, "test_tss_callbacks (mock): exit scope " INT64_FORMAT, (int64) token);
	test_tss_scope_active_token = TSS_STATEMENT_SCOPE_NOT_ENTERED;
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

/*
 * Install the mock before the real entrypoint begins tracking. The scheduler
 * test hook cannot be used here: the job entrypoint disables callbacks for it.
 */
TS_FUNCTION_INFO_V1(ts_test_tss_bgw_main);
Datum
ts_test_tss_bgw_main(PG_FUNCTION_ARGS)
{
	dsm_segment *seg;
	TSSCallbacks **legacy;
	TSSStatementScopeCallbacks **scope;

	seg = dsm_attach(DatumGetUInt32(MyBgworkerEntry->bgw_main_arg));
	TestAssertTrue(seg != NULL);
	test_tss_bgw_state = dsm_segment_address(seg);
	/* The real entrypoint expects the database OID, not our DSM handle. */
	MyBgworkerEntry->bgw_main_arg = ObjectIdGetDatum(test_tss_bgw_state->database_id);
	legacy = (TSSCallbacks **) find_rendezvous_variable(TSS_CALLBACKS_VAR_NAME);
	scope = (TSSStatementScopeCallbacks **) find_rendezvous_variable(
		TSS_STATEMENT_SCOPE_CALLBACKS_VAR_NAME);
	*legacy = &test_tss_callbacks_v1;
	*scope = &test_tss_scope_callbacks_v1.callbacks;

	PG_TRY();
	{
		ts_bgw_job_entrypoint(fcinfo);
	}
	PG_CATCH();
	{
		test_tss_bgw_state->sqlerrcode = geterrcode();
		test_tss_bgw_state->scope_active =
			test_tss_scope_active_token != TSS_STATEMENT_SCOPE_NOT_ENTERED;
		test_tss_bgw_state->finished = true;
		PG_RE_THROW();
	}
	PG_END_TRY();

	test_tss_bgw_state->scope_active =
		test_tss_scope_active_token != TSS_STATEMENT_SCOPE_NOT_ENTERED;
	test_tss_bgw_state->finished = true;
	PG_RETURN_VOID();
}

TS_FUNCTION_INFO_V1(ts_test_tss_scope_active);
Datum
ts_test_tss_scope_active(PG_FUNCTION_ARGS)
{
	TestAssertTrue(MyBgworkerEntry != NULL && test_tss_bgw_state != NULL);
	TestAssertTrue(test_tss_scope_active_token > 0);
	test_tss_bgw_state->checkpoints++;
	PG_RETURN_BOOL(true);
}

/* A procedure so the scheduler's mark-start transaction can commit before waiting. */
TS_FUNCTION_INFO_V1(ts_test_tss_run_bgw_job);
Datum
ts_test_tss_run_bgw_job(PG_FUNCTION_ARGS)
{
	int32 job_id = PG_GETARG_INT32(0);
	bool expect_error = PG_GETARG_BOOL(1);
	BackgroundWorker worker = {
		.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION,
		.bgw_start_time = BgWorkerStart_RecoveryFinished,
		.bgw_restart_time = BGW_NEVER_RESTART,
		.bgw_notify_pid = MyProcPid,
		.bgw_name = "statement scope test",
		.bgw_function_name = "ts_test_tss_bgw_main",
	};
	BackgroundWorkerHandle *handle;
	BgwParams params = { 0 };
	dsm_segment *seg;
	TestTssBgwState *state;
	BgwJob *job;

	TestAssertTrue(SPI_connect_ext(SPI_OPT_NONATOMIC) == SPI_OK_CONNECT);
	job = ts_bgw_job_find(job_id, CurrentMemoryContext, true);
	ts_bgw_job_stat_mark_start(job);
	params.job_id = job_id;
	params.user_oid = job->fd.owner;
	params.job_history_id = job->job_history.id;
	params.job_history_execution_start = job->job_history.execution_start;
	strlcpy(worker.bgw_library_name, ts_extension_get_so_name(), BGW_MAXLEN);
	SPI_commit();
	TestAssertTrue(SPI_finish() == SPI_OK_FINISH);

	seg = dsm_create(sizeof(*state), 0);
	state = dsm_segment_address(seg);
	memset(state, 0, sizeof(*state));
	state->database_id = MyDatabaseId;
	worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(seg));
	memcpy(worker.bgw_extra, &params, sizeof(params));
	TestAssertTrue(RegisterDynamicBackgroundWorker(&worker, &handle));
	PG_TRY();
	{
		/* A fast worker may finish before the caller observes startup. */
		TestAssertTrue(WaitForBackgroundWorkerShutdown(handle) == BGWH_STOPPED);
	}
	PG_CATCH();
	{
		TerminateBackgroundWorker(handle);
		PG_RE_THROW();
	}
	PG_END_TRY();

	TestAssertTrue(state->finished);
	TestAssertInt64Eq(state->entries, 1);
	TestAssertInt64Eq(state->exits, 1);
	TestAssertInt64Eq(state->stores, expect_error ? 0 : 1);
	TestAssertInt64Eq(state->checkpoints, 2);
	TestAssertInt64Eq(state->sqlerrcode, expect_error ? ERRCODE_RAISE_EXCEPTION : 0);
	TestAssertTrue(!state->scope_active);
	dsm_detach(seg);
	pfree(handle);
	PG_RETURN_VOID();
}
