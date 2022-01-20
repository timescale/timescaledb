/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/relscan.h>
#include <access/xact.h>
#include <catalog/namespace.h>
#include <storage/bufmgr.h>
#include <storage/dsm.h>
#include <storage/lmgr.h>
#include <storage/spin.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>

#include "params.h"
#include "timer_mock.h"
#include "test_utils.h"
#include "log.h"
#include "scanner.h"
#include "ts_catalog/catalog.h"

typedef struct FormData_bgw_dsm_handle
{
	/* handle is actually a uint32 */
	int64 handle;
} FormData_bgw_dsm_handle;

typedef struct TestParamsWrapper
{
	TestParams params;
	slock_t mutex;
} TestParamsWrapper;

static Oid
get_dsm_handle_table_oid()
{
	return get_relname_relid("bgw_dsm_handle_store", get_namespace_oid("public", false));
}

static void
params_register_dsm_handle(dsm_handle handle)
{
	Relation rel;
	TableScanDesc scan;
	HeapTuple tuple;
	FormData_bgw_dsm_handle *fd;

	rel = table_open(get_dsm_handle_table_oid(), RowExclusiveLock);
	scan = table_beginscan(rel, SnapshotSelf, 0, NULL);
	tuple = heap_copytuple(heap_getnext(scan, ForwardScanDirection));
	fd = (FormData_bgw_dsm_handle *) GETSTRUCT(tuple);
	fd->handle = handle;
	ts_catalog_update(rel, tuple);
	heap_freetuple(tuple);
	heap_endscan(scan);
	table_close(rel, RowExclusiveLock);
}

static dsm_handle
params_load_dsm_handle()
{
	Relation rel;
	TableScanDesc scan;
	HeapTuple tuple;
	FormData_bgw_dsm_handle *fd;
	dsm_handle handle;

	rel = table_open(get_dsm_handle_table_oid(), RowExclusiveLock);
	scan = table_beginscan(rel, SnapshotSelf, 0, NULL);
	tuple = heap_getnext(scan, ForwardScanDirection);
	TestAssertTrue(tuple != NULL);
	tuple = heap_copytuple(tuple);
	fd = (FormData_bgw_dsm_handle *) GETSTRUCT(tuple);
	handle = fd->handle;
	heap_freetuple(tuple);
	heap_endscan(scan);
	table_close(rel, RowExclusiveLock);

	return handle;
}

static dsm_handle
params_get_dsm_handle()
{
	static dsm_handle handle = 0;

	if (handle == 0)
		handle = params_load_dsm_handle();

	return handle;
}

static TestParamsWrapper *
params_open_wrapper(bool *do_close)
{
	dsm_segment *seg;
	dsm_handle handle = params_get_dsm_handle();
	TestParamsWrapper *wrapper;

	/*
	 * If segment is returned via the mapping then there's no need to call
	 * dsm_detach on it in params_close_wrapper
	 */
	seg = dsm_find_mapping(handle);
	if (seg == NULL)
	{
		seg = dsm_attach(handle);
		if (seg == NULL)
			elog(ERROR, "got NULL segment in params_open_wrapper");
		*do_close = true;
	}
	else
		*do_close = false;

	TestAssertTrue(seg != NULL);

	wrapper = dsm_segment_address(seg);

	TestAssertTrue(wrapper != NULL);

	return wrapper;
};

static void
params_close_wrapper(TestParamsWrapper *wrapper)
{
	dsm_segment *seg = dsm_find_mapping(params_get_dsm_handle());

	TestAssertTrue(seg != NULL);
	dsm_detach(seg);
}

TestParams *
ts_params_get()
{
	bool do_close;
	TestParamsWrapper *wrapper = params_open_wrapper(&do_close);
	TestParams *res;

	TestAssertTrue(wrapper != NULL);

	res = palloc(sizeof(TestParams));

	SpinLockAcquire(&wrapper->mutex);

	memcpy(res, &wrapper->params, sizeof(TestParams));

	SpinLockRelease(&wrapper->mutex);

	if (do_close)
		params_close_wrapper(wrapper);

	return res;
};

void
ts_params_set_time(int64 new_val, bool set_latch)
{
	bool do_close;
	TestParamsWrapper *wrapper = params_open_wrapper(&do_close);

	TestAssertTrue(wrapper != NULL);

	SpinLockAcquire(&wrapper->mutex);
	wrapper->params.current_time = new_val;
	SpinLockRelease(&wrapper->mutex);

	if (set_latch)
		SetLatch(&wrapper->params.timer_latch);

	if (do_close)
		params_close_wrapper(wrapper);
}

void
ts_initialize_timer_latch()
{
	bool do_close;
	TestParamsWrapper *wrapper = params_open_wrapper(&do_close);

	TestAssertTrue(wrapper != NULL);

	SpinLockAcquire(&wrapper->mutex);

	InitLatch(&wrapper->params.timer_latch);

	SpinLockRelease(&wrapper->mutex);

	if (do_close)
		params_close_wrapper(wrapper);
}

void
ts_reset_and_wait_timer_latch()
{
	bool do_close;
	TestParamsWrapper *wrapper = params_open_wrapper(&do_close);

	TestAssertTrue(wrapper != NULL);

	ResetLatch(&wrapper->params.timer_latch);
	WaitLatch(&wrapper->params.timer_latch,
			  WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
			  10000,
			  PG_WAIT_EXTENSION);

	if (do_close)
		params_close_wrapper(wrapper);
}

static void
params_set_mock_wait_type(MockWaitType new_val)
{
	bool do_close;
	TestParamsWrapper *wrapper = params_open_wrapper(&do_close);

	TestAssertTrue(wrapper != NULL);

	SpinLockAcquire(&wrapper->mutex);

	wrapper->params.mock_wait_type = new_val;

	SpinLockRelease(&wrapper->mutex);

	if (do_close)
		params_close_wrapper(wrapper);
}

TS_FUNCTION_INFO_V1(ts_bgw_params_reset_time);
Datum
ts_bgw_params_reset_time(PG_FUNCTION_ARGS)
{
	ts_params_set_time(PG_GETARG_INT64(0), PG_GETARG_BOOL(1));

	PG_RETURN_VOID();
}

TS_FUNCTION_INFO_V1(ts_bgw_params_mock_wait_returns_immediately);
Datum
ts_bgw_params_mock_wait_returns_immediately(PG_FUNCTION_ARGS)
{
	params_set_mock_wait_type(PG_GETARG_INT32(0));

	PG_RETURN_VOID();
}

TS_FUNCTION_INFO_V1(ts_bgw_params_create);
Datum
ts_bgw_params_create(PG_FUNCTION_ARGS)
{
	dsm_segment *seg = dsm_create(sizeof(TestParamsWrapper), 0);
	TestParamsWrapper *params;

	TestAssertTrue(seg != NULL);

	params = dsm_segment_address(seg);
	*params = (TestParamsWrapper)
	{
		.params =
		{
			.current_time = 0,
		},
	};
	SpinLockInit(&params->mutex);

	params_register_dsm_handle(dsm_segment_handle(seg));

	dsm_pin_mapping(seg);
	dsm_pin_segment(seg);

	PG_RETURN_VOID();
}

TS_FUNCTION_INFO_V1(ts_bgw_params_destroy);
Datum
ts_bgw_params_destroy(PG_FUNCTION_ARGS)
{
	/*
	 * Removing shared memory segment unpin for now because:
	 * 1) This can fail in EXEC_BACKEND cases.
	 * 2) There's no way to unpin in PG9.6.
	 * 3) The EXEC_BACKEND compile-time flag is not correctly passed down.
	 * 4) This should only affect tests, not actual DB functionality.
	 * #if PG10 && !defined(EXEC_BACKEND)
	 *	dsm_unpin_segment(params_get_dsm_handle());
	 * #endif
	 */
	PG_RETURN_VOID();
}
