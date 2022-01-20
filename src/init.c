/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

#include <access/xact.h>
#include <commands/extension.h>
#include <miscadmin.h>
#include <utils/guc.h>
#include <parser/analyze.h>
#include <storage/ipc.h>

#include "extension.h"
#include "bgw/launcher_interface.h"
#include "guc.h"
#include "debug_guc.h"
#include "ts_catalog/catalog.h"
#include "version.h"
#include "compat/compat.h"
#include "config.h"
#include "license_guc.h"
#include "nodes/constraint_aware_append/constraint_aware_append.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

extern void _hypertable_cache_init(void);
extern void _hypertable_cache_fini(void);

extern void _cache_invalidate_init(void);
extern void _cache_invalidate_fini(void);

extern void _cache_init(void);
extern void _cache_fini(void);

extern void _planner_init(void);
extern void _planner_fini(void);

extern void _process_utility_init(void);
extern void _process_utility_fini(void);

extern void _event_trigger_init(void);
extern void _event_trigger_fini(void);

extern void _conn_plain_init();
extern void _conn_plain_fini();

#ifdef TS_USE_OPENSSL
extern void _conn_ssl_init();
extern void _conn_ssl_fini();
#endif

#ifdef TS_DEBUG
extern void _conn_mock_init();
extern void _conn_mock_fini();
#endif

extern void _chunk_append_init();

extern void TSDLLEXPORT _PG_init(void);
extern void TSDLLEXPORT _PG_fini(void);

TS_FUNCTION_INFO_V1(ts_post_load_init);

/* Called when the backend exits */
static void
cleanup_on_pg_proc_exit(int code, Datum arg)
{
	/*
	 * Order of items should be strict reverse order of _PG_init. Please
	 * document any exceptions.
	 */
#ifdef TS_DEBUG
	_conn_mock_fini();
#endif
#ifdef TS_USE_OPENSSL
	_conn_ssl_fini();
#endif
	_conn_plain_fini();
	_guc_fini();
	_process_utility_fini();
	_event_trigger_fini();
	_planner_fini();
	_cache_invalidate_fini();
	_hypertable_cache_fini();
	_cache_fini();
}

void
_PG_init(void)
{
	/*
	 * Check extension_is loaded to catch certain errors such as calls to
	 * functions defined on the wrong extension version
	 */
	ts_extension_check_version(TIMESCALEDB_VERSION_MOD);
	ts_extension_check_server_version();
	ts_bgw_check_loader_api_version();

	_cache_init();
	_hypertable_cache_init();
	_cache_invalidate_init();
	_planner_init();
	_constraint_aware_append_init();
	_chunk_append_init();
	_event_trigger_init();
	_process_utility_init();
	_guc_init();
	_conn_plain_init();
#ifdef TS_USE_OPENSSL
	_conn_ssl_init();
#endif
#ifdef TS_DEBUG
	_conn_mock_init();
	ts_debug_init();
#endif

	/* Register a cleanup function to be called when the backend exits */
	on_proc_exit(cleanup_on_pg_proc_exit, 0);
}

void
_PG_fini(void)
{
	cleanup_on_pg_proc_exit(0, 0);
}

TSDLLEXPORT Datum
ts_post_load_init(PG_FUNCTION_ARGS)
{
	/*
	 * Unfortunately, if we load the tsl during _PG_init parallel workers try
	 * to load the tsl before timescale itself, causing link-time errors. To
	 * prevent this we defer loading until here.
	 */
	ts_license_enable_module_loading();

	PG_RETURN_VOID();
}
