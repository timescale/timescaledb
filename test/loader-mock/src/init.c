#include <postgres.h>
#include <pg_config.h>
#include <access/xact.h>
#include <commands/extension.h>
#include <miscadmin.h>
#include <utils/guc.h>
#include <utils/inval.h>
#include <parser/analyze.h>
#include <nodes/print.h>
#include <access/parallel.h>

#define STR_EXPAND(x) #x
#define STR(x) STR_EXPAND(x)

#define TS_FUNCTION_INFO_V1(fn) \
	PGDLLEXPORT Datum fn(PG_FUNCTION_ARGS); \
	PG_FUNCTION_INFO_V1(fn)

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

extern void PGDLLEXPORT _PG_init(void);
extern void PGDLLEXPORT _PG_fini(void);

post_parse_analyze_hook_type prev_post_parse_analyze_hook;

bool		extension_invalidate(Oid relid);
bool		extension_is_loaded(void);
void		extension_check_version(const char *actual_version);

static void
cache_invalidate_callback(Datum arg, Oid relid)
{
	extension_invalidate(relid);
}

static void
post_analyze_hook(ParseState *pstate, Query *query)
{
	if (extension_is_loaded())
		elog(WARNING, "mock post_analyze_hook " STR(TIMESCALEDB_VERSION_MOD));
	if (prev_post_parse_analyze_hook != NULL && !IsParallelWorker())
		elog(ERROR, "the extension called with a loader should always have a NULL prev hook");
	if (BROKEN && !creating_extension)
		elog(ERROR, "mock broken " STR(TIMESCALEDB_VERSION_MOD));
}

void
_PG_init(void)
{
	/*
	 * Check extension_is loaded to catch certain errors such as calls to
	 * functions defined on the wrong extension version
	 */
	extension_check_version(TIMESCALEDB_VERSION_MOD);
	elog(WARNING, "mock init " STR(TIMESCALEDB_VERSION_MOD));
	prev_post_parse_analyze_hook = post_parse_analyze_hook;
	if (prev_post_parse_analyze_hook != NULL && !IsParallelWorker())
		elog(ERROR, "the extension loaded with a loader should always have a NULL prev hook");
	post_parse_analyze_hook = post_analyze_hook;
	CacheRegisterRelcacheCallback(cache_invalidate_callback, PointerGetDatum(NULL));
}

void
_PG_fini(void)
{
	post_parse_analyze_hook = prev_post_parse_analyze_hook;
	/* No way to unregister relcache callback */
}

/* mock for extension.c */
void		catalog_reset(void);
void
catalog_reset()
{
}

/* mock for guc.c */
void		hypertable_cache_invalidate_callback(void);
void
hypertable_cache_invalidate_callback(void)
{
}

TS_FUNCTION_INFO_V1(mock_function);

Datum
mock_function(PG_FUNCTION_ARGS)
{
	elog(WARNING, "mock function call " STR(TIMESCALEDB_VERSION_MOD));
	PG_RETURN_VOID();
}
