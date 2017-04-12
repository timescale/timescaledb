#include <postgres.h>
#include <pg_config.h>
#include <access/xact.h>
#include <commands/extension.h>

#define MIN_SUPPORTED_VERSION_STR "9.6"
#define MIN_SUPPORTED_VERSION_NUM 90600

#if PG_VERSION_NUM < MIN_SUPPORTED_VERSION_NUM
    #error "Unsupported version of PostgreSQL. Check src/init.c for supported versions."
#endif

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

extern void _hypertable_cache_init(void);
extern void _hypertable_cache_fini(void);

extern void _chunk_cache_init(void);
extern void _chunk_cache_fini(void);

extern void _cache_invalidate_init(void);
extern void _cache_invalidate_fini(void);

extern void _planner_init(void);
extern void _planner_fini(void);

extern void _process_utility_init(void);
extern void _process_utility_fini(void);

extern void _PG_init(void);
extern void _PG_fini(void);

void
_PG_init(void)
{
	elog(INFO, "timescaledb loaded");
	_hypertable_cache_init();
	_chunk_cache_init();
	_cache_invalidate_init();
	_planner_init();
	_process_utility_init();
}

void
_PG_fini(void)
{
	_process_utility_fini();
	_planner_fini();
	_cache_invalidate_fini();
	_hypertable_cache_fini();
	_chunk_cache_fini();
}
