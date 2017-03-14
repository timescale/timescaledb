#include <postgres.h>
#include <access/xact.h>
#include <commands/extension.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

extern void _hypertable_cache_init(void);
extern void _hypertable_cache_fini(void);

extern void _chunk_cache_init(void);
extern void _chunk_cache_fini(void);

extern void _cache_invalidate_init(void);
extern void _cache_invalidate_fini(void);
extern void _cache_invalidate_extload(void);

extern void _planner_init(void);
extern void _planner_fini(void);

extern void _process_utility_init(void);
extern void _process_utility_fini(void);

extern void _xact_init(void);
extern void _xact_fini(void);

extern void _PG_init(void);
extern void _PG_fini(void);

extern bool IobeamLoaded(void);

void
_PG_init(void)
{
	elog(INFO, "timescaledb loaded");
	_hypertable_cache_init();
	_chunk_cache_init();
	_cache_invalidate_init();
	_planner_init();
	_process_utility_init();
	_xact_init();
}

void
_PG_fini(void)
{
	_xact_fini();
	_process_utility_fini();
	_planner_fini();
	_cache_invalidate_fini();
	_hypertable_cache_fini();
	_chunk_cache_fini();
}

static bool isLoaded = false;

bool
IobeamLoaded(void)
{
	if (!isLoaded)
	{
		Oid			id;

		if (!IsTransactionState())
		{
			return false;
		}

		id = get_extension_oid("timescaledb", true);

		if (id != InvalidOid && !(creating_extension && id == CurrentExtensionObject))
		{
			isLoaded = true;
			_cache_invalidate_extload();
		}
	}
	return isLoaded;
}
