/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <foreign/fdwapi.h>
#include "planner.h"
#include "gapfill/planner.h"
#include "hypertable_cache.h"
#include "compat.h"
#if !PG96
#include "fdw/timescaledb_fdw.h"
#endif
#include "guc.h"
#include <planner.h>

void
tsl_create_upper_paths_hook(PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel,
							RelOptInfo *output_rel)
{
	if (UPPERREL_GROUP_AGG == stage)
		plan_add_gapfill(root, output_rel);
	if (UPPERREL_WINDOW == stage)
	{
		if (IsA(linitial(input_rel->pathlist), CustomPath))
			gapfill_adjust_window_targetlist(root, input_rel, output_rel);
	}
}

#if !PG96
/* The fdw needs to expand a distributed hypertable inside the `GetForeignPath` callback. But, since
 * the hypertable base table is not a foreign table, that callback would not normally be called.
 * Thus, we call it manually in this hook.
 */
void
tsl_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
	Cache *hcache = ts_hypertable_cache_pin();
	Hypertable *ht = ts_hypertable_cache_get_entry(hcache, rte->relid);

	if (rel->fdw_private != NULL && ht != NULL && ht->fd.replication_factor > 0)
	{
		FdwRoutine *fdw = (FdwRoutine *) DatumGetPointer(
			DirectFunctionCall1(timescaledb_fdw_handler, PointerGetDatum(NULL)));

		fdw->GetForeignRelSize(root, rel, rte->relid);
		fdw->GetForeignPaths(root, rel, rte->relid);
	}

	ts_cache_release(hcache);
}

/*
 * For distributed hypertables, we skip expanding the hypertable chunks when using the
 * per-server-queries optimization. This is because we don't want the chunk relations in the plan.
 * Instead the fdw will create paths for hypertable-server relations during path creation.
 */
bool
tsl_hypertable_should_be_expanded(RelOptInfo *rel, RangeTblEntry *rte, Hypertable *ht,
								  List *chunk_oids)
{
	if (ts_guc_enable_per_server_queries && ht->fd.replication_factor > 0)
	{
		TimescaleDBPrivate *rel_info;

		Assert(rel->fdw_private != NULL);
		rel_info = rel->fdw_private;
		rel_info->chunk_oids = chunk_oids;

		/* turn off expansion */
		rte->inh = false;
		return false;
	}
	return true;
}
#endif
