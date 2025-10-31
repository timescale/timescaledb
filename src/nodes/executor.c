/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <executor/executor.h>

#include "chunk.h"
#include "extension.h"
#include "hypertable.h"
#include "nodes/executor.h"
#include "partition_chunk.h"

void _planner_init(void);
void _planner_fini(void);

static ExecutorEnd_hook_type prev_executor_end_hook = NULL;

static void
ts_executor_end_hook(QueryDesc *queryDesc)
{
	ListCell *lc;

	if (prev_executor_end_hook)
		prev_executor_end_hook(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);

	/*
	 * Chunks cannot be created as a partition or attached as partition until
	 * this point since Postgres does not allow such operations when there is
	 * an open reference to the parent table. ModifyTable node opens the parent
	 * table and it only gets closed in ExecEndPlan.
	 */
	if (queryDesc->operation == CMD_INSERT && ts_extension_is_loaded())
	{
		foreach (lc, queryDesc->plannedstmt->rtable)
		{
			RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);
			if (ts_hypertable_relid_to_id(rte->relid) != INVALID_HYPERTABLE_ID)
			{
				ListCell *lc_oid;
				PartChunkCacheEntry *entry = ts_partition_cache_get_by_hypertable(rte->relid);

				if (entry == NULL)
					continue;

				foreach (lc_oid, entry->chunk_oids)
				{
					Oid part_to_attach = lfirst_oid(lc_oid);
					ts_partition_chunk_attach(entry->ht, ts_chunk_get_by_relid(part_to_attach, true));
				}
			}
		}

		ts_partition_cache_destroy();
	}
}

void
_executor_init(void)
{
	prev_executor_end_hook = ExecutorEnd_hook;
	ExecutorEnd_hook = ts_executor_end_hook;
}

void
_executor_fini(void)
{
	ExecutorEnd_hook = prev_executor_end_hook;
}
