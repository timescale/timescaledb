/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/xact.h>
#include <commands/event_trigger.h>
#include <catalog/namespace.h>
#include <catalog/pg_trigger.h>

#include "compression/create.h"
#include "continuous_aggs/create.h"
#include "ts_catalog/continuous_agg.h"
#include "hypertable_cache.h"
#include "process_utility.h"

/* AlterTableCmds that need tsl side processing invoke this function
 * we only process AddColumn command right now.
 */
void
tsl_process_altertable_cmd(Hypertable *ht, const AlterTableCmd *cmd)
{
	switch (cmd->subtype)
	{
		case AT_AddColumn:
#if PG16_LT
		case AT_AddColumnRecurse:
#endif
			if (TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht) ||
				TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(ht))
			{
				ColumnDef *orig_coldef = castNode(ColumnDef, cmd->def);
				tsl_process_compress_table_add_column(ht, orig_coldef);
			}
			break;
		case AT_DropColumn:
#if PG16_LT
		case AT_DropColumnRecurse:
#endif
			if (TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht) ||
				TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(ht))
			{
				tsl_process_compress_table_drop_column(ht, cmd->name);
			}
			break;
		default:
			break;
	}
}

void
tsl_process_rename_cmd(Oid relid, Cache *hcache, const RenameStmt *stmt)
{
	if (stmt->renameType == OBJECT_COLUMN)
	{
		Hypertable *ht = ts_hypertable_cache_get_entry(hcache, relid, CACHE_FLAG_MISSING_OK);
		if (!ht)
		{
			ContinuousAgg *cagg = ts_continuous_agg_find_by_relid(relid);
			if (cagg)
			{
				ht = ts_hypertable_cache_get_entry_by_id(hcache, cagg->data.mat_hypertable_id);
				Assert(ht);
				cagg_rename_view_columns(cagg);
			}
		}

		/* Continuous aggregates do not have compression right now, but we
		 * check the status for the materialized hypertable anyway since it is
		 * harmless. */
		if (ht &&
			(TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht) || TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(ht)))
		{
			tsl_process_compress_table_rename_column(ht, stmt);
		}
	}
}
