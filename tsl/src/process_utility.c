/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/xact.h>
#include <catalog/namespace.h>
#include <catalog/pg_trigger.h>
#include <commands/event_trigger.h>
#include <commands/tablecmds.h>
#include <nodes/makefuncs.h>
#include <nodes/nodes.h>
#include <nodes/parsenodes.h>
#include <storage/lockdefs.h>

#include "compression/create.h"
#include "continuous_aggs/create.h"
#include "hypercore/hypercore_handler.h"
#include "hypercore/utils.h"
#include "hypertable_cache.h"
#include "process_utility.h"
#include "ts_catalog/continuous_agg.h"

DDLResult
tsl_ddl_command_start(ProcessUtilityArgs *args)
{
	DDLResult result = DDL_CONTINUE;

	switch (nodeTag(args->parsetree))
	{
		case T_AlterTableStmt:
		{
			AlterTableStmt *stmt = castNode(AlterTableStmt, args->parsetree);
			ListCell *lc;

			foreach (lc, stmt->cmds)
			{
				AlterTableCmd *cmd = lfirst_node(AlterTableCmd, lc);

				switch (cmd->subtype)
				{
#if PG15_GE
					case AT_SetAccessMethod:
					{
						Oid relid = AlterTableLookupRelation(stmt, NoLock);
						bool to_hypercore = (strcmp(cmd->name, TS_HYPERCORE_TAM_NAME) == 0);
						Relation rel = RelationIdGetRelation(relid);
						bool is_hypercore = rel->rd_tableam == hypercore_routine();
						RelationClose(rel);

						/* If neither the current tableam nor the desired
						 * tableam is hypercore, we do nothing. We also do
						 * nothing if the table is already using hypercore
						 * and we are trying to convert to hypercore
						 * again. */
						if (is_hypercore == to_hypercore)
							break;
						/* Here we know that we are either moving to or from a
						 * hypercore. Check that it is on a chunk or
						 * hypertable. */
						Chunk *chunk = ts_chunk_get_by_relid(relid, false);

						if (chunk)
						{
							/* Check if we can do quick migration */
							if (!is_hypercore && ts_chunk_is_compressed(chunk))
							{
								hypercore_set_am(stmt->relation);
								/* Skip this command in the alter table
								 * statement since we process it via quick
								 * migration */
								stmt->cmds = foreach_delete_current(stmt->cmds, lc);
								continue;
							}

							hypercore_alter_access_method_begin(relid, !to_hypercore);
						}
						else if (!ts_is_hypertable(relid))
							ereport(ERROR,
									errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("hypercore access method not supported on \"%s\"",
										   stmt->relation->relname),
									errdetail("Hypercore access method is only supported on "
											  "hypertables and chunks."));

						break;
					}
#endif
					default:
						break;
				}
			}

			/* If there are no commands left, then there is no point in
			 * processing the alter table statement */
			if (stmt->cmds == NIL)
				result = DDL_DONE;
			break;
		}
		default:
			break;
	}

	return result;
}

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

void
tsl_ddl_command_end(EventTriggerData *command)
{
	switch (nodeTag(command->parsetree))
	{
		case T_AlterTableStmt:
		{
			AlterTableStmt *stmt = castNode(AlterTableStmt, command->parsetree);
			ListCell *lc;

			foreach (lc, stmt->cmds)
			{
				AlterTableCmd *cmd = lfirst_node(AlterTableCmd, lc);

				switch (cmd->subtype)
				{
#if PG15_GE
					case AT_SetAccessMethod:
					{
						Oid relid = AlterTableLookupRelation(stmt, NoLock);
						bool to_hypercore = (strcmp(cmd->name, TS_HYPERCORE_TAM_NAME) == 0);
						hypercore_alter_access_method_finish(relid, !to_hypercore);
						break;
					}
#endif
					default:
						break;
				}
			}

			break;
		}
		default:
			break;
	}
}
