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
#include <nodes/nodes.h>
#include <nodes/parsenodes.h>
#include <storage/lockdefs.h>

#include "compression/compressionam_handler.h"
#include "compression/create.h"
#include "continuous_aggs/create.h"
#include "hypertable_cache.h"
#include "process_utility.h"
#include "ts_catalog/continuous_agg.h"

void
tsl_ddl_command_start(ProcessUtilityArgs *args)
{
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
						bool to_compressionam = (strcmp(cmd->name, "hyperstore") == 0);

						if (to_compressionam)
							compressionam_alter_access_method_begin(relid, false);
						else
						{
							Relation rel = RelationIdGetRelation(relid);

							if (rel->rd_tableam == compressionam_routine())
								compressionam_alter_access_method_begin(relid, true);
							RelationClose(rel);
						}
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

void
tsl_process_vacuum_cmd(const VacuumStmt *vacstmt)
{
	/* Check if this is an ANALYZE statement */
	bool analyze = !vacstmt->is_vacuumcmd;
	ListCell *lc;

	/* It could be a VACUUM statement with analyze option */
	if (!analyze)
	{
		foreach (lc, vacstmt->options)
		{
			DefElem *opt = (DefElem *) lfirst(lc);

			if (strcmp(opt->defname, "analyze") == 0)
				analyze = defGetBoolean(opt);
		}
	}

	/* Return if not doing analyze */
	if (!analyze)
		return;

	/* Check for compression TAM rels being analyzed */
	foreach (lc, vacstmt->rels)
	{
		VacuumRelation *vrel = lfirst_node(VacuumRelation, lc);
		Oid relid = vrel->oid;
		bool locked = false;

		if (!OidIsValid(relid))
		{
			relid = RangeVarGetRelid(vrel->relation, AccessShareLock, true);

			if (!OidIsValid(relid))
				continue;

			locked = true;
		}

		Relation rel = table_open(relid, locked ? NoLock : AccessShareLock);

		if (rel->rd_tableam == compressionam_routine())
		{
			/* When analyzing the compression TAM, we need to return the sum
			 * of the number of blocks in both the compressed and
			 * non-compressed relation. Remember that we need to compute this
			 * sum in the relation_size() callback. */
			compressionam_set_analyze_relid(relid);
		}

		table_close(rel, NoLock);
	}
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
						bool to_compressionam = (strcmp(cmd->name, "hyperstore") == 0);
						compressionam_alter_access_method_finish(relid, !to_compressionam);
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
