/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/xact.h>
#include <catalog/namespace.h>
#include <catalog/pg_trigger.h>
#include <catalog/pg_type.h>
#include <commands/event_trigger.h>
#include <commands/tablecmds.h>
#include <nodes/makefuncs.h>
#include <nodes/nodes.h>
#include <nodes/parsenodes.h>
#include <storage/lmgr.h>
#include <storage/lockdefs.h>
#include <utils/inval.h>
#include <utils/lsyscache.h>

#include "bgw_policy/policies_v2.h"
#include "compression/create.h"
#include "continuous_aggs/create.h"
#include "continuous_aggs/tenant_tracker.h"
#include "dimension.h"
#include "guc.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "process_utility.h"
#include "time_utils.h"
#include "ts_catalog/continuous_agg.h"
#include "ts_catalog/hypertable_cagg_settings.h"
#include "utils.h"
#include "with_clause/alter_table_with_clause.h"
#include "with_clause/with_clause_parser.h"

/* AlterTableCmds that need tsl side processing invoke this function
 * we only process AddColumn command right now.
 */
void
tsl_process_altertable_cmd(Hypertable *ht, const AlterTableCmd *cmd)
{
	switch (cmd->subtype)
	{
		case AT_AddColumn:
			if (TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(ht))
			{
				ColumnDef *orig_coldef = castNode(ColumnDef, cmd->def);
				tsl_process_compress_table_add_column(ht, orig_coldef);
			}
			break;
		case AT_DropColumn:
			if (TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(ht))
			{
				tsl_process_compress_table_drop_column(ht, cmd->name);
			}
			break;
		default:
			break;
	}
}

static int64
parse_granular_refresh_offset(WithClauseResult option, Oid time_type)
{
	Oid interval_type = InvalidOid;
	Datum interval = ts_create_table_parse_interval_value(option, time_type, &interval_type);
	int64 offset = interval_to_int64(interval, interval_type);

	if (offset < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid granular refresh offset"),
				 errdetail("timescaledb.%s must not be negative.",
						   option.definition->arg_names[0])));
	}

	return offset;
}

/* Parse the stored offset from text to its corresponding type */
static int64
parse_stored_granular_refresh_offset(const text *offset, Oid time_type)
{
	/* Matches what ts_hypertable_cagg_settings_cast_offset produces. */
	Oid datum_type = IS_INTEGER_TYPE(time_type) ? time_type : INTERVALOID;

	return interval_to_int64(ts_hypertable_cagg_settings_cast_offset(offset, time_type),
							 datum_type);
}

/*
 * ALTER TABLE <hypertable> SET (timescaledb.cagg_enable_granular_refresh = false)
 *
 * Removes the hypertable's granular refresh configuration, which stops the DML
 * path from collecting tenants, and releases the tenant tracker's shared
 * memory.
 * A no-op when nothing is configured, so the statement is idempotent.
 */
static void
granular_refresh_disable(Hypertable *ht)
{
	FormData_hypertable_cagg_settings settings = { 0 };
	List *caggs;
	ListCell *lc;

	/*
	 * ALTER TABLE carrying only timescaledb options takes no relation lock at
	 * all: the subcommand is consumed here and standard_ProcessUtility never
	 * runs, so AlterTableGetLockLevel is never reached. Acquire
	 * AccessExclusiveLock. This makes releasing the tracker's
	 * shared memory safe as the lock will block DML writers (that write to
	 * shared memory), so once we start hold this lock no writer can be
	 * inside the tracker. so all dml blocked till the end of this DDL.
	 *
	 * The lock is held past the commit callback that actually frees the
	 * tracker (PostgreSQL releases locks after XACT_EVENT_COMMIT), so the
	 * writers stay locked out for the whole window.
	 */
	LockRelationOid(ht->main_table_relid, AccessExclusiveLock);

	/* verify current settings under the lock */
	if (!ts_hypertable_cagg_settings_get(ht->fd.id, &settings, NULL))
	{
		return; /* not configured: nothing to disable */
	}

	/* check if any cagg needs this setting */
	caggs = ts_continuous_aggs_find_by_raw_table_id(ht->fd.id);

	foreach (lc, caggs)
	{
		const ContinuousAgg *cagg = lfirst(lc);

		if (cagg->data.granular_refresh_enabled)
		{
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("granular refresh is enabled for continuous aggregate \"%s\"",
							NameStr(cagg->data.user_view_name)),
					 errhint("Disable it first with ALTER MATERIALIZED VIEW ... SET "
							 "(timescaledb.enable_granular_refresh = false).")));
		}
	}

	list_free(caggs);

	ts_hypertable_cagg_settings_delete(ht->fd.id);

	/* Queue the shared-memory free for commit time */
	ts_tenant_tracker_remove_at_commit(ht->fd.id, ht->main_table_relid);

	/* Tell other backends to drop their cached pointer to the tracker*/
	CacheInvalidateRelcacheByRelid(ht->main_table_relid);
}

/*
 * ALTER TABLE <hypertable> SET (timescaledb.cagg_enable_granular_refresh = true,
 *                               timescaledb.cagg_granular_refresh_column = ...,
 *                               timescaledb.cagg_granular_refresh_start_offset = ...,
 *                               timescaledb.cagg_granular_refresh_end_offset = ...)
 *
 * Enables granular refresh of continuous aggregates on the raw hypertable.
 * Continuous aggregates opt in separately and share these settings.
 *
 *  To enable: All four options are required to enable it.
 *  Update config options: Only start_offset and end_offset can be updated.
 *  To disable: cagg_enable_granular_refresh = false
 *
 * A changed offset reaches the tracker when the next flush activates a
 * generation, so writes keep gating on the previous window until then.
 */
void
tsl_process_granular_refresh_options(Hypertable *ht, WithClauseResult *with_clause_options)
{
	if ((ts_continuous_agg_hypertable_status(ht->fd.id) & HypertableIsMaterialization) != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Granular refresh is not supported on a materialized hypertable"),
				 errhint("Granular refresh tracking can only be configured on a hypertable "
						 "that is not a continuous aggregate's internal materialization "
						 "table."),
				 errdetail("Hypertable \"%s\" is a materialized hypertable.",
						   NameStr(ht->fd.table_name))));
	}

	bool set_column = !with_clause_options[AlterTableFlagGranularRefreshColumn].is_default;
	bool set_start_offset =
		!with_clause_options[AlterTableFlagGranularRefreshStartOffset].is_default;
	bool set_end_offset = !with_clause_options[AlterTableFlagGranularRefreshEndOffset].is_default;

	/* Whether the statement mentioned timescaledb.cagg_enable_granular_refresh*/
	bool set_enable = !with_clause_options[AlterTableFlagCaggEnableGranularRefresh].is_default;

	/*whether the statement set timescaledb.cagg_enable_granular_refresh to true*/
	bool enabling =
		DatumGetBool(with_clause_options[AlterTableFlagCaggEnableGranularRefresh].parsed);

	FormData_hypertable_cagg_settings settings = { 0 };

	if (set_enable && !enabling)
	{
		if (set_column || set_start_offset || set_end_offset)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("conflicting granular refresh options"),
					 errdetail("timescaledb.cagg_enable_granular_refresh = false cannot be "
							   "combined with the timescaledb.cagg_granular_refresh_* options.")));
		}

		granular_refresh_disable(ht);
		return;
	}

	/* Enabling takes the whole configuration. Checked before anything is locked
	 * or read to avoid the cost*/
	if (enabling && (!set_column || !set_start_offset || !set_end_offset))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("incomplete granular refresh configuration"),
				 errdetail("timescaledb.cagg_enable_granular_refresh, "
						   "timescaledb.cagg_granular_refresh_column, "
						   "timescaledb.cagg_granular_refresh_start_offset and "
						   "timescaledb.cagg_granular_refresh_end_offset must all be set to enable "
						   "granular refresh.")));
	}

	/*
	 * We are going to update the setting tuple. So acquire an exclusive tuple lock on it
	 * to avoid concurrent updates to the same tuple.
	 * The tuple lock is released at the end of this transaction.
	 */
	ScanTupLock tuplock = {
		.lockmode = LockTupleExclusive,
		.waitpolicy = LockWaitBlock,
		.lockflags = TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
	};

	bool configured = ts_hypertable_cagg_settings_get(ht->fd.id, &settings, &tuplock);

	/* Enabling is only allowed for a hypertable that is not enabled yet, error out otherwise */
	if (enabling && configured)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("granular refresh is already enabled on hypertable \"%s\"",
						NameStr(ht->fd.table_name)),
				 errhint("Change timescaledb.cagg_granular_refresh_start_offset and "
						 "timescaledb.cagg_granular_refresh_end_offset on their own, without "
						 "timescaledb.cagg_enable_granular_refresh.")));
	}

	/* The granular_refresh_* options on their own change a configuration that is
	 * already stored, and anything left out of the statement keeps the value it
	 * already has. Storing one in the first place takes an enabling statement. */
	if (!configured && !enabling)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("granular refresh is not enabled on hypertable \"%s\"",
						NameStr(ht->fd.table_name)),
				 errdetail("timescaledb.cagg_enable_granular_refresh must be set to true together "
						   "with timescaledb.cagg_granular_refresh_column, "
						   "timescaledb.cagg_granular_refresh_start_offset and "
						   "timescaledb.cagg_granular_refresh_end_offset to enable it.")));
	}

	if (set_column)
	{
		char *colname =
			TextDatumGetCString(with_clause_options[AlterTableFlagGranularRefreshColumn].parsed);

		/* Tracking rows already hold tenant ids read from the configured column,
		 * and the refresh filters on that column, so the two cannot diverge.
		 * Naming the column it already has is accepted and changes nothing,
		 * whether or not the statement carries offsets as well. */
		if (configured && namestrcmp(&settings.granular_refresh_column, colname) != 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot change the granular refresh column of hypertable \"%s\"",
							NameStr(ht->fd.table_name)),
					 errdetail("The column is currently \"%s\".",
							   NameStr(settings.granular_refresh_column)),
					 errhint("Only timescaledb.cagg_granular_refresh_start_offset and "
							 "timescaledb.cagg_granular_refresh_end_offset can be changed.")));
		}

		if (colname[0] == '\0')
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("granular refresh column cannot be empty"),
					 errhint("timescaledb.cagg_granular_refresh_column must reference a valid "
							 "column.")));
		}

		AttrNumber attno = get_attnum(ht->main_table_relid, colname);

		if (attno < 1)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" does not exist", colname),
					 errhint("The timescaledb.cagg_granular_refresh_column option must reference a "
							 "valid column.")));
		}

		Oid tenant_typid;
		int32 tenant_typmod;
		Oid tenant_collid;

		get_atttypetypmodcoll(ht->main_table_relid,
							  attno,
							  &tenant_typid,
							  &tenant_typmod,
							  &tenant_collid);
		tenant_typid = getBaseTypeAndTypmod(tenant_typid, &tenant_typmod);

		if (!ts_tenant_type_is_supported(tenant_typid))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("invalid granular refresh column type"),
					 errhint(
						 "timescaledb.cagg_granular_refresh_column must be a date, integer, UUID, "
						 "or string type.")));
		}

		/* character(n) blank-pads every value to exactly n bytes, so for n over the
		 * tracker's key limit no tenant is ever storable. */
		if (tenant_typid == BPCHAROID && tenant_typmod > VARHDRSZ &&
			tenant_typmod - VARHDRSZ > TENANT_TRACKER_KEY_MAXLEN)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("granular refresh column \"%s\" is wider than the %d byte tenant key "
							"limit",
							colname,
							TENANT_TRACKER_KEY_MAXLEN)));
		}

		/* store the normalized column name */
		namestrcpy(&settings.granular_refresh_column,
				   get_attname(ht->main_table_relid, attno, false /* missing_ok */));
	}

	/* get the time type */
	Dimension *dim = ts_hyperspace_get_mutable_dimension(ht->space, DIMENSION_TYPE_OPEN, 0);
	Ensure(dim, "hypertable without open dimension");
	Oid time_type = get_atttype(dim->main_table_relid, dim->column_attno);

	int64 start_offset =
		set_start_offset ?
			parse_granular_refresh_offset(with_clause_options
											  [AlterTableFlagGranularRefreshStartOffset],
										  time_type) :
			parse_stored_granular_refresh_offset(settings.granular_refresh_start_offset, time_type);
	int64 end_offset =
		set_end_offset ?
			parse_granular_refresh_offset(with_clause_options
											  [AlterTableFlagGranularRefreshEndOffset],
										  time_type) :
			parse_stored_granular_refresh_offset(settings.granular_refresh_end_offset, time_type);

	/*
	 * The refresh window is [now() - start_offset, now() - end_offset), so the
	 * start offset must be the larger one, as in refresh policies.
	 */
	if (start_offset <= end_offset)
	{
		/* Report the offsets the window was built from, which for anything left
		 * out of the statement is the value already stored. */
		const char *start_text =
			set_start_offset ?
				TextDatumGetCString(
					with_clause_options[AlterTableFlagGranularRefreshStartOffset].parsed) :
				text_to_cstring(settings.granular_refresh_start_offset);
		const char *end_text =
			set_end_offset ?
				TextDatumGetCString(
					with_clause_options[AlterTableFlagGranularRefreshEndOffset].parsed) :
				text_to_cstring(settings.granular_refresh_end_offset);

		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid granular refresh window"),
				 errdetail("timescaledb.cagg_granular_refresh_start_offset (%s) must be greater "
						   "than "
						   "timescaledb.cagg_granular_refresh_end_offset (%s).",
						   start_text,
						   end_text)));
	}

	settings.hypertable_id = ht->fd.id;

	if (set_start_offset)
	{
		settings.granular_refresh_start_offset =
			DatumGetTextPCopy(with_clause_options[AlterTableFlagGranularRefreshStartOffset].parsed);
	}

	if (set_end_offset)
	{
		settings.granular_refresh_end_offset =
			DatumGetTextPCopy(with_clause_options[AlterTableFlagGranularRefreshEndOffset].parsed);
	}

	if (configured)
	{
		ts_hypertable_cagg_settings_update(&settings);
	}
	else
	{
		ts_hypertable_cagg_settings_insert(&settings);
	}
}

void
tsl_process_rename_cmd(Oid relid, Cache *hcache, const RenameStmt *stmt)
{
	if (stmt->renameType == OBJECT_COLUMN)
	{
		/*
		 * process_rename_column() always sets relid to the materialization
		 * hypertable before calling us, so the cache lookup always succeeds.
		 */
		Hypertable *ht = ts_hypertable_cache_get_entry(hcache, relid, CACHE_FLAG_MISSING_OK);

		if (ht)
		{
			ContinuousAgg *cagg = ts_continuous_agg_find_by_mat_hypertable_id(ht->fd.id, true);
			if (cagg)
			{
				cagg_rename_view_columns(cagg);
			}
		}

		if (ht && TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(ht))
		{
			tsl_process_compress_table_rename_column(ht, stmt);
		}
	}
}
