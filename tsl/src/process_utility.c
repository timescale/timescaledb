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
#include <storage/lockdefs.h>
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
 * ALTER TABLE <hypertable> SET (timescaledb.granular_refresh_column = ...,
 *                               timescaledb.granular_refresh_start_offset = ...,
 *                               timescaledb.granular_refresh_end_offset = ...)
 *
 * Enables granular refresh of continuous aggregates on the raw hypertable.
 * Continuous aggregates opt in separately and share these settings.
 *
 * All three options are required to enable it. Afterwards the offsets can be
 * changed, individually or together. The column cannot be changed, and nothing can be
 * cleared.
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
	FormData_hypertable_cagg_settings settings = { 0 };
	bool configured = ts_hypertable_cagg_settings_get(ht->fd.id, &settings);

	Dimension *dim = ts_hyperspace_get_mutable_dimension(ht->space, DIMENSION_TYPE_OPEN, 0);
	Ensure(dim, "hypertable without open dimension");
	Oid time_type = get_atttype(dim->main_table_relid, dim->column_attno);

	/* Enabling needs the whole configuration. Once it is stored, anything left
	 * out of the statement keeps the value it already has. */
	if (!configured && (!set_column || !set_start_offset || !set_end_offset))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("incomplete granular refresh configuration"),
				 errhint("timescaledb.granular_refresh_column, "
						 "timescaledb.granular_refresh_start_offset and "
						 "timescaledb.granular_refresh_end_offset must all be set to enable "
						 "granular refresh.")));
	}

	if (set_column)
	{
		char *colname =
			TextDatumGetCString(with_clause_options[AlterTableFlagGranularRefreshColumn].parsed);

		/* Tracking rows already hold tenant ids read from the configured column,
		 * and the refresh filters on that column, so the two cannot diverge.
		 * Naming the column it already has is accepted and changes nothing,
		 * whether or not the statement carries offsets as well. */
		if (configured && strcmp(colname, NameStr(settings.granular_refresh_column)) != 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot change the granular refresh column of hypertable \"%s\"",
							NameStr(ht->fd.table_name)),
					 errdetail("The column is currently \"%s\".",
							   NameStr(settings.granular_refresh_column)),
					 errhint("Only timescaledb.granular_refresh_start_offset and "
							 "timescaledb.granular_refresh_end_offset can be changed.")));
		}

		if (colname[0] == '\0')
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("granular refresh column cannot be empty"),
					 errhint(
						 "timescaledb.granular_refresh_column must reference a valid column.")));
		}

		AttrNumber attno = get_attnum(ht->main_table_relid, colname);

		if (attno < 1)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" does not exist", colname),
					 errhint("The timescaledb.granular_refresh_column option must reference a "
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
					 errhint("timescaledb.granular_refresh_column must be a date, integer, UUID, "
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
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid granular refresh window"),
				 errdetail("timescaledb.granular_refresh_start_offset must be greater than "
						   "timescaledb.granular_refresh_end_offset.")));
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
