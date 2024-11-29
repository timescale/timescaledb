/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <compat/compat.h>
#include <postgres.h>
#include <executor/spi.h>
#include <fmgr.h>
#include <lib/stringinfo.h>
#include <scan_iterator.h>
#include <scanner.h>
#include <time_utils.h>
#include <utils/builtins.h>
#include <utils/date.h>
#include <utils/guc.h>
#include <utils/palloc.h>
#include <utils/rel.h>
#include <utils/relcache.h>
#include <utils/snapmgr.h>
#include <utils/timestamp.h>

#include "debug_assert.h"
#include "guc.h"
#include "materialize.h"
#include "ts_catalog/continuous_agg.h"
#include "ts_catalog/continuous_aggs_watermark.h"

#define CHUNKIDFROMRELID "chunk_id_from_relid"
#define CONTINUOUS_AGG_CHUNK_ID_COL_NAME "chunk_id"

typedef enum MaterializationPlanType
{
	PLAN_TYPE_SELECT,
	PLAN_TYPE_INSERT,
	PLAN_TYPE_DELETE,
	PLAN_TYPE_UPDATE,
	PLAN_TYPE_EXISTS,
	PLAN_TYPE_MERGE,
	PLAN_TYPE_MERGE_DELETE,
	_MAX_MATERIALIZATION_PLAN_TYPES
} MaterializationPlanType;

typedef struct MaterializationPlan
{
	const char *query;
	SPIPlanPtr plan;
	int nargs;
	Oid *argtypes;
	bool read_only;
} MaterializationPlan;

static SPIPlanPtr materialization_plans[_MAX_MATERIALIZATION_PLAN_TYPES + 1] = {
	[PLAN_TYPE_SELECT] = NULL,		[PLAN_TYPE_INSERT] = NULL, [PLAN_TYPE_DELETE] = NULL,
	[PLAN_TYPE_UPDATE] = NULL,		[PLAN_TYPE_EXISTS] = NULL, [PLAN_TYPE_MERGE] = NULL,
	[PLAN_TYPE_MERGE_DELETE] = NULL
};

static MaterializationPlan _materialization_plans[_MAX_MATERIALIZATION_PLAN_TYPES + 1] = {
	[PLAN_TYPE_SELECT] = { .query = NULL,
						   .plan = NULL,
						   .nargs = 0,
						   .argtypes = NULL,
						   .read_only = true },
	[PLAN_TYPE_INSERT] = { .query = "INSERT INTO %s.%s SELECT * FROM %s.%s AS I "
									"WHERE I.%s >= $1 AND I.%s < $2 %s;",
						   .plan = NULL,
						   .nargs = 2,
						   .argtypes = NULL,
						   .read_only = false },
	[PLAN_TYPE_DELETE] = { .query = NULL,
						   .plan = NULL,
						   .nargs = 0,
						   .argtypes = NULL,
						   .read_only = false },
	[PLAN_TYPE_UPDATE] = { .query = NULL,
						   .plan = NULL,
						   .nargs = 0,
						   .argtypes = NULL,
						   .read_only = false },
	[PLAN_TYPE_EXISTS] = { .query = NULL,
						   .plan = NULL,
						   .nargs = 0,
						   .argtypes = NULL,
						   .read_only = true },
	[PLAN_TYPE_MERGE] = { .query = NULL,
						  .plan = NULL,
						  .nargs = 0,
						  .argtypes = NULL,
						  .read_only = false },
	[PLAN_TYPE_MERGE_DELETE] = { .query = NULL,
								 .plan = NULL,
								 .nargs = 0,
								 .argtypes = NULL,
								 .read_only = false },
};

static void SetMaterializationPlanArgumentTypes(MaterializationPlanType plan_type, Oid *argtypes);
static MaterializationPlan *GetMaterializationPlan(MaterializationPlanType plan_type);
static void CreateMaterializationPlan(MaterializationPlanType plan_type, const char *query);
static int ExecuteMaterializationPlan(MaterializationPlanType plan_type, const char *query,
									  Datum *values, const char *nulls);

static bool ranges_overlap(InternalTimeRange invalidation_range,
						   InternalTimeRange new_materialization_range);
static TimeRange internal_time_range_to_time_range(InternalTimeRange internal);
static int64 range_length(const InternalTimeRange range);
static Datum internal_to_time_value_or_infinite(int64 internal, Oid time_type,
												bool *is_infinite_out);
static List *cagg_find_aggref_and_var_cols(ContinuousAgg *cagg, Hypertable *mat_ht);
static char *build_merge_insert_columns(List *strings, const char *separator, const char *prefix);
static char *build_merge_join_clause(List *column_names);
static char *build_merge_update_clause(List *column_names);

/***************************
 * materialization support *
 ***************************/

typedef struct MaterializationContext
{
	Hypertable *mat_ht;
	ContinuousAgg *cagg;
	SchemaAndName partial_view;
	SchemaAndName materialization_table;
	NameData *time_column_name;
	TimeRange invalidation_range;
	char *chunk_condition;
} MaterializationContext;

static void spi_reset_plan(MaterializationPlanType plan_type);
static void spi_prepare_plan(MaterializationPlanType plan_type, const char *query, int nargs,
							 Oid *argtypes);
static void UpdateWatermark(Hypertable *mat_ht, SchemaAndName materialization_table,
							const NameData *time_column_name, TimeRange materialization_range,
							const char *const chunk_condition);
static void UpdateMaterializations(Hypertable *mat_ht, const ContinuousAgg *cagg,
								   SchemaAndName partial_view, SchemaAndName materialization_table,
								   const NameData *time_column_name, TimeRange invalidation_range,
								   const int32 chunk_id);
static uint64 DeleteMaterializations(SchemaAndName materialization_table,
									 const NameData *time_column_name,
									 TimeRange materialization_range,
									 const char *const chunk_condition);
static uint64
InsertMaterializations(Hypertable *mat_ht, const ContinuousAgg *cagg, SchemaAndName partial_view,
					   SchemaAndName materialization_table, const NameData *time_column_name,
					   TimeRange materialization_range, const char *const chunk_condition);
static bool ExistsMaterializations(SchemaAndName materialization_table,
								   const NameData *time_column_name,
								   TimeRange materialization_range);
static uint64 MergeMaterializations(Hypertable *mat_ht, const ContinuousAgg *cagg,
									SchemaAndName partial_view, SchemaAndName materialization_table,
									const NameData *time_column_name,
									TimeRange materialization_range);

void
continuous_agg_update_materialization(Hypertable *mat_ht, const ContinuousAgg *cagg,
									  SchemaAndName partial_view,
									  SchemaAndName materialization_table,
									  const NameData *time_column_name,
									  InternalTimeRange new_materialization_range,
									  InternalTimeRange invalidation_range, int32 chunk_id)
{
	InternalTimeRange combined_materialization_range = new_materialization_range;
	bool materialize_invalidations_separately = range_length(invalidation_range) > 0;

	/* Lock down search_path */
	int save_nestlevel = NewGUCNestLevel();
	RestrictSearchPath();

	/* pin the start of new_materialization to the end of new_materialization,
	 * we are not allowed to materialize beyond that point
	 */
	if (new_materialization_range.start > new_materialization_range.end)
		new_materialization_range.start = new_materialization_range.end;

	if (range_length(invalidation_range) > 0)
	{
		Assert(invalidation_range.start <= invalidation_range.end);

		/* we never materialize beyond the new materialization range */
		if (invalidation_range.start >= new_materialization_range.end ||
			invalidation_range.end > new_materialization_range.end)
			elog(ERROR, "internal error: invalidation range ahead of new materialization range");

		/* If the invalidation and new materialization ranges overlap, materialize in one go */
		materialize_invalidations_separately =
			!ranges_overlap(invalidation_range, new_materialization_range);

		combined_materialization_range.start =
			int64_min(invalidation_range.start, new_materialization_range.start);
	}

	/* Then insert the materializations.
	 * We insert them in two groups:
	 * [lowest_invalidated, greatest_invalidated] and
	 * [start_of_new_materialization, end_of_new_materialization]
	 * eventually, we may want more precise deletions and insertions for the invalidated ranges.
	 * if greatest_invalidated == end_of_new_materialization then we only perform 1 insertion.
	 * to prevent values from being inserted multiple times.
	 */
	if (range_length(invalidation_range) == 0 || !materialize_invalidations_separately)
	{
		UpdateMaterializations(mat_ht,
							   cagg,
							   partial_view,
							   materialization_table,
							   time_column_name,
							   internal_time_range_to_time_range(combined_materialization_range),
							   chunk_id);
	}
	else
	{
		UpdateMaterializations(mat_ht,
							   cagg,
							   partial_view,
							   materialization_table,
							   time_column_name,
							   internal_time_range_to_time_range(invalidation_range),
							   chunk_id);

		UpdateMaterializations(mat_ht,
							   cagg,
							   partial_view,
							   materialization_table,
							   time_column_name,
							   internal_time_range_to_time_range(new_materialization_range),
							   chunk_id);
	}

	/* Restore search_path */
	AtEOXact_GUC(false, save_nestlevel);
}

static bool
ranges_overlap(InternalTimeRange invalidation_range, InternalTimeRange new_materialization_range)
{
	Assert(invalidation_range.start <= invalidation_range.end);
	Assert(new_materialization_range.start <= new_materialization_range.end);
	return !(invalidation_range.end < new_materialization_range.start ||
			 new_materialization_range.end < invalidation_range.start);
}

static int64
range_length(const InternalTimeRange range)
{
	Assert(range.end >= range.start);

	return int64_saturating_sub(range.end, range.start);
}

static Datum
time_range_internal_to_min_time_value(Oid type)
{
	switch (type)
	{
		case TIMESTAMPOID:
			return TimestampGetDatum(DT_NOBEGIN);
		case TIMESTAMPTZOID:
			return TimestampTzGetDatum(DT_NOBEGIN);
		case DATEOID:
			return DateADTGetDatum(DATEVAL_NOBEGIN);
		default:
			return ts_internal_to_time_value(PG_INT64_MIN, type);
	}
}

static Datum
time_range_internal_to_max_time_value(Oid type)
{
	switch (type)
	{
		case TIMESTAMPOID:
			return TimestampGetDatum(DT_NOEND);
		case TIMESTAMPTZOID:
			return TimestampTzGetDatum(DT_NOEND);
		case DATEOID:
			return DateADTGetDatum(DATEVAL_NOEND);
			break;
		default:
			return ts_internal_to_time_value(PG_INT64_MAX, type);
	}
}

static Datum
internal_to_time_value_or_infinite(int64 internal, Oid time_type, bool *is_infinite_out)
{
	/* MIN and MAX can occur due to NULL thresholds, or due to a lack of invalidations. Since our
	 * regular conversion function errors in those cases, and we want to use those as markers for an
	 * open threshold in one direction, we special case this here*/
	if (internal == PG_INT64_MIN)
	{
		if (is_infinite_out != NULL)
			*is_infinite_out = true;
		return time_range_internal_to_min_time_value(time_type);
	}
	else if (internal == PG_INT64_MAX)
	{
		if (is_infinite_out != NULL)
			*is_infinite_out = true;
		return time_range_internal_to_max_time_value(time_type);
	}
	else
	{
		if (is_infinite_out != NULL)
			*is_infinite_out = false;
		return ts_internal_to_time_value(internal, time_type);
	}
}

static TimeRange
internal_time_range_to_time_range(InternalTimeRange internal)
{
	TimeRange range;
	range.type = internal.type;

	range.start = internal_to_time_value_or_infinite(internal.start, internal.type, NULL);
	range.end = internal_to_time_value_or_infinite(internal.end, internal.type, NULL);

	return range;
}

static List *
cagg_find_aggref_and_var_cols(ContinuousAgg *cagg, Hypertable *mat_ht)
{
	List *retlist = NIL;
	ListCell *lc;
	Query *cagg_view_query = ts_continuous_agg_get_query(cagg);

	foreach (lc, cagg_view_query->targetList)
	{
		TargetEntry *tle = castNode(TargetEntry, lfirst(lc));

		if (!tle->resjunk && (tle->ressortgroupref == 0 ||
							  get_sortgroupref_clause_noerr(tle->ressortgroupref,
															cagg_view_query->groupClause) == NULL))
			retlist = lappend(retlist, get_attname(mat_ht->main_table_relid, tle->resno, false));
	}

	return retlist;
}

static char *
build_merge_insert_columns(List *strings, const char *separator, const char *prefix)
{
	StringInfo ret = makeStringInfo();

	Assert(strings != NIL);

	ListCell *lc;
	foreach (lc, strings)
	{
		char *grpcol = (char *) lfirst(lc);
		if (ret->len > 0)
			appendStringInfoString(ret, separator);

		if (prefix)
			appendStringInfoString(ret, prefix);
		appendStringInfoString(ret, quote_identifier(grpcol));
	}

	elog(DEBUG2, "%s: %s", __func__, ret->data);
	return ret->data;
}

static char *
build_merge_join_clause(List *column_names)
{
	StringInfo ret = makeStringInfo();

	Assert(column_names != NIL);

	ListCell *lc;
	foreach (lc, column_names)
	{
		char *column = (char *) lfirst(lc);

		if (ret->len > 0)
			appendStringInfoString(ret, " AND ");

		appendStringInfoString(ret, "P.");
		appendStringInfoString(ret, quote_identifier(column));
		appendStringInfoString(ret, " = M.");
		appendStringInfoString(ret, quote_identifier(column));
	}

	elog(DEBUG2, "%s: %s", __func__, ret->data);
	return ret->data;
}

static char *
build_merge_update_clause(List *column_names)
{
	StringInfo ret = makeStringInfo();

	Assert(column_names != NIL);

	ListCell *lc;
	foreach (lc, column_names)
	{
		char *column = (char *) lfirst(lc);

		if (ret->len > 0)
			appendStringInfoString(ret, ", ");

		appendStringInfoString(ret, quote_identifier(column));
		appendStringInfoString(ret, " = P.");
		appendStringInfoString(ret, quote_identifier(column));
	}

	elog(DEBUG2, "%s: %s", __func__, ret->data);
	return ret->data;
}

static void
spi_reset_plan(MaterializationPlanType plan_type)
{
	if (materialization_plans[plan_type] != NULL)
	{
		SPI_freeplan(materialization_plans[plan_type]);
		materialization_plans[plan_type] = NULL;
	}
}

static void
ResetMaterializationPlan(MaterializationPlanType plan_type)
{
	if (_materialization_plans[plan_type].plan != NULL)
	{
		SPI_freeplan(_materialization_plans[plan_type].plan);
		_materialization_plans[plan_type].plan = NULL;
	}
}

static void
spi_prepare_plan(MaterializationPlanType plan_type, const char *query, int nargs, Oid *argtypes)
{
	if (materialization_plans[plan_type] == NULL)
	{
		SPIPlanPtr plan = SPI_prepare(query, nargs, argtypes);
		if (plan == NULL)
			elog(ERROR, "SPI_prepare failed: %s", query);

		materialization_plans[plan_type] = plan;
	}

	SPI_keepplan(materialization_plans[plan_type]);
}

static void
SetMaterializationPlanArgumentTypes(MaterializationPlanType plan_type, Oid *argtypes)
{
	if (_materialization_plans[plan_type].plan == NULL)
	{
		_materialization_plans[plan_type].argtypes = argtypes;
	}
}

static MaterializationPlan *
GetMaterializationPlan(MaterializationPlanType plan_type)
{
	return &_materialization_plans[plan_type];
}

static void
CreateMaterializationPlan(MaterializationPlanType plan_type, const char *query)
{
	MaterializationPlan *materialization = GetMaterializationPlan(plan_type);

	if (materialization->plan == NULL)
	{
		SPIPlanPtr plan =
			SPI_prepare(query, materialization->nargs, materialization->argtypes);
		if (plan == NULL)
			elog(ERROR, "SPI_prepare failed: %s", query);

		materialization->plan = plan;
		SPI_keepplan(materialization->plan);
	}
}

static int
ExecuteMaterializationPlan(MaterializationPlanType plan_type, const char *query, Datum *values,
						   const char *nulls)
{
	MaterializationPlan *materialization = GetMaterializationPlan(plan_type);

	if (materialization->plan == NULL)
	{
		CreateMaterializationPlan(plan_type, query);
	}

	int res = SPI_execute_plan(materialization->plan, values, nulls, materialization->read_only, 0);

	if (res < 0)
		elog(ERROR, "SPI_execute_plan failed: %s", query);

	return res;
}

static void
UpdateWatermark(Hypertable *mat_ht, SchemaAndName materialization_table,
				const NameData *time_column_name, TimeRange materialization_range,
				const char *const chunk_condition)
{
	int res;
	StringInfo command = makeStringInfo();
	Oid types[] = { materialization_range.type };
	Datum values[] = { materialization_range.start };
	char nulls[] = { false };

	appendStringInfo(command,
					 "SELECT %s FROM %s.%s AS I "
					 "WHERE I.%s >= $1 %s "
					 "ORDER BY 1 DESC LIMIT 1;",
					 quote_identifier(NameStr(*time_column_name)),
					 quote_identifier(NameStr(*materialization_table.schema)),
					 quote_identifier(NameStr(*materialization_table.name)),
					 quote_identifier(NameStr(*time_column_name)),
					 chunk_condition);

	elog(DEBUG2, "%s: %s", __func__, command->data);
	res = SPI_execute_with_args(command->data,
								1,
								types,
								values,
								nulls,
								false /* read_only */,
								0 /* count */);

	if (res < 0)
		elog(ERROR, "could not get the last bucket of the materialized data");

	Ensure(SPI_gettypeid(SPI_tuptable->tupdesc, 1) == materialization_range.type,
		   "partition types for result (%d) and dimension (%d) do not match",
		   SPI_gettypeid(SPI_tuptable->tupdesc, 1),
		   materialization_range.type);

	if (SPI_processed > 0)
	{
		bool isnull;
		Datum maxdat = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);

		if (!isnull)
		{
			int64 watermark = ts_time_value_to_internal(maxdat, materialization_range.type);
			ts_cagg_watermark_update(mat_ht, watermark, isnull, false);
		}
	}
}

static void
UpdateMaterializations(Hypertable *mat_ht, const ContinuousAgg *cagg, SchemaAndName partial_view,
					   SchemaAndName materialization_table, const NameData *time_column_name,
					   TimeRange invalidation_range, const int32 chunk_id)
{
	StringInfo chunk_condition = makeStringInfo();
	uint64 rows_processed = 0;

	/* MERGE statement is available starting on PG15 and we'll support it only in the new format of
	 * CAggs and for non-compressed hypertables */
	if (ts_guc_enable_merge_on_cagg_refresh && PG_VERSION_NUM >= 150000 &&
		ContinuousAggIsFinalized(cagg) && !TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(mat_ht))
	{
		rows_processed = MergeMaterializations(mat_ht,
											   cagg,
											   partial_view,
											   materialization_table,
											   time_column_name,
											   invalidation_range);
	}
	else
	{
		/*
		 * chunk_id is valid if the materializaion update should be done only on the given chunk.
		 * This is used currently for refresh on chunk drop only. In other cases, manual
		 * call to refresh_continuous_aggregate or call from a refresh policy, chunk_id is
		 * not provided, i.e., invalid.
		 */
		if (chunk_id != INVALID_CHUNK_ID)
			appendStringInfo(chunk_condition, "AND chunk_id = %d", chunk_id);

		rows_processed += DeleteMaterializations(materialization_table,
												 time_column_name,
												 invalidation_range,
												 chunk_condition->data);
		rows_processed += InsertMaterializations(mat_ht,
												 cagg,
												 partial_view,
												 materialization_table,
												 time_column_name,
												 invalidation_range,
												 chunk_condition->data);
	}

	/* Get the max(time_dimension) of the materialized data */
	if (rows_processed > 0)
	{
		UpdateWatermark(mat_ht,
						materialization_table,
						time_column_name,
						invalidation_range,
						chunk_condition->data);
	}
}

static bool
ExistsMaterializations(SchemaAndName materialization_table, const NameData *time_column_name,
					   TimeRange materialization_range)
{
	int res;
	StringInfo command = makeStringInfo();
	Oid types[] = { materialization_range.type, materialization_range.type };
	Datum values[] = { materialization_range.start, materialization_range.end };
	char nulls[] = { false, false };

	appendStringInfo(command,
					 "SELECT 1 FROM %s.%s AS M "
					 "WHERE M.%s >= $1 AND M.%s < $2 "
					 "LIMIT 1;",
					 quote_identifier(NameStr(*materialization_table.schema)),
					 quote_identifier(NameStr(*materialization_table.name)),
					 quote_identifier(NameStr(*time_column_name)),
					 quote_identifier(NameStr(*time_column_name)));

	elog(DEBUG2, "%s", command->data);
	spi_prepare_plan(PLAN_TYPE_EXISTS, command->data, 2, types);
	res = SPI_execute_plan(materialization_plans[PLAN_TYPE_EXISTS],
						   values,
						   nulls,
						   true /* read_only */,
						   0 /* count */);

	if (res < 0)
		elog(ERROR,
			 "could not check the materialization table \"%s.%s\"",
			 NameStr(*materialization_table.schema),
			 NameStr(*materialization_table.name));

	spi_reset_plan(PLAN_TYPE_EXISTS);

	return (SPI_processed > 0);
}

static uint64
MergeMaterializations(Hypertable *mat_ht, const ContinuousAgg *cagg, SchemaAndName partial_view,
					  SchemaAndName materialization_table, const NameData *time_column_name,
					  TimeRange materialization_range)
{
	int res;
	StringInfo command = makeStringInfo();
	Oid types[] = { materialization_range.type, materialization_range.type };
	Datum values[] = { materialization_range.start, materialization_range.end };
	char nulls[] = { false, false };

	/* Fallback to INSERT materializations if there's no rows to change on it */
	if (!ExistsMaterializations(materialization_table, time_column_name, materialization_range))
	{
		elog(DEBUG2,
			 "no rows to update on materialization table \"%s.%s\", falling back to INSERT",
			 NameStr(*materialization_table.schema),
			 NameStr(*materialization_table.name));
		return InsertMaterializations(mat_ht,
									  cagg,
									  partial_view,
									  materialization_table,
									  time_column_name,
									  materialization_range,
									  "" /* empty chunk condition for Finalized CAggs */);
	}

	List *grp_colnames = cagg_find_groupingcols((ContinuousAgg *) cagg, mat_ht);
	List *agg_colnames = cagg_find_aggref_and_var_cols((ContinuousAgg *) cagg, mat_ht);
	List *all_columns = NIL;
	uint64 rows_processed = 0;

	/* Concat both lists into a single one*/
	all_columns = list_concat(all_columns, grp_colnames);
	all_columns = list_concat(all_columns, agg_colnames);

	StringInfo merge_update = makeStringInfo();
	char *merge_update_clause = build_merge_update_clause(all_columns);

	/* It make no sense but is possible to create a cagg only with time bucket (without aggregate
	 * functions) */
	if (merge_update_clause != NULL)
	{
		appendStringInfo(merge_update,
						 "  WHEN MATCHED AND ROW(M.*) IS DISTINCT FROM ROW(P.*) THEN "
						 "    UPDATE SET %s ",
						 merge_update_clause);
	}

	/* MERGE statement to UPDATE affected buckets and INSERT new ones */
	appendStringInfo(command,
					 "WITH partial AS ( "
					 "  SELECT * "
					 "  FROM %s.%s "
					 "  WHERE %s >= $1 AND %s < $2 "
					 ") "
					 "MERGE INTO %s.%s M "
					 "USING partial P ON %s AND M.%s >= $1 AND M.%s < $2 "
					 "  %s " /* UPDATE */
					 "  WHEN NOT MATCHED THEN "
					 "    INSERT (%s) VALUES (%s) ",

					 /* partial VIEW */
					 quote_identifier(NameStr(*partial_view.schema)),
					 quote_identifier(NameStr(*partial_view.name)),

					 /* partial WHERE */
					 quote_identifier(NameStr(*time_column_name)),
					 quote_identifier(NameStr(*time_column_name)),

					 /* materialization hypertable */
					 quote_identifier(NameStr(*materialization_table.schema)),
					 quote_identifier(NameStr(*materialization_table.name)),

					 /* MERGE JOIN condition */
					 build_merge_join_clause(grp_colnames),

					 /* extra MERGE JOIN condition with primary dimension */
					 quote_identifier(NameStr(*time_column_name)),
					 quote_identifier(NameStr(*time_column_name)),

					 /* UPDATE */
					 merge_update->data,

					 /* INSERT */
					 build_merge_insert_columns(all_columns, ", ", NULL),
					 build_merge_insert_columns(all_columns, ", ", "P."));

	elog(DEBUG2, "%s: %s", __func__, command->data);
	spi_prepare_plan(PLAN_TYPE_MERGE, command->data, 2, types);
	res = SPI_execute_plan(materialization_plans[PLAN_TYPE_MERGE],
						   values,
						   nulls,
						   false /* read_only */,
						   0 /* count */);

	if (res < 0)
		elog(ERROR,
			 "could not materialize values into the materialization table \"%s.%s\"",
			 NameStr(*materialization_table.schema),
			 NameStr(*materialization_table.name));
	else
		elog(LOG,
			 "merged " UINT64_FORMAT " row(s) into materialization table \"%s.%s\"",
			 SPI_processed,
			 NameStr(*materialization_table.schema),
			 NameStr(*materialization_table.name));

	rows_processed += SPI_processed;

	spi_reset_plan(PLAN_TYPE_MERGE);

	/* DELETE rows from the materialization hypertable when necessary */
	resetStringInfo(command);
	appendStringInfo(command,
					 "DELETE "
					 "FROM %s.%s M "
					 "WHERE M.%s >= $1 AND M.%s < $2 "
					 "AND NOT EXISTS ("
					 " SELECT FROM %s.%s P "
					 " WHERE %s AND P.%s >= $1 AND P.%s < $2) ",

					 /* materialization hypertable */
					 quote_identifier(NameStr(*materialization_table.schema)),
					 quote_identifier(NameStr(*materialization_table.name)),

					 /* materialization hypertable WHERE */
					 quote_identifier(NameStr(*time_column_name)),
					 quote_identifier(NameStr(*time_column_name)),

					 /* partial VIEW */
					 quote_identifier(NameStr(*partial_view.schema)),
					 quote_identifier(NameStr(*partial_view.name)),

					 /* MERGE JOIN condition */
					 build_merge_join_clause(grp_colnames),

					 /* partial WHERE */
					 quote_identifier(NameStr(*time_column_name)),
					 quote_identifier(NameStr(*time_column_name)));

	elog(DEBUG2, "%s: %s", __func__, command->data);
	spi_prepare_plan(PLAN_TYPE_MERGE_DELETE, command->data, 2, types);
	res = SPI_execute_plan(materialization_plans[PLAN_TYPE_MERGE_DELETE],
						   values,
						   nulls,
						   false /* read_only */,
						   0 /* count */);

	if (res < 0)
		elog(ERROR,
			 "could not delete values from the materialization table \"%s.%s\"",
			 NameStr(*materialization_table.schema),
			 NameStr(*materialization_table.name));
	else
		elog(LOG,
			 "deleted " UINT64_FORMAT " row(s) from materialization table \"%s.%s\"",
			 SPI_processed,
			 NameStr(*materialization_table.schema),
			 NameStr(*materialization_table.name));

	rows_processed += SPI_processed;

	spi_reset_plan(PLAN_TYPE_MERGE_DELETE);

	return rows_processed;
}

static uint64
DeleteMaterializations(SchemaAndName materialization_table, const NameData *time_column_name,
					   TimeRange materialization_range, const char *const chunk_condition)
{
	int res;
	StringInfo command = makeStringInfo();
	Oid types[] = { materialization_range.type, materialization_range.type };
	Datum values[] = { materialization_range.start, materialization_range.end };
	char nulls[] = { false, false };

	appendStringInfo(command,
					 "DELETE FROM %s.%s AS D WHERE "
					 "D.%s >= $1 AND D.%s < $2 %s;",
					 quote_identifier(NameStr(*materialization_table.schema)),
					 quote_identifier(NameStr(*materialization_table.name)),
					 quote_identifier(NameStr(*time_column_name)),
					 quote_identifier(NameStr(*time_column_name)),
					 chunk_condition);

	elog(DEBUG2, "%s: %s", __func__, command->data);
	spi_prepare_plan(PLAN_TYPE_DELETE, command->data, 2, types);
	res = SPI_execute_plan(materialization_plans[PLAN_TYPE_DELETE],
						   values,
						   nulls,
						   false /* read_only */,
						   0 /* count */);

	if (res < 0)
		elog(ERROR,
			 "could not delete old values from materialization table \"%s.%s\"",
			 NameStr(*materialization_table.schema),
			 NameStr(*materialization_table.name));
	else
		elog(LOG,
			 "deleted " UINT64_FORMAT " row(s) from materialization table \"%s.%s\"",
			 SPI_processed,
			 NameStr(*materialization_table.schema),
			 NameStr(*materialization_table.name));

	spi_reset_plan(PLAN_TYPE_DELETE);

	return SPI_processed;
}

static uint64
InsertMaterializations(Hypertable *mat_ht, const ContinuousAgg *cagg, SchemaAndName partial_view,
					   SchemaAndName materialization_table, const NameData *time_column_name,
					   TimeRange materialization_range, const char *const chunk_condition)
{
	int res;
	StringInfo command = makeStringInfo();
	Oid types[] = { materialization_range.type, materialization_range.type };
	Datum values[] = { materialization_range.start, materialization_range.end };
	char nulls[] = { false, false };

	SetMaterializationPlanArgumentTypes(PLAN_TYPE_INSERT, types);

	appendStringInfo(command,
					 _materialization_plans[PLAN_TYPE_INSERT].query,
					 quote_identifier(NameStr(*materialization_table.schema)),
					 quote_identifier(NameStr(*materialization_table.name)),
					 quote_identifier(NameStr(*partial_view.schema)),
					 quote_identifier(NameStr(*partial_view.name)),
					 quote_identifier(NameStr(*time_column_name)),
					 quote_identifier(NameStr(*time_column_name)),
					 chunk_condition);

	elog(DEBUG2, "%s: %s", __func__, command->data);
	res = ExecuteMaterializationPlan(PLAN_TYPE_INSERT, command->data, values, nulls);

	if (res < 0)
		elog(ERROR,
			 "could not materialize values into the materialization table \"%s.%s\"",
			 NameStr(*materialization_table.schema),
			 NameStr(*materialization_table.name));
	else
		elog(LOG,
			 "inserted " UINT64_FORMAT " row(s) into materialization table \"%s.%s\"",
			 SPI_processed,
			 NameStr(*materialization_table.schema),
			 NameStr(*materialization_table.name));

	ResetMaterializationPlan(PLAN_TYPE_INSERT);

	return SPI_processed;
}
