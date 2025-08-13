/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>

#include <executor/spi.h>
#include <fmgr.h>
#include <lib/stringinfo.h>
#include <utils/builtins.h>
#include <utils/date.h>
#include <utils/guc.h>
#include <utils/palloc.h>
#include <utils/rel.h>
#include <utils/relcache.h>
#include <utils/snapmgr.h>
#include <utils/timestamp.h>

#include "compat/compat.h"
#include "debug_assert.h"
#include "guc.h"
#include "materialize.h"
#include "scan_iterator.h"
#include "scanner.h"
#include "time_utils.h"
#include "ts_catalog/continuous_agg.h"
#include "ts_catalog/continuous_aggs_watermark.h"

#define CONTINUOUS_AGG_CHUNK_ID_COL_NAME "chunk_id"

/*********************
 * utility functions *
 *********************/

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
typedef enum MaterializationPlanType
{
	PLAN_TYPE_INSERT,
	PLAN_TYPE_DELETE,
	PLAN_TYPE_EXISTS,
	PLAN_TYPE_MERGE,
	PLAN_TYPE_MERGE_DELETE,
	_MAX_MATERIALIZATION_PLAN_TYPES
} MaterializationPlanType;

typedef struct MaterializationContext
{
	Hypertable *mat_ht;
	const ContinuousAgg *cagg;
	SchemaAndName partial_view;
	SchemaAndName materialization_table;
	NameData *time_column_name;
	TimeRange materialization_range;
	char *chunk_condition;
} MaterializationContext;

typedef char *(*MaterializationCreateStatement)(MaterializationContext *context);
typedef void (*MaterializationEmitError)(MaterializationContext *context);
typedef void (*MaterializationEmitProgress)(MaterializationContext *context, uint64 rows_processed);

typedef struct MaterializationPlan
{
	SPIPlanPtr plan;
	bool read_only;
	int nargs;
	MaterializationCreateStatement create_statement;
	MaterializationEmitError emit_error;
	MaterializationEmitProgress emit_progress;
} MaterializationPlan;

static char *create_materialization_insert_statement(MaterializationContext *context);
static char *create_materialization_delete_statement(MaterializationContext *context);
static char *create_materialization_exists_statement(MaterializationContext *context);
static char *create_materialization_merge_statement(MaterializationContext *context);
static char *create_materialization_merge_delete_statement(MaterializationContext *context);

static void emit_materialization_insert_error(MaterializationContext *context);
static void emit_materialization_delete_error(MaterializationContext *context);
static void emit_materialization_exists_error(MaterializationContext *context);
static void emit_materialization_merge_error(MaterializationContext *context);

static void emit_materialization_insert_progress(MaterializationContext *context,
												 uint64 rows_processed);
static void emit_materialization_delete_progress(MaterializationContext *context,
												 uint64 rows_processed);
static void emit_materialization_merge_progress(MaterializationContext *context,
												uint64 rows_processed);

static MaterializationPlan materialization_plans[_MAX_MATERIALIZATION_PLAN_TYPES + 1] = {
	[PLAN_TYPE_INSERT] = { .nargs = 2,
						   .create_statement = create_materialization_insert_statement,
						   .emit_error = emit_materialization_insert_error,
						   .emit_progress = emit_materialization_insert_progress },
	[PLAN_TYPE_DELETE] = { .nargs = 2,
						   .create_statement = create_materialization_delete_statement,
						   .emit_error = emit_materialization_delete_error,
						   .emit_progress = emit_materialization_delete_progress },
	[PLAN_TYPE_EXISTS] = { .read_only = true,
						   .nargs = 2,
						   .create_statement = create_materialization_exists_statement,
						   .emit_error = emit_materialization_exists_error },
	[PLAN_TYPE_MERGE] = { .nargs = 2,
						  .create_statement = create_materialization_merge_statement,
						  .emit_error = emit_materialization_merge_error,
						  .emit_progress = emit_materialization_merge_progress },
	[PLAN_TYPE_MERGE_DELETE] = { .nargs = 2,
								 .create_statement = create_materialization_merge_delete_statement,
								 .emit_error = emit_materialization_delete_error,
								 .emit_progress = emit_materialization_delete_progress },
};

static Oid *create_materialization_plan_argtypes(MaterializationContext *context,
												 MaterializationPlanType plan_type, int nargs);
static MaterializationPlan *create_materialization_plan(MaterializationContext *context,
														MaterializationPlanType plan_type);
static void create_materialization_plan_args(MaterializationContext *context,
											 MaterializationPlanType plan_type, int nargs,
											 Datum **values, char **nulls);
static uint64 execute_materialization_plan(MaterializationContext *context,
										   MaterializationPlanType plan_type);
static void free_materialization_plan(MaterializationContext *context,
									  MaterializationPlanType plan_type);
static void free_materialization_plans(MaterializationContext *context);

static void update_watermark(MaterializationContext *context);
static void execute_materializations(MaterializationContext *context);

/* API to update materializations from refresh code */
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

	MaterializationContext context = {
		.mat_ht = mat_ht,
		.cagg = cagg,
		.partial_view = partial_view,
		.materialization_table = materialization_table,
		.time_column_name = (NameData *) time_column_name,
		.materialization_range = internal_time_range_to_time_range(new_materialization_range),
		/*
		 * chunk_id is valid if the materializaion update should be done only on the given chunk.
		 * This is used currently for refresh on chunk drop only. In other cases, manual
		 * call to refresh_continuous_aggregate or call from a refresh policy, chunk_id is
		 * not provided, i.e., invalid. Also the chunk_id is used only on the old format.
		 */
		.chunk_condition =
			chunk_id != INVALID_CHUNK_ID && !ContinuousAggIsFinalized(cagg) ?
				psprintf(" AND %s = %d", CONTINUOUS_AGG_CHUNK_ID_COL_NAME, chunk_id) :
				"",
	};

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
		context.materialization_range =
			internal_time_range_to_time_range(combined_materialization_range);
		execute_materializations(&context);
	}
	else
	{
		context.materialization_range = internal_time_range_to_time_range(invalidation_range);
		execute_materializations(&context);

		context.materialization_range =
			internal_time_range_to_time_range(new_materialization_range);
		execute_materializations(&context);
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
		appendStringInfoString(ret, " IS NOT DISTINCT FROM M.");
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

/* Create INSERT statement */
static char *
create_materialization_insert_statement(MaterializationContext *context)
{
	StringInfoData query;
	initStringInfo(&query);
	appendStringInfo(&query,
					 "INSERT INTO %s.%s SELECT * FROM %s.%s AS I "
					 "WHERE I.%s >= $1 AND I.%s < $2 %s;",
					 quote_identifier(NameStr(*context->materialization_table.schema)),
					 quote_identifier(NameStr(*context->materialization_table.name)),
					 quote_identifier(NameStr(*context->partial_view.schema)),
					 quote_identifier(NameStr(*context->partial_view.name)),
					 quote_identifier(NameStr(*context->time_column_name)),
					 quote_identifier(NameStr(*context->time_column_name)),
					 context->chunk_condition);
	return query.data;
}

/* Create DELETE statement */
static char *
create_materialization_delete_statement(MaterializationContext *context)
{
	StringInfoData query;
	initStringInfo(&query);
	appendStringInfo(&query,
					 "DELETE FROM %s.%s AS D "
					 "WHERE D.%s >= $1 AND D.%s < $2 %s;",
					 quote_identifier(NameStr(*context->materialization_table.schema)),
					 quote_identifier(NameStr(*context->materialization_table.name)),
					 quote_identifier(NameStr(*context->time_column_name)),
					 quote_identifier(NameStr(*context->time_column_name)),
					 context->chunk_condition);
	return query.data;
}

/* Create SELECT EXISTS statement */
static char *
create_materialization_exists_statement(MaterializationContext *context)
{
	StringInfoData query;
	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT 1 FROM %s.%s AS M "
					 "WHERE M.%s >= $1 AND M.%s < $2 "
					 "LIMIT 1;",
					 quote_identifier(NameStr(*context->materialization_table.schema)),
					 quote_identifier(NameStr(*context->materialization_table.name)),
					 quote_identifier(NameStr(*context->time_column_name)),
					 quote_identifier(NameStr(*context->time_column_name)));
	return query.data;
}

/* Create MERGE statement */
static char *
create_materialization_merge_statement(MaterializationContext *context)
{
	List *grp_colnames = cagg_find_groupingcols((ContinuousAgg *) context->cagg, context->mat_ht);
	List *agg_colnames =
		cagg_find_aggref_and_var_cols((ContinuousAgg *) context->cagg, context->mat_ht);
	List *all_columns = NIL;

	/* Concat both lists into a single one*/
	all_columns = list_concat(all_columns, grp_colnames);
	all_columns = list_concat(all_columns, agg_colnames);

	StringInfoData merge_update;
	initStringInfo(&merge_update);
	char *merge_update_clause = build_merge_update_clause(all_columns);

	/* It make no sense but is possible to create a cagg only with time bucket (without
	 * aggregate functions) */
	if (merge_update_clause != NULL)
	{
		appendStringInfo(&merge_update,
						 "  WHEN MATCHED AND ROW(M.*) IS DISTINCT FROM ROW(P.*) THEN "
						 "    UPDATE SET %s ",
						 merge_update_clause);
	}

	StringInfoData query;
	initStringInfo(&query);

	/* MERGE statement to UPDATE affected buckets and INSERT new ones */
	appendStringInfo(&query,
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
					 quote_identifier(NameStr(*context->partial_view.schema)),
					 quote_identifier(NameStr(*context->partial_view.name)),

					 /* partial WHERE */
					 quote_identifier(NameStr(*context->time_column_name)),
					 quote_identifier(NameStr(*context->time_column_name)),

					 /* materialization hypertable */
					 quote_identifier(NameStr(*context->materialization_table.schema)),
					 quote_identifier(NameStr(*context->materialization_table.name)),

					 /* MERGE JOIN condition */
					 build_merge_join_clause(grp_colnames),

					 /* extra MERGE JOIN condition with primary dimension */
					 quote_identifier(NameStr(*context->time_column_name)),
					 quote_identifier(NameStr(*context->time_column_name)),

					 /* UPDATE */
					 merge_update.data,

					 /* INSERT */
					 build_merge_insert_columns(all_columns, ", ", NULL),
					 build_merge_insert_columns(all_columns, ", ", "P."));
	return query.data;
}

/* Create DELETE after MERGE query statement */
static char *
create_materialization_merge_delete_statement(MaterializationContext *context)
{
	StringInfoData query;
	initStringInfo(&query);
	List *grp_colnames = cagg_find_groupingcols((ContinuousAgg *) context->cagg, context->mat_ht);

	appendStringInfo(&query,
					 "DELETE "
					 "FROM %s.%s M "
					 "WHERE M.%s >= $1 AND M.%s < $2 "
					 "AND NOT EXISTS ("
					 " SELECT FROM %s.%s P "
					 " WHERE %s AND P.%s >= $1 AND P.%s < $2) ",

					 /* materialization hypertable */
					 quote_identifier(NameStr(*context->materialization_table.schema)),
					 quote_identifier(NameStr(*context->materialization_table.name)),

					 /* materialization hypertable WHERE */
					 quote_identifier(NameStr(*context->time_column_name)),
					 quote_identifier(NameStr(*context->time_column_name)),

					 /* partial VIEW */
					 quote_identifier(NameStr(*context->partial_view.schema)),
					 quote_identifier(NameStr(*context->partial_view.name)),

					 /* MERGE JOIN condition */
					 build_merge_join_clause(grp_colnames),

					 /* partial WHERE */
					 quote_identifier(NameStr(*context->time_column_name)),
					 quote_identifier(NameStr(*context->time_column_name)));
	return query.data;
}

static void
emit_materialization_insert_error(MaterializationContext *context)
{
	elog(ERROR,
		 "could not insert old values into materialization table \"%s.%s\"",
		 NameStr(*context->materialization_table.schema),
		 NameStr(*context->materialization_table.name));
}

static void
emit_materialization_delete_error(MaterializationContext *context)
{
	elog(ERROR,
		 "could not delete old values from materialization table \"%s.%s\"",
		 NameStr(*context->materialization_table.schema),
		 NameStr(*context->materialization_table.name));
}

static void
emit_materialization_exists_error(MaterializationContext *context)
{
	elog(ERROR,
		 "could not check the materialization table \"%s.%s\"",
		 NameStr(*context->materialization_table.schema),
		 NameStr(*context->materialization_table.name));
}

static void
emit_materialization_merge_error(MaterializationContext *context)
{
	elog(ERROR,
		 "could not merge old values into materialization table \"%s.%s\"",
		 NameStr(*context->materialization_table.schema),
		 NameStr(*context->materialization_table.name));
}

static void
emit_materialization_insert_progress(MaterializationContext *context, uint64 rows_processed)
{
	elog(LOG,
		 "inserted " UINT64_FORMAT " row(s) into materialization table \"%s.%s\"",
		 rows_processed,
		 NameStr(*context->materialization_table.schema),
		 NameStr(*context->materialization_table.name));
}

static void
emit_materialization_delete_progress(MaterializationContext *context, uint64 rows_processed)
{
	elog(LOG,
		 "deleted " UINT64_FORMAT " row(s) from materialization table \"%s.%s\"",
		 rows_processed,
		 NameStr(*context->materialization_table.schema),
		 NameStr(*context->materialization_table.name));
}

static void
emit_materialization_merge_progress(MaterializationContext *context, uint64 rows_processed)
{
	elog(LOG,
		 "merged " UINT64_FORMAT " row(s) into materialization table \"%s.%s\"",
		 rows_processed,
		 NameStr(*context->materialization_table.schema),
		 NameStr(*context->materialization_table.name));
}

static Oid *
create_materialization_plan_argtypes(MaterializationContext *context,
									 MaterializationPlanType plan_type, int nargs)
{
	Oid *argtypes = (Oid *) palloc(nargs * sizeof(Oid));
	argtypes[0] = context->materialization_range.type;
	argtypes[1] = context->materialization_range.type;

	return argtypes;
}

static MaterializationPlan *
create_materialization_plan(MaterializationContext *context, MaterializationPlanType plan_type)
{
	Assert(plan_type >= PLAN_TYPE_INSERT);
	Assert(plan_type < _MAX_MATERIALIZATION_PLAN_TYPES);

	MaterializationPlan *materialization = &materialization_plans[plan_type];

	if (materialization->plan == NULL)
	{
		char *query = materialization->create_statement(context);
		Oid *argtypes =
			create_materialization_plan_argtypes(context, plan_type, materialization->nargs);

		elog(DEBUG2, "%s: %s", __func__, query);
		materialization->plan = SPI_prepare(query, materialization->nargs, argtypes);
		if (materialization->plan == NULL)
			elog(ERROR, "%s: SPI_prepare failed: %s", __func__, query);

		SPI_keepplan(materialization->plan);
		pfree(query);
		pfree(argtypes);
	}

	return materialization;
}

static void
create_materialization_plan_args(MaterializationContext *context, MaterializationPlanType plan_type,
								 int nargs, Datum **values, char **nulls)
{
	*values = (Datum *) palloc(nargs * sizeof(Datum));
	*nulls = (char *) palloc(nargs * sizeof(char));
	(*values)[0] = context->materialization_range.start;
	(*values)[1] = context->materialization_range.end;
	(*nulls)[0] = false;
	(*nulls)[1] = false;
}

static uint64
execute_materialization_plan(MaterializationContext *context, MaterializationPlanType plan_type)
{
	MaterializationPlan *materialization = create_materialization_plan(context, plan_type);

	Datum *values = NULL;
	char *nulls = NULL;
	create_materialization_plan_args(context, plan_type, materialization->nargs, &values, &nulls);

	int res = SPI_execute_plan(materialization->plan, values, nulls, materialization->read_only, 0);

	if (res < 0)
	{
		if (materialization->emit_error)
			materialization->emit_error(context);
	}

	if (materialization->emit_progress)
		materialization->emit_progress(context, SPI_processed);

	pfree(values);
	pfree(nulls);

	return SPI_processed;
}

static void
free_materialization_plan(MaterializationContext *context, MaterializationPlanType plan_type)
{
	MaterializationPlan *materialization = &materialization_plans[plan_type];

	if (materialization->plan != NULL)
	{
		SPI_freeplan(materialization->plan);
		materialization->plan = NULL;
	}
}

static void
free_materialization_plans(MaterializationContext *context)
{
	for (int plan_type = PLAN_TYPE_INSERT; plan_type < _MAX_MATERIALIZATION_PLAN_TYPES; plan_type++)
	{
		free_materialization_plan(context, plan_type);
	}
}

static void
update_watermark(MaterializationContext *context)
{
	int res;
	StringInfo command = makeStringInfo();
	Oid types[] = { context->materialization_range.type };
	Datum values[] = { context->materialization_range.start };
	char nulls[] = { false };

	appendStringInfo(command,
					 "SELECT %s FROM %s.%s AS I "
					 "WHERE I.%s >= $1 %s "
					 "ORDER BY 1 DESC LIMIT 1;",
					 quote_identifier(NameStr(*context->time_column_name)),
					 quote_identifier(NameStr(*context->materialization_table.schema)),
					 quote_identifier(NameStr(*context->materialization_table.name)),
					 quote_identifier(NameStr(*context->time_column_name)),
					 context->chunk_condition);

	elog(DEBUG2, "%s: %s", __func__, command->data);
	res = SPI_execute_with_args(command->data,
								1,
								types,
								values,
								nulls,
								false /* read_only */,
								0 /* count */);

	if (res < 0)
		elog(ERROR, "%s: could not get the last bucket of the materialized data", __func__);

	Ensure(SPI_gettypeid(SPI_tuptable->tupdesc, 1) == context->materialization_range.type,
		   "partition types for result (%d) and dimension (%d) do not match",
		   SPI_gettypeid(SPI_tuptable->tupdesc, 1),
		   context->materialization_range.type);

	if (SPI_processed > 0)
	{
		bool isnull;
		Datum maxdat = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);

		if (!isnull)
		{
			int64 watermark =
				ts_time_value_to_internal(maxdat, context->materialization_range.type);
			ts_cagg_watermark_update(context->mat_ht, watermark, isnull, false);
		}
	}
}

static void
execute_materializations(MaterializationContext *context)
{
	volatile uint64 rows_processed = 0;

	PG_TRY();
	{
		/* MERGE statement is available starting on PG15 and we'll support it only in the new format
		 * of CAggs and for non-compressed hypertables */
		if (ts_guc_enable_merge_on_cagg_refresh && PG_VERSION_NUM >= 150000 &&
			ContinuousAggIsFinalized(context->cagg) &&
			!TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(context->mat_ht))
		{
			/* Fallback to INSERT materializations if there are no rows to change on it */
			if (execute_materialization_plan(context, PLAN_TYPE_EXISTS) == 0)
			{
				elog(DEBUG2,
					 "no rows to merge on materialization table \"%s.%s\", falling back to INSERT",
					 NameStr(*context->materialization_table.schema),
					 NameStr(*context->materialization_table.name));
				rows_processed = execute_materialization_plan(context, PLAN_TYPE_INSERT);
			}
			else
			{
				rows_processed += execute_materialization_plan(context, PLAN_TYPE_MERGE);
				rows_processed += execute_materialization_plan(context, PLAN_TYPE_MERGE_DELETE);
			}
		}
		else
		{
			rows_processed += execute_materialization_plan(context, PLAN_TYPE_DELETE);
			rows_processed += execute_materialization_plan(context, PLAN_TYPE_INSERT);
		}

		/* Free all cached plans */
		free_materialization_plans(context);
	}
	PG_CATCH();
	{
		/* Make sure all cached plans in the session be released before rethrowing the error */
		free_materialization_plans(context);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Get the max(time_dimension) of the materialized data */
	if (rows_processed > 0)
	{
		update_watermark(context);
	}
}
