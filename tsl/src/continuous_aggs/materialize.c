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
#include <utils/lsyscache.h>
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
#include "ts_catalog/array_utils.h"
#include "ts_catalog/compression_settings.h"
#include "ts_catalog/continuous_agg.h"
#include "ts_catalog/continuous_aggs_watermark.h"

/*********************
 * utility functions *
 *********************/
static TimeRange internal_time_range_to_time_range(InternalTimeRange internal);
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
	PLAN_TYPE_INSERT_BY_TENANT,
	PLAN_TYPE_DELETE_BY_TENANT,
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
	InternalTimeRange internal_materialization_range;
	int nargs;

	/*
	 * Optional per-tenant filter. NULL tenant_column_name => time-only path.
	 * tenant_type is the element type (e.g. INT4OID). tenant_values is an
	 * ArrayType* (as Datum) of that element type; the SQL uses `= ANY($3)`.
	 */
	const NameData *tenant_column_name;
	Oid tenant_type;
	Datum tenant_values;
} MaterializationContext;

typedef char *(*MaterializationCreateStatement)(MaterializationContext *context);

typedef struct MaterializationPlan
{
	SPIPlanPtr plan;
	bool read_only;
	bool catalog_security_context;
	int nargs;
	MaterializationCreateStatement create_statement;
	const char *error_message;
	const char *progress_message;
	/*
	 * Type of $3 (tenant value) recorded at SPI_prepare time. Only meaningful
	 * for *_BY_TENANT slots; left InvalidOid otherwise. Used to detect when a
	 * cached plan was compiled for a different tenant column type and must
	 * be freed before recompiling.
	 */
	Oid last_tenant_type;
} MaterializationPlan;

static char *build_order_by_clause(MaterializationContext *context);
static char *create_materialization_insert_statement(MaterializationContext *context);
static char *create_materialization_delete_statement(MaterializationContext *context);
static char *create_materialization_exists_statement(MaterializationContext *context);
static char *create_materialization_merge_statement(MaterializationContext *context);
static char *create_materialization_merge_delete_statement(MaterializationContext *context);
static char *create_materialization_insert_by_tenant_statement(MaterializationContext *context);
static char *create_materialization_delete_by_tenant_statement(MaterializationContext *context);
static MaterializationPlan materialization_plans[_MAX_MATERIALIZATION_PLAN_TYPES + 1] = {
	[PLAN_TYPE_INSERT] = { .nargs = 2,
						   .create_statement = create_materialization_insert_statement,
						   .error_message =
							   "could not insert old values into materialization table \"%s.%s\"",
						   .progress_message = "inserted " UINT64_FORMAT
											   " row(s) into materialization table \"%s.%s\"" },
	[PLAN_TYPE_DELETE] = { .nargs = 2,
						   .create_statement = create_materialization_delete_statement,
						   .error_message =
							   "could not delete old values from materialization table \"%s.%s\"",
						   .progress_message = "deleted " UINT64_FORMAT
											   " row(s) from materialization table \"%s.%s\"" },
	[PLAN_TYPE_EXISTS] = { .read_only = true,
						   .nargs = 2,
						   .create_statement = create_materialization_exists_statement,
						   .error_message = "could not check the materialization table \"%s.%s\"" },
	[PLAN_TYPE_MERGE] = { .nargs = 2,
						  .create_statement = create_materialization_merge_statement,
						  .error_message =
							  "could not merge old values into materialization table \"%s.%s\"",
						  .progress_message = "merged " UINT64_FORMAT
											  " row(s) into materialization table \"%s.%s\"" },
	[PLAN_TYPE_MERGE_DELETE] = { .nargs = 2,
								 .create_statement = create_materialization_merge_delete_statement,
								 .error_message = "could not delete old values from "
												  "materialization table \"%s.%s\"",
								 .progress_message =
									 "deleted " UINT64_FORMAT
									 " row(s) from materialization table \"%s.%s\"" },
	[PLAN_TYPE_INSERT_BY_TENANT] = { .nargs = 3,
									 .create_statement =
										 create_materialization_insert_by_tenant_statement,
									 .error_message = "could not insert old values into "
													  "materialization table \"%s.%s\"",
									 .progress_message =
										 "inserted " UINT64_FORMAT
										 " row(s) into materialization table \"%s.%s\"" },
	[PLAN_TYPE_DELETE_BY_TENANT] = { .nargs = 3,
									 .create_statement =
										 create_materialization_delete_by_tenant_statement,
									 .error_message = "could not delete old values from "
													  "materialization table \"%s.%s\"",
									 .progress_message =
										 "deleted " UINT64_FORMAT
										 " row(s) from materialization table \"%s.%s\"" },
};

static Oid *create_materialization_plan_argtypes(MaterializationContext *context,
												 MaterializationPlanType plan_type, int nargs);
static MaterializationPlan *create_materialization_plan(MaterializationContext *context,
														MaterializationPlanType plan_type);
static void create_materialization_plan_args(MaterializationContext *context,
											 MaterializationPlanType plan_type, Datum **values,
											 char **nulls);
static uint64 execute_materialization_plan(MaterializationContext *context,
										   MaterializationPlanType plan_type);
static void free_materialization_plan(MaterializationContext *context,
									  MaterializationPlanType plan_type);
static void free_materialization_plans(MaterializationContext *context);

static void update_watermark(MaterializationContext *context);
static void execute_materializations(MaterializationContext *context);
static void execute_materializations_by_tenant(MaterializationContext *context);

/* API to update materializations from refresh code */
void
continuous_agg_update_materialization(Hypertable *mat_ht, const ContinuousAgg *cagg,
									  SchemaAndName partial_view,
									  SchemaAndName materialization_table,
									  const NameData *time_column_name,
									  InternalTimeRange materialization_range)
{
	MaterializationContext context = {
		.mat_ht = mat_ht,
		.cagg = cagg,
		.partial_view = partial_view,
		.materialization_table = materialization_table,
		.time_column_name = (NameData *) time_column_name,
		.materialization_range = internal_time_range_to_time_range(materialization_range),
		.internal_materialization_range = materialization_range,
	};

	/* Lock down search_path */
	int save_nestlevel = NewGUCNestLevel();
	RestrictSearchPath();

	/* pin the start of new_materialization to the end of new_materialization,
	 * we are not allowed to materialize beyond that point
	 */
	if (materialization_range.start > materialization_range.end)
		materialization_range.start = materialization_range.end;

	/* Then insert the materializations */
	context.materialization_range = internal_time_range_to_time_range(materialization_range);
	execute_materializations(&context);

	/* Restore search_path */
	AtEOXact_GUC(false, save_nestlevel);
}

/*
 * Per-tenant variant of continuous_agg_update_materialization. Re-materializes
 * a (tenants[], time-range) rectangle by issuing a DELETE then INSERT both
 * narrowed by AND <tenant_column_name> = ANY(<tenant_values_array>).
 *
 * Caller must ensure tenant_column_name names a real column on mat_ht and
 * the partial view (i.e. it is a GROUP BY column of the cagg defining query),
 * tenant_type matches that column's atttypid, and tenant_values_array is a
 * constructed ArrayType* (as Datum) with that element type.
 */
void
continuous_agg_update_materialization_for_tenant(Hypertable *mat_ht, const ContinuousAgg *cagg,
												 SchemaAndName partial_view,
												 SchemaAndName materialization_table,
												 const NameData *time_column_name,
												 InternalTimeRange materialization_range,
												 const NameData *tenant_column_name,
												 Datum tenant_values_array, Oid tenant_type)
{
	MaterializationContext context = {
		.mat_ht = mat_ht,
		.cagg = cagg,
		.partial_view = partial_view,
		.materialization_table = materialization_table,
		.time_column_name = (NameData *) time_column_name,
		.materialization_range = internal_time_range_to_time_range(materialization_range),
		.internal_materialization_range = materialization_range,
		.tenant_column_name = tenant_column_name,
		.tenant_values = tenant_values_array,
		.tenant_type = tenant_type,
	};

	int save_nestlevel = NewGUCNestLevel();
	RestrictSearchPath();

	if (materialization_range.start > materialization_range.end)
		materialization_range.start = materialization_range.end;

	context.materialization_range = internal_time_range_to_time_range(materialization_range);
	execute_materializations_by_tenant(&context);

	AtEOXact_GUC(false, save_nestlevel);
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
	StringInfoData ret;
	initStringInfo(&ret);

	Assert(strings != NIL);

	ListCell *lc;
	foreach (lc, strings)
	{
		char *grpcol = (char *) lfirst(lc);
		if (ret.len > 0)
			appendStringInfoString(&ret, separator);

		if (prefix)
			appendStringInfoString(&ret, prefix);
		appendStringInfoString(&ret, quote_identifier(grpcol));
	}

	elog(DEBUG2, "%s: %s", __func__, ret.data);
	return ret.data;
}

static char *
build_merge_join_clause(List *column_names)
{
	StringInfoData ret;
	initStringInfo(&ret);

	Assert(column_names != NIL);

	ListCell *lc;
	foreach (lc, column_names)
	{
		char *column = (char *) lfirst(lc);

		if (ret.len > 0)
			appendStringInfoString(&ret, " AND ");

		appendStringInfoString(&ret, "P.");
		appendStringInfoString(&ret, quote_identifier(column));
		appendStringInfoString(&ret, " IS NOT DISTINCT FROM M.");
		appendStringInfoString(&ret, quote_identifier(column));
	}

	elog(DEBUG2, "%s: %s", __func__, ret.data);
	return ret.data;
}

static char *
build_merge_update_clause(List *column_names)
{
	StringInfoData ret;
	initStringInfo(&ret);

	Assert(column_names != NIL);

	ListCell *lc;
	foreach (lc, column_names)
	{
		char *column = (char *) lfirst(lc);

		if (ret.len > 0)
			appendStringInfoString(&ret, ", ");

		appendStringInfoString(&ret, quote_identifier(column));
		appendStringInfoString(&ret, " = P.");
		appendStringInfoString(&ret, quote_identifier(column));
	}

	elog(DEBUG2, "%s: %s", __func__, ret.data);
	return ret.data;
}

/* Build ORDER BY clause based on segmentby + orderby compression settings */
static char *
build_order_by_clause(MaterializationContext *context)
{
	/* Don't build ORDER BY clause if compression is not enabled */
	if (!TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(context->mat_ht))
		return ""; /* No ORDER BY if no compression */

	CompressionSettings *settings = ts_compression_settings_get(context->mat_ht->main_table_relid);

	int num_segmentby = ts_array_length(settings->fd.segmentby);
	int num_orderby = ts_array_length(settings->fd.orderby);

	StringInfo ret = makeStringInfo();
	appendStringInfoString(ret, "ORDER BY ");

	/* process segmentby settings */
	for (int i = 1; i <= num_segmentby; i++)
	{
		if (i > 1)
			appendStringInfoString(ret, ", ");
		appendStringInfoString(ret,
							   quote_identifier(
								   ts_array_get_element_text(settings->fd.segmentby, i)));
	}

	/* process orderby settings */
	for (int i = 1; i <= num_orderby; i++)
	{
		bool is_orderby_desc = ts_array_get_element_bool(settings->fd.orderby_desc, i);
		bool is_null_first = ts_array_get_element_bool(settings->fd.orderby_nullsfirst, i);

		if (num_segmentby > 0 || i > 1)
			appendStringInfoString(ret, ", ");
		appendStringInfoString(ret,
							   quote_identifier(
								   ts_array_get_element_text(settings->fd.orderby, i)));
		if (is_orderby_desc)
			appendStringInfoString(ret, " DESC");
		else
			appendStringInfoString(ret, " ASC");
		if (is_null_first)
			appendStringInfoString(ret, " NULLS FIRST");
		else
			appendStringInfoString(ret, " NULLS LAST");
	}

	elog(DEBUG2, "%s: %s", __func__, ret->data);
	return ret->data;
}

static inline bool
has_direct_compress_on_cagg_refresh_enabled(MaterializationContext *context)
{
	return ts_guc_enable_direct_compress_on_cagg_refresh &&
		   TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(context->mat_ht);
}

/* Create INSERT statement */
static char *
create_materialization_insert_statement(MaterializationContext *context)
{
	/* If direct compress on cagg refresh is enabled, build ORDER BY clause based on segmentby and
	 * orderby settings. This is necessary because we set
	 * `timescaledb.enable_direct_compress_insert_client_sorted=on` in order to send ordered data to
	 * the compressor. */
	char *orderby =
		has_direct_compress_on_cagg_refresh_enabled(context) ? build_order_by_clause(context) : "";

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
					 orderby);
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
					 "WHERE D.%s >= $1 AND D.%s < $2;",
					 quote_identifier(NameStr(*context->materialization_table.schema)),
					 quote_identifier(NameStr(*context->materialization_table.name)),
					 quote_identifier(NameStr(*context->time_column_name)),
					 quote_identifier(NameStr(*context->time_column_name)));
	return query.data;
}

/*
 * Create INSERT statement scoped to the set of tenants in the $3 array.
 * The array element type matches context->tenant_type; SPI binds $3 as the
 * corresponding array type.
 */
static char *
create_materialization_insert_by_tenant_statement(MaterializationContext *context)
{
	char *orderby =
		has_direct_compress_on_cagg_refresh_enabled(context) ? build_order_by_clause(context) : "";

	Assert(context->tenant_column_name != NULL);

	StringInfoData query;
	initStringInfo(&query);
	appendStringInfo(&query,
					 "INSERT INTO %s.%s SELECT * FROM %s.%s AS I "
					 "WHERE I.%s >= $1 AND I.%s < $2 AND I.%s = ANY($3) %s;",
					 quote_identifier(NameStr(*context->materialization_table.schema)),
					 quote_identifier(NameStr(*context->materialization_table.name)),
					 quote_identifier(NameStr(*context->partial_view.schema)),
					 quote_identifier(NameStr(*context->partial_view.name)),
					 quote_identifier(NameStr(*context->time_column_name)),
					 quote_identifier(NameStr(*context->time_column_name)),
					 quote_identifier(NameStr(*context->tenant_column_name)),
					 orderby);
	return query.data;
}

/*
 * Create DELETE statement scoped to the set of tenants in the $3 array.
 */
static char *
create_materialization_delete_by_tenant_statement(MaterializationContext *context)
{
	Assert(context->tenant_column_name != NULL);

	StringInfoData query;
	initStringInfo(&query);
	appendStringInfo(&query,
					 "DELETE FROM %s.%s AS D "
					 "WHERE D.%s >= $1 AND D.%s < $2 AND D.%s = ANY($3);",
					 quote_identifier(NameStr(*context->materialization_table.schema)),
					 quote_identifier(NameStr(*context->materialization_table.name)),
					 quote_identifier(NameStr(*context->time_column_name)),
					 quote_identifier(NameStr(*context->time_column_name)),
					 quote_identifier(NameStr(*context->tenant_column_name)));
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

static inline bool
is_by_tenant_plan_type(MaterializationPlanType plan_type)
{
	return plan_type == PLAN_TYPE_INSERT_BY_TENANT || plan_type == PLAN_TYPE_DELETE_BY_TENANT;
}

static Oid *
create_materialization_plan_argtypes(MaterializationContext *context,
									 MaterializationPlanType plan_type, int nargs)
{
	Oid *argtypes = (Oid *) palloc(nargs * sizeof(Oid));

	argtypes[0] = context->materialization_range.type;
	argtypes[1] = context->materialization_range.type;

	if (is_by_tenant_plan_type(plan_type))
	{
		Assert(nargs == 3);
		Assert(OidIsValid(context->tenant_type));

		/*
		 * SQL uses `= ANY($3)`, so $3 must be declared as the array type of
		 * the tenant column's element type.
		 */
		Oid array_type = get_array_type(context->tenant_type);
		if (!OidIsValid(array_type))
			elog(ERROR,
				 "no array type found for tenant column element type %u",
				 context->tenant_type);
		argtypes[2] = array_type;
	}

	return argtypes;
}

static MaterializationPlan *
create_materialization_plan(MaterializationContext *context, MaterializationPlanType plan_type)
{
	Assert(plan_type >= PLAN_TYPE_INSERT);
	Assert(plan_type < _MAX_MATERIALIZATION_PLAN_TYPES);

	MaterializationPlan *materialization = &materialization_plans[plan_type];
	bool is_by_tenant = is_by_tenant_plan_type(plan_type);

	/*
	 * If a by-tenant plan was cached for a different tenant column type, the
	 * compiled argtypes[2] no longer matches the value we'd bind. Free the
	 * stale plan so the block below recompiles it.
	 */
	if (is_by_tenant && materialization->plan != NULL &&
		materialization->last_tenant_type != context->tenant_type)
	{
		SPI_freeplan(materialization->plan);
		materialization->plan = NULL;
	}

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
		if (is_by_tenant)
			materialization->last_tenant_type = context->tenant_type;

		pfree(query);
		pfree(argtypes);
	}

	return materialization;
}

static void
create_materialization_plan_args(MaterializationContext *context, MaterializationPlanType plan_type,
								 Datum **values, char **nulls)
{
	(*values)[0] = context->materialization_range.start;
	(*values)[1] = context->materialization_range.end;
	(*nulls)[0] = false;
	(*nulls)[1] = false;

	if (is_by_tenant_plan_type(plan_type))
	{
		(*values)[2] = context->tenant_values;
		(*nulls)[2] = false;
	}
}

static uint64
execute_materialization_plan(MaterializationContext *context, MaterializationPlanType plan_type)
{
	MaterializationPlan *materialization = create_materialization_plan(context, plan_type);

	Datum *values = (Datum *) palloc(materialization->nargs * sizeof(Datum));
	char *nulls = (char *) palloc(materialization->nargs * sizeof(char));

	create_materialization_plan_args(context, plan_type, &values, &nulls);

	CatalogSecurityContext sec_ctx;
	if (materialization->catalog_security_context)
		ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);

	int res = SPI_execute_plan(materialization->plan, values, nulls, materialization->read_only, 0);

	if (materialization->catalog_security_context)
		ts_catalog_restore_user(&sec_ctx);

	if (res < 0)
	{
		Ensure(materialization->error_message,
			   "materialization plan error message not set for plan type %d",
			   plan_type);
		elog(ERROR,
			 materialization->error_message,
			 NameStr(*context->materialization_table.schema),
			 NameStr(*context->materialization_table.name));
	}
	else if (materialization->progress_message)
	{
		elog(LOG,
			 materialization->progress_message,
			 SPI_processed,
			 NameStr(*context->materialization_table.schema),
			 NameStr(*context->materialization_table.name));
	}

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
	StringInfoData command;
	Oid types[] = { context->materialization_range.type };
	Datum values[] = { context->materialization_range.start };
	char nulls[] = { false };

	initStringInfo(&command);
	appendStringInfo(&command,
					 "SELECT %s FROM %s.%s AS I "
					 "WHERE I.%s >= $1 "
					 "ORDER BY 1 DESC LIMIT 1;",
					 quote_identifier(NameStr(*context->time_column_name)),
					 quote_identifier(NameStr(*context->materialization_table.schema)),
					 quote_identifier(NameStr(*context->materialization_table.name)),
					 quote_identifier(NameStr(*context->time_column_name)));

	elog(DEBUG2, "%s: %s", __func__, command.data);
	res = SPI_execute_with_args(command.data,
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
	bool prev_enable_direct_compress_insert = ts_guc_enable_direct_compress_insert;
	bool prev_enable_direct_compress_insert_client_sorted =
		ts_guc_enable_direct_compress_insert_client_sorted;

	if (has_direct_compress_on_cagg_refresh_enabled(context))
	{
		/* Force the direct compress on INSERT */
		SetConfigOption("timescaledb.enable_direct_compress_insert",
						"on",
						PGC_USERSET,
						PGC_S_SESSION);

		SetConfigOption("timescaledb.enable_direct_compress_insert_client_sorted",
						"on",
						PGC_USERSET,
						PGC_S_SESSION);
	}

	PG_TRY();
	{
		/* MERGE statement is supported only for non-compressed CAggs */
		if (ts_guc_enable_merge_on_cagg_refresh &&
			!TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(context->mat_ht))
		{
			/* Fallback to INSERT materializations if there are no rows to change on it */
			if (execute_materialization_plan(context, PLAN_TYPE_EXISTS) == 0)
			{
				elog(DEBUG2,
					 "no rows to merge on materialization table \"%s.%s\", falling back to "
					 "INSERT",
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

	/* Restore previous GUC values */
	SetConfigOption("timescaledb.enable_direct_compress_insert",
					prev_enable_direct_compress_insert ? "on" : "off",
					PGC_USERSET,
					PGC_S_SESSION);
	SetConfigOption("timescaledb.enable_direct_compress_insert_client_sorted",
					prev_enable_direct_compress_insert_client_sorted ? "on" : "off",
					PGC_USERSET,
					PGC_S_SESSION);
}

/*
 * Per-tenant materialization: DELETE then INSERT, both filtered by the bound
 * tenant_value. No MERGE / EXISTS path — backfill refreshes always do the
 * straightforward delete-then-insert pair.
 */
static void
execute_materializations_by_tenant(MaterializationContext *context)
{
	volatile uint64 rows_processed = 0;

	Assert(context->tenant_column_name != NULL);
	Assert(OidIsValid(context->tenant_type));

	PG_TRY();
	{
		rows_processed += execute_materialization_plan(context, PLAN_TYPE_DELETE_BY_TENANT);
		rows_processed += execute_materialization_plan(context, PLAN_TYPE_INSERT_BY_TENANT);

		free_materialization_plans(context);
	}
	PG_CATCH();
	{
		free_materialization_plans(context);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (rows_processed > 0)
		update_watermark(context);
}
