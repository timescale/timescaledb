/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>

#include <access/tupconvert.h>
#include <access/xact.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <commands/trigger.h>
#include <executor/spi.h>
#include <fmgr.h>
#include <lib/stringinfo.h>
#include <storage/lmgr.h>
#include <utils/builtins.h>
#include <utils/rel.h>
#include <utils/relcache.h>
#include <utils/date.h>
#include <utils/snapmgr.h>

#include <scanner.h>
#include <compat.h>
#include <interval.h>
#include <scan_iterator.h>

#include "chunk.h"
#include "continuous_agg.h"
#include "dimension.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "partitioning.h"

#include "utils.h"
#include "time_bucket.h"

#include "continuous_aggs/materialize.h"

/***********************
 * Time ranges
 ***********************/

typedef struct InternalTimeRange
{
	Oid type;
	int64 start; /* inclusive */
	int64 end;   /* exclusive */
} InternalTimeRange;

typedef struct TimeRange
{
	Oid type;
	Datum start;
	Datum end;
} TimeRange;

static bool ranges_overlap(InternalTimeRange invalidation_range,
						   InternalTimeRange new_materialization_range);
static TimeRange internal_time_range_to_time_range(InternalTimeRange internal);
static bool range_expand_right(InternalTimeRange *range, const InternalTimeRange to_cover,
							   int64 max_length, int64 bucket_width);
static void range_clamp_end(InternalTimeRange *range, int64 end_value, int64 bucket_width);
static void range_check(const InternalTimeRange range, int64 bucket_width);
static int64 range_length(const InternalTimeRange range);
static bool range_cut_invalidation_entry_left(const InternalTimeRange invalidation_range_knife,
											  int64 *lowest_modified_value,
											  int64 *greatest_modified_value,
											  bool *delete_entry_out);
/***********************
 * materialization job *
 ***********************/

static Form_continuous_agg get_continuous_agg(int32 mat_hypertable_id);
static int64 get_materialization_end_point_for_table(int32 raw_hypertable_id,
													 int32 materialization_id, int64 refresh_lag,
													 int64 bucket_width, int64 max_interval_per_job,
													 int64 completed_threshold,
													 bool *materializing_new_range,
													 bool *truncated_materialization, bool verbose);
static void drain_invalidation_log(int32 raw_hypertable_id, List **invalidations_out);
static void insert_materialization_invalidation_logs(List *caggs, List *invalidations,
													 Relation rel);
static void invalidation_threshold_set(int32 raw_hypertable_id, int64 invalidation_threshold);
static Datum internal_to_time_value_or_infinite(int64 internal, Oid time_type,
												bool *is_infinite_out);
static InternalTimeRange materialization_invalidation_log_get_range(int32 materialization_id,
																	Oid type, int64 bucket_width,
																	int64 max_interval_per_job,
																	int64 completed_threshold,
																	int64 invalidate_prior_to_time);
static void materialization_invalidation_log_delete_or_cut(int32 cagg_id,
														   InternalTimeRange invalidation_range,
														   int64 completed_threshold);
/* must be called without a transaction started if !options->within_single_transaction */
bool
continuous_agg_materialize(int32 materialization_id, ContinuousAggMatOptions *options)
{
	Hypertable *raw_hypertable;
	Oid raw_table_oid;
	Relation raw_table_relation;
	Hypertable *materialization_table;
	Oid materialization_table_oid;
	Relation materialization_table_relation;
	Relation materialization_invalidation_log_table_relation;
	Oid partial_view_oid;
	Relation partial_view_relation;
	LockRelId raw_lock_relid;
	LockRelId materialization_lock_relid;
	LockRelId partial_view_lock_relid;
	Form_continuous_agg cagg;
	FormData_continuous_agg cagg_data;
	int64 materialization_invalidation_threshold;
	bool materializing_new_range = false;
	bool truncated_materialization = false;
	SchemaAndName partial_view;
	List *caggs = NIL;
	Catalog *catalog;
	InternalTimeRange invalidation_range;
	Oid time_column_type;
	List *invalidations = NIL;
	int64 completed_threshold;

	/* validate the options */
	if (options->within_single_transaction)
	{
		/* if not using multiple txns, should not change the invalidation
		 * threshold and thus cannot only do invalidations */
		Assert(options->process_only_invalidation);
	}

	/*
	 * Transaction 1: a) copy the invalidation logs and then
	 *                b) discover the new range in the raw table we will materialize
	 *                and update the invalidation threshold to it, if needed
	 */
	if (!options->within_single_transaction)
	{
		StartTransactionCommand();
		/* we need a snapshot for the SPI commands in get_materialization_end_point_for_table */
		PushActiveSnapshot(GetTransactionSnapshot());
	}

	cagg = get_continuous_agg(materialization_id);
	if (cagg == NULL)
		elog(ERROR, "no continuous aggregate with materialization_id %d", materialization_id);

	/* copy the continuous aggregate data into a local variable, we want to access it after
	 * we close this transaction
	 */
	cagg_data = *cagg;

	/* We're going to close this transaction, so SessionLock the objects we are using for this
	 * materialization to ensure they're not alter in a way which would invalidate our work.
	 * We need to lock:
	 *     The raw table, to prevent it from being ALTERed too much
	 *     The materialization table, to prevent concurrent materializations
	 *     The partial_view, to prevent it from being renamed (we pass the names across
	 * transactions) and to prevent the view definition from being altered We still wish to allow
	 * SELECTs on all of these Continuous aggregate state should be locked in the order
	 *    raw hypertable / user view
	 *    materialization table
	 *    partial view
	 */
	raw_hypertable = ts_hypertable_get_by_id(cagg_data.raw_hypertable_id);
	if (raw_hypertable == NULL)
		elog(ERROR, "hypertable dropped before materialization could start");

	raw_table_oid = raw_hypertable->main_table_relid;
	raw_table_relation = relation_open(raw_table_oid, AccessShareLock);
	raw_lock_relid = raw_table_relation->rd_lockInfo.lockRelId;
	time_column_type =
		ts_dimension_get_partition_type(hyperspace_get_open_dimension(raw_hypertable->space, 0));
	LockRelationIdForSession(&raw_lock_relid, AccessShareLock);
	relation_close(raw_table_relation, NoLock);

	materialization_table = ts_hypertable_get_by_id(cagg_data.mat_hypertable_id);
	if (materialization_table == NULL)
		elog(ERROR, "materialization table dropped before materialization could start");

	materialization_table_oid = materialization_table->main_table_relid;

	materialization_table_relation =
		relation_open(materialization_table_oid, ShareRowExclusiveLock);
	materialization_lock_relid = materialization_table_relation->rd_lockInfo.lockRelId;
	LockRelationIdForSession(&materialization_lock_relid, ShareRowExclusiveLock);
	relation_close(materialization_table_relation, NoLock);

	partial_view_oid =
		get_relname_relid(NameStr(cagg_data.partial_view_name),
						  get_namespace_oid(NameStr(cagg_data.partial_view_schema), false));
	Assert(OidIsValid(partial_view_oid));
	partial_view_relation = relation_open(partial_view_oid, ShareRowExclusiveLock);
	partial_view_lock_relid = partial_view_relation->rd_lockInfo.lockRelId;
	LockRelationIdForSession(&partial_view_lock_relid, ShareRowExclusiveLock);
	relation_close(partial_view_relation, NoLock);

	catalog = ts_catalog_get();

	/* copy over all the materializations from the raw hypertable to all the continuous aggs */
	caggs = ts_continuous_aggs_find_by_raw_table_id(cagg_data.raw_hypertable_id);
	drain_invalidation_log(cagg_data.raw_hypertable_id, &invalidations);
	materialization_invalidation_log_table_relation =
		table_open(catalog_get_table_id(catalog, CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG),
				   RowExclusiveLock);
	insert_materialization_invalidation_logs(caggs,
											 invalidations,
											 materialization_invalidation_log_table_relation);

	completed_threshold = ts_continuous_agg_get_completed_threshold(materialization_id);

	/* decide on the invalidation and new materialization ranges */
	invalidation_range =
		materialization_invalidation_log_get_range(materialization_id,
												   time_column_type,
												   cagg_data.bucket_width,
												   cagg_data.max_interval_per_job,
												   completed_threshold,
												   options->invalidate_prior_to_time);

	if (range_length(invalidation_range) >= cagg_data.max_interval_per_job)
		truncated_materialization = true;

	if (options->process_only_invalidation || truncated_materialization)
	{
		materializing_new_range = false;
		materialization_invalidation_threshold = completed_threshold;
	}
	else
	{
		materialization_invalidation_threshold =
			get_materialization_end_point_for_table(cagg_data.raw_hypertable_id,
													materialization_id,
													cagg_data.refresh_lag,
													cagg_data.bucket_width,
													cagg_data.max_interval_per_job -
														range_length(invalidation_range),
													completed_threshold,
													&materializing_new_range,
													&truncated_materialization,
													options->verbose);
	}

	if (options->verbose)
	{
		StringInfo msg = makeStringInfo();
		appendStringInfo(msg,
						 "materializing continuous aggregate %s.%s: ",
						 NameStr(cagg_data.user_view_schema),
						 NameStr(cagg_data.user_view_name));

		if (range_length(invalidation_range) > 0)
			appendStringInfo(msg, "processing invalidations, ");
		else
			appendStringInfo(msg, "nothing to invalidate, ");

		if (materializing_new_range)
			appendStringInfo(msg,
							 "new range up to %s",
							 ts_internal_to_time_string(materialization_invalidation_threshold,
														time_column_type));
		else
			appendStringInfo(msg, "no new range");
		elog(LOG, "%s", msg->data);
	}

	if (materializing_new_range)
	{
		LockRelationOid(catalog_get_table_id(catalog, CONTINUOUS_AGGS_INVALIDATION_THRESHOLD),
						AccessExclusiveLock);
		invalidation_threshold_set(cagg_data.raw_hypertable_id,
								   materialization_invalidation_threshold);
	}

	table_close(materialization_invalidation_log_table_relation, NoLock);

	if (!options->within_single_transaction)
	{
		PopActiveSnapshot();
		CommitTransactionCommand();
	}

	/*
	 * Transaction 2:
	 *                run the materialization, and update completed_threshold
	 * read per-cagg log for materialization
	 */
	if (!options->within_single_transaction)
	{
		StartTransactionCommand();
		/* we need a snapshot for the SPI commands within continuous_agg_execute_materialization */
		PushActiveSnapshot(GetTransactionSnapshot());
	}

	/* adjust the materialization invalidation log to delete or cut any entries that
	 * will be processed in the invalidation range. This must occur in the second transaction
	 * so that it shares fate with the query to populate the invalidation range */
	materialization_invalidation_log_delete_or_cut(materialization_id,
												   invalidation_range,
												   completed_threshold);
	/* if there's nothing to materialize, don't bother with the rest */
	if (!materializing_new_range && range_length(invalidation_range) == 0)
	{
		if (options->verbose)
			elog(LOG,
				 "materializing continuous aggregate %s.%s: no new range to materialize or "
				 "invalidations found, exiting early",
				 NameStr(cagg_data.user_view_schema),
				 NameStr(cagg_data.user_view_name));
		goto finish;
	}

	partial_view = (SchemaAndName){
		.schema = &cagg_data.partial_view_schema,
		.name = &cagg_data.partial_view_name,
	};
	catalog = ts_catalog_get();
	LockRelationOid(catalog_get_table_id(catalog, CONTINUOUS_AGGS_COMPLETED_THRESHOLD),
					RowExclusiveLock);

	continuous_agg_execute_materialization(cagg_data.bucket_width,
										   cagg_data.raw_hypertable_id,
										   cagg_data.mat_hypertable_id,
										   partial_view,
										   invalidation_range.start,
										   invalidation_range.end,
										   materialization_invalidation_threshold);

finish:
	UnlockRelationIdForSession(&partial_view_lock_relid, ShareRowExclusiveLock);
	UnlockRelationIdForSession(&materialization_lock_relid, ShareRowExclusiveLock);
	UnlockRelationIdForSession(&raw_lock_relid, AccessShareLock);

	if (!options->within_single_transaction)
	{
		PopActiveSnapshot();
		CommitTransactionCommand();
	}

	return !truncated_materialization;
}

static ScanTupleResult
continuous_agg_tuple_found(TupleInfo *ti, void *data)
{
	Form_continuous_agg *cagg = data;
	*cagg = (Form_continuous_agg) GETSTRUCT(ti->tuple);
	return SCAN_CONTINUE;
}

static Form_continuous_agg
get_continuous_agg(int32 mat_hypertable_id)
{
	Form_continuous_agg cagg = NULL;
	ScanKeyData scankey[1];
	bool found;

	ScanKeyInit(&scankey[0],
				Anum_continuous_agg_pkey_mat_hypertable_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(mat_hypertable_id));

	found = ts_catalog_scan_one(CONTINUOUS_AGG,
								CONTINUOUS_AGG_PKEY,
								scankey,
								1,
								continuous_agg_tuple_found,
								AccessShareLock,
								CONTINUOUS_AGG_TABLE_NAME,
								&cagg);

	if (!found)
		return NULL;

	return cagg;
}

static bool hypertable_get_min_and_max_time_value(SchemaAndName hypertable, Name time_column,
												  int64 search_start, Oid time_type, int64 *min_out,
												  int64 *max_out);

static int64
get_materialization_end_point_for_table(int32 raw_hypertable_id, int32 materialization_id,
										int64 refresh_lag, int64 bucket_width,
										int64 max_interval_per_job, int64 old_completed_threshold,
										bool *materializing_new_range,
										bool *truncated_materialization, bool verbose)
{
	Hypertable *raw_table = ts_hypertable_get_by_id(raw_hypertable_id);
	SchemaAndName hypertable = {
		.schema = &raw_table->fd.schema_name,
		.name = &raw_table->fd.table_name,
	};
	Dimension *time_column = hyperspace_get_open_dimension(raw_table->space, 0);
	NameData time_column_name = time_column->fd.column_name;
	Oid time_column_type = ts_dimension_get_partition_type(time_column);
	int64 now_time = ts_get_now_internal(time_column);
	int64 end_time, start_time, min_time, max_time = PG_INT64_MAX;
	bool found_new_tuples = false;

	start_time = old_completed_threshold;

	found_new_tuples = hypertable_get_min_and_max_time_value(hypertable,
															 &time_column_name,
															 old_completed_threshold,
															 time_column_type,
															 &min_time,
															 &max_time);
	if (start_time == PG_INT64_MIN)
	{
		/* If there is no completion threshold yet set, use the minimum value stored in the
		 * hypertable */
		if (!found_new_tuples)
		{
			if (verbose)
				elog(LOG,
					 "new materialization range not found for %s.%s (time column %s): no data in "
					 "table",
					 NameStr(*hypertable.schema),
					 NameStr(*hypertable.name),
					 NameStr(time_column_name));
			*materializing_new_range = false;
			return old_completed_threshold;
		}
		start_time = min_time;
	}

	/* check for values which would overflow 64 bit subtractionb */
	if (refresh_lag >= 0 && now_time <= PG_INT64_MIN + refresh_lag)
	{
		if (verbose)
			elog(LOG,
				 "new materialization range not found for %s.%s (time column %s): not enough data "
				 "that satisfies the refresh lag criterion as of %s",
				 NameStr(*hypertable.schema),
				 NameStr(*hypertable.name),
				 NameStr(time_column_name),
				 ts_internal_to_time_string(now_time, time_column_type));
		*materializing_new_range = false;
		return old_completed_threshold;
	}
	else if (refresh_lag < 0 && now_time >= PG_INT64_MAX + refresh_lag)
	{
		/* note since refresh_lag is negative
		 * PG_INT64_MAX + refresh_lag is smaller than PG_INT64_MAX
		 * pick a value so that end_time -= refresh_lag will be
		 * PG_INT64_MAX, we should materialize everything
		 */
		now_time = PG_INT64_MAX + refresh_lag;
	}

	end_time = now_time - refresh_lag;
	end_time = ts_time_bucket_by_type(bucket_width, end_time, time_column_type);

	if (!found_new_tuples || (end_time <= start_time))
	{
		if (verbose)
			elog(LOG,
				 "new materialization range not found for %s.%s (time column %s): "
				 "not enough new data past completion threshold of %s as of %s",
				 NameStr(*hypertable.schema),
				 NameStr(*hypertable.name),
				 NameStr(time_column_name),
				 ts_internal_to_time_string(start_time, time_column_type),
				 ts_internal_to_time_string(now_time, time_column_type));
		*materializing_new_range = false;
		return old_completed_threshold;
	}
	if (max_time < end_time)
	{
		/* end_time is based on now(). But we might not have any new data to materialize.
		 * So limit the end_time to max value from the hypertable's data, adjusted
		 * by time_bucket width parameter (which is a positive value)
		 */
		int64 maxtime_corrected = ts_time_bucket_by_type(bucket_width, max_time, time_column_type);
		if (maxtime_corrected <= max_time)
		{
			/* we need to include max_time in our materialization range as we materialize upto the
			 * end_time but not inclusive. i.e. [start_time , end_time)
			 */
			maxtime_corrected = int64_saturating_add(maxtime_corrected, bucket_width);
		}
		if (maxtime_corrected < end_time)
		{
			end_time = maxtime_corrected;
		}
	}
	/* pin the end time to start_time + max_interval_per_job
	 * so we don't materialize more than max_interval_per_job per run
	 */
	if (end_time - start_time > max_interval_per_job)
	{
		int64 new_end_time;
		new_end_time = ts_time_bucket_by_type(bucket_width,
											  start_time + max_interval_per_job,
											  time_column_type);
		if (verbose)
			elog(LOG,
				 "new materialization range for %s.%s (time column %s) larger than allowed in one "
				 "run, truncating %s "
				 "to %s",
				 NameStr(*hypertable.schema),
				 NameStr(*hypertable.name),
				 NameStr(time_column_name),
				 ts_internal_to_time_string(end_time, time_column_type),
				 ts_internal_to_time_string(new_end_time, time_column_type));
		end_time = new_end_time;
		Assert(end_time > old_completed_threshold);
		*truncated_materialization = true;
	}
	else
		*truncated_materialization = false;

	Assert(end_time > old_completed_threshold);

	*materializing_new_range = true;
	return end_time;
}

/* get min and max value for timestamp column for hypertable */
static bool
hypertable_get_min_and_max_time_value(SchemaAndName hypertable, Name time_column,
									  int64 search_start, Oid time_type, int64 *min_out,
									  int64 *max_out)
{
	Datum last_time_value;
	Datum first_time_value;
	bool fval_is_null, lval_is_null;
	bool search_start_is_infinite = false;
	bool found_new_tuples = false;
	StringInfo command = makeStringInfo();
	int res;

	Datum search_start_val =
		internal_to_time_value_or_infinite(search_start, time_type, &search_start_is_infinite);

	Assert(max_out != NULL);
	Assert(min_out != NULL);
	Assert(time_column != NULL);

	if (search_start_is_infinite && search_start > 0)
	{
		/* the previous completed time was +infinity, there can be no new ranges */
		return false;
	}

	res = SPI_connect();
	if (res != SPI_OK_CONNECT)
		elog(ERROR, "could not connect to SPI while search for new tuples");

	/* We always SELECT both max and min in the following queries. There are two cases
	 * 1. there is no index on time: then we want to perform only one seqscan.
	 * 2. there is a btree index on time: then postgres will transform the query
	 *    into two index-only scans, which should add very little extra work
	 *    compared to the materialization.
	 * Ordered append append also fires, so we never scan beyond the first and last chunks
	 */
	if (search_start_is_infinite)
	{
		/* previous completed time is -infinity, or does not exist, so we must scan from the
		 * beginning */
		appendStringInfo(command,
						 "SELECT max(%s), min(%s) FROM %s.%s",
						 quote_identifier(NameStr(*time_column)),
						 quote_identifier(NameStr(*time_column)),
						 quote_identifier(NameStr(*hypertable.schema)),
						 quote_identifier(NameStr(*hypertable.name)));
	}
	else
	{
		Oid out_fn;
		bool type_is_varlena;
		char *search_start_str;

		getTypeOutputInfo(time_type, &out_fn, &type_is_varlena);

		search_start_str = OidOutputFunctionCall(out_fn, search_start_val);

		*min_out = search_start;
		/* normal case, add a WHERE to take advantage of chunk constraints */
		/*We handled the +infinity case above*/
		Assert(!search_start_is_infinite);
		appendStringInfo(command,
						 "SELECT max(%s), min(%s) FROM %s.%s WHERE %s >= %s",
						 quote_identifier(NameStr(*time_column)),
						 quote_identifier(NameStr(*time_column)),
						 quote_identifier(NameStr(*hypertable.schema)),
						 quote_identifier(NameStr(*hypertable.name)),
						 quote_identifier(NameStr(*time_column)),
						 quote_literal_cstr(search_start_str));
	}

	res = SPI_execute_with_args(command->data,
								0 /*=nargs*/,
								NULL,
								NULL,
								NULL /*=Nulls*/,
								true /*=read_only*/,
								0 /*count*/);
	if (res < 0)
		elog(ERROR, "could not find the minimum/maximum time value for hypertable");

	Assert(SPI_gettypeid(SPI_tuptable->tupdesc, 1) == time_type);
	Assert(SPI_gettypeid(SPI_tuptable->tupdesc, 2) == time_type);

	first_time_value =
		SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &fval_is_null);
	last_time_value = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &lval_is_null);
	Assert(fval_is_null == lval_is_null);

	if (lval_is_null)
	{
		*min_out = PG_INT64_MIN;
		*max_out = PG_INT64_MAX;
	}
	else
	{
		*min_out = ts_time_value_to_internal(first_time_value, time_type);
		*max_out = ts_time_value_to_internal(last_time_value, time_type);
		found_new_tuples = true;
	}

	res = SPI_finish();
	Assert(res == SPI_OK_FINISH);
	return found_new_tuples;
}

/* materialization_invalidation_threshold is used only for materialization_invalidation_log
 table */
typedef struct InvalidationScanState
{
	List **invalidations;
	MemoryContext mctx;
	int64 materialization_invalidation_threshold;
} InvalidationScanState;

static ScanTupleResult
scan_take_invalidation_tuple(TupleInfo *ti, void *data)
{
	InvalidationScanState *scan_state = (InvalidationScanState *) data;

	MemoryContext old_ctx = MemoryContextSwitchTo(scan_state->mctx);
	Form_continuous_aggs_hypertable_invalidation_log invalidation_form =
		((Form_continuous_aggs_hypertable_invalidation_log) GETSTRUCT(ti->tuple));
	Invalidation *invalidation = palloc(sizeof(*invalidation));

	invalidation->modification_time = invalidation_form->modification_time;
	invalidation->lowest_modified_value = invalidation_form->lowest_modified_value;
	invalidation->greatest_modified_value = invalidation_form->greatest_modified_value;

	Assert(invalidation->lowest_modified_value <= invalidation->greatest_modified_value);

	*scan_state->invalidations = lappend(*scan_state->invalidations, invalidation);

	MemoryContextSwitchTo(old_ctx);

	ts_catalog_delete(ti->scanrel, ti->tuple);

	return SCAN_CONTINUE;
}

static void
drain_invalidation_log(int32 raw_hypertable_id, List **invalidations_out)
{
	InvalidationScanState scan_state = {
		.invalidations = invalidations_out,
		.mctx = CurrentMemoryContext,
		.materialization_invalidation_threshold = PG_INT64_MAX,
	};
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_continuous_aggs_hypertable_invalidation_log_idx_hypertable_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(raw_hypertable_id));

	ts_catalog_scan_all(CONTINUOUS_AGGS_HYPERTABLE_INVALIDATION_LOG /*=table*/,
						CONTINUOUS_AGGS_HYPERTABLE_INVALIDATION_LOG_IDX /*=indexid*/
						,
						scankey /*=scankey*/,
						1 /*=num_keys*/,
						scan_take_invalidation_tuple /*=tuple_found*/,
						RowExclusiveLock /*=lockmode*/,
						&scan_state /*=data*/);
}

static InternalTimeRange
materialization_invalidation_log_get_range(int32 materialization_id, Oid type, int64 bucket_width,
										   int64 max_interval_per_job, int64 completed_threshold,
										   int64 invalidate_prior_to_time)
{
	bool found = false;
	InternalTimeRange invalidation_range;
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG,
								AccessShareLock,
								CurrentMemoryContext);
	iterator.ctx.index = catalog_get_index(ts_catalog_get(),
										   CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG,
										   CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG_IDX);
	ts_scan_iterator_scan_key_init(
		&iterator,
		Anum_continuous_aggs_materialization_invalidation_log_idx_materialization_id,
		BTEqualStrategyNumber,
		F_INT4EQ,
		Int32GetDatum(materialization_id));

	/*  note: this scan is in ASC order of lowest_modified_value. This logic  depends on that. */
	ts_scanner_foreach(&iterator)
	{
		Form_continuous_aggs_materialization_invalidation_log invalidation_form =
			((Form_continuous_aggs_materialization_invalidation_log) GETSTRUCT(
				iterator.tinfo->tuple));

		InternalTimeRange entry_range = {
			.type = type,
			.start = ts_time_bucket_by_type(bucket_width,
											invalidation_form->lowest_modified_value,
											type),
			/* add a bucket width to cover the endpoint */
			.end = int64_saturating_add(ts_time_bucket_by_type(bucket_width,
															   invalidation_form
																   ->greatest_modified_value,
															   type),
										bucket_width),
		};

		/* don't process anything starting after the completed threshold; safe to break because
		 * order is lowest_modified_value ASC */
		if (entry_range.start >= completed_threshold)
			break;

		/* clamp the end to the completed threshold; the completion threshold is already bucketed.
		 * This also guarantees
		 * that entry_range.end is bucketed even if it is saturated in the add above. */
		range_clamp_end(&entry_range, completed_threshold, bucket_width);

		/* if invalidate_prior_to_time is set that means invalidations should process buckets cover
		 * all points up to but excluding invalidate_prior_to_time. The last bucket should cover
		 * those points and is allowed to go over. */
		if (invalidate_prior_to_time != PG_INT64_MAX)
		{
			int64 prior_to_time_bucketed =
				ts_time_bucket_by_type(bucket_width, invalidate_prior_to_time, type);
			/* the last bucket should cover prior_to_time */
			if (prior_to_time_bucketed != invalidate_prior_to_time)
				prior_to_time_bucketed = int64_saturating_add(prior_to_time_bucketed, bucket_width);

			Assert(prior_to_time_bucketed >= invalidate_prior_to_time);

			range_clamp_end(&entry_range, prior_to_time_bucketed, bucket_width);
		}

		if (entry_range.end > completed_threshold)
			entry_range.end = completed_threshold;

		if (range_length(entry_range) == 0)
			continue;

		if (!found)
		{
			found = true;
			invalidation_range = entry_range;
			/* force cutting the initial range as well */
			range_expand_right(&invalidation_range,
							   entry_range,
							   max_interval_per_job,
							   bucket_width);
		}
		else
		{
			range_expand_right(&invalidation_range,
							   entry_range,
							   max_interval_per_job,
							   bucket_width);
		}
		found = true;
	}
	ts_scan_iterator_close(&iterator);

	if (!found)
	{
		/* return an empty range */
		invalidation_range = (InternalTimeRange){
			.type = type,
		};
		range_check(invalidation_range, bucket_width);
		return invalidation_range;
	}
	range_check(invalidation_range, bucket_width);
	return invalidation_range;
}

static void
materialization_invalidation_log_delete_or_cut(int32 cagg_id, InternalTimeRange invalidation_range,
											   int64 completed_threshold)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG,
								RowExclusiveLock,
								CurrentMemoryContext);
	iterator.ctx.index = catalog_get_index(ts_catalog_get(),
										   CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG,
										   CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG_IDX);
	ts_scan_iterator_scan_key_init(
		&iterator,
		Anum_continuous_aggs_materialization_invalidation_log_idx_materialization_id,
		BTEqualStrategyNumber,
		F_INT4EQ,
		Int32GetDatum(cagg_id));

	ts_scanner_foreach(&iterator)
	{
		HeapTuple tuple = heap_copytuple(ts_scan_iterator_tuple(&iterator));
		Form_continuous_aggs_materialization_invalidation_log invalidation_form =
			((Form_continuous_aggs_materialization_invalidation_log) GETSTRUCT(tuple));
		bool delete_entry = false;
		bool modify_entry;

		if (invalidation_form->lowest_modified_value >= completed_threshold)
		{
			modify_entry = true;
			delete_entry = true;
		}
		else
		{
			modify_entry =
				range_cut_invalidation_entry_left(invalidation_range,
												  &invalidation_form->lowest_modified_value,
												  &invalidation_form->greatest_modified_value,
												  &delete_entry);
			if (!delete_entry && invalidation_form->greatest_modified_value >= completed_threshold)
			{
				/*
				 * modify the entry if it is higher than the completion threshold
				 * to only go up to the completion threshold. Nothing above completion
				 * has yet been materialized and thus does not need invalidation
				 */
				invalidation_form->greatest_modified_value =
					int64_saturating_sub(completed_threshold, 1);
				modify_entry = true;
				/* delete the entry if the above modification made it length <= 0 */
				delete_entry = (invalidation_form->lowest_modified_value >
								invalidation_form->greatest_modified_value);
			}
		}

		/* don't need to modify the entry so continue; don't break because you may still be able to
		 * delete entries above completion threshold. */
		if (!modify_entry)
			continue;

		if (delete_entry)
			ts_catalog_delete(ts_scan_iterator_tuple_info(&iterator)->scanrel, tuple);
		else
			ts_catalog_update(ts_scan_iterator_tuple_info(&iterator)->scanrel, tuple);
	}
	ts_scan_iterator_close(&iterator);
}

/* copy invalidation logs for the other cont. aggs
 * rel is the relation of the table which has already been opened.
 * the caller opens and closes the relation.
 *
 * We avoid copying over logs that will never be processed because of the
 * `ignore_invalidation_older_than` setting. This is done by applying this
 * setting to the modification_time (current time of the modifying txn). Thus,
 * this setting is logically applied to the time of modification as opposed
 * to the current time during materialization. This is semantics we want to
 * expose to users. So for example if we are doing an insert on Dec 4 of ROW 1
 * with values( 'Nov 30',  .... ) and ROW 2 with values( 'Sep 30', ...) and the
 * ignore_invalidation_older_than setting is 30 days, then ROW 1 will cause an
 * invalidation (Dec 4 - Nov 30) <= 30 days and ROW 2 will not, independent of
 * when we materialize.
 */
static void
insert_materialization_invalidation_logs(List *caggs, List *invalidations,
										 Relation materialization_invalidation_log_rel)
{
	TupleDesc desc;
	Datum values[Natts_continuous_aggs_materialization_invalidation_log];
	CatalogSecurityContext sec_ctx;
	bool nulls[Natts_continuous_aggs_materialization_invalidation_log] = { false };
	ListCell *lc, *lc2;
	if (caggs == NIL)
		return;

	desc = RelationGetDescr(materialization_invalidation_log_rel);
	foreach (lc, caggs)
	{
		ContinuousAgg *ca = lfirst(lc);
		int32 cagg_id = ca->data.mat_hypertable_id;
		int64 ignore_invalidation_older_than = ca->data.ignore_invalidation_older_than;
		foreach (lc2, invalidations)
		{
			Invalidation *entry = (Invalidation *) lfirst(lc2);
			int64 lowest_modified_value = entry->lowest_modified_value;
			int64 minimum_invalidation_time =
				ts_continuous_aggs_get_minimum_invalidation_time(entry->modification_time,
																 ignore_invalidation_older_than);
			if (entry->greatest_modified_value < minimum_invalidation_time)
				continue;

			if (entry->lowest_modified_value < minimum_invalidation_time)
				lowest_modified_value = minimum_invalidation_time;

			values[AttrNumberGetAttrOffset(
				Anum_continuous_aggs_materialization_invalidation_log_materialization_id)] =
				ObjectIdGetDatum(cagg_id);
			values[AttrNumberGetAttrOffset(
				Anum_continuous_aggs_materialization_invalidation_log_modification_time)] =
				Int64GetDatum(entry->modification_time);
			values[AttrNumberGetAttrOffset(
				Anum_continuous_aggs_materialization_invalidation_log_lowest_modified_value)] =
				Int64GetDatum(lowest_modified_value);
			values[AttrNumberGetAttrOffset(
				Anum_continuous_aggs_materialization_invalidation_log_greatest_modified_value)] =
				Int64GetDatum(entry->greatest_modified_value);
			ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
			ts_catalog_insert_values(materialization_invalidation_log_rel, desc, values, nulls);
			ts_catalog_restore_user(&sec_ctx);
		}
	}
}

/***************************
 * materialization support *
 ***************************/

static void update_materializations(SchemaAndName partial_view, SchemaAndName materialization_table,
									Name time_column_name,
									InternalTimeRange new_materialization_range, int64 bucket_width,
									InternalTimeRange invalidation_range);
static void spi_update_materializations(SchemaAndName partial_view,
										SchemaAndName materialization_table, Name time_column_name,
										TimeRange invalidation_range);
static void spi_delete_materializations(SchemaAndName materialization_table, Name time_column_name,
										TimeRange invalidation_range);
static void spi_insert_materializations(SchemaAndName partial_view,
										SchemaAndName materialization_table, Name time_column_name,
										TimeRange materialization_range);
static void continuous_aggs_completed_threshold_set(int32 materialization_id,
													int64 old_completed_threshold);

/* execute the materialization for the continuous aggregate materialization_id
 * we will re-materialize all values within
 *     [lowest_invalidated_value, greatest_invalidated_value]
 * and materialize for the first time all rows within
 *     [completed threshold, invalidation threshold]
 * thus the materialization worker should update the invalidation threshold before
 * this function is called if it wants new rows to be updated.
 *
 * This function does _not_ start it's own transactions, in order to allow it to
 * be called from SQL for testing purposes; the materialization worker should
 * start a new transaction before calling this function.
 *
 * args:
 *     hypertable_id: the id of the hypertable that owns this continuous aggregate
 *     materialization_id: the mat_hypertable_id of the continuous aggregate
 *     lowest_invalidated_value: the lowest time to be re-materialized
 *     greatest_invalidated_value: the greatest time to be re-materialized
 */
void
continuous_agg_execute_materialization(int64 bucket_width, int32 hypertable_id,
									   int32 materialization_id, SchemaAndName partial_view,
									   int64 invalidation_range_start, int64 invalidation_range_end,
									   int64 materialization_invalidation_threshold)
{
	CatalogSecurityContext sec_ctx;
	SchemaAndName materialization_table_name;
	Cache *hcache = ts_hypertable_cache_pin();
	Hypertable *raw_table = ts_hypertable_cache_get_entry_by_id(hcache, hypertable_id);

	if (raw_table == NULL)
		elog(ERROR, "can only materialize continuous aggregates on a hypertable");

	Oid time_col_type =
		ts_dimension_get_partition_type(hyperspace_get_open_dimension(raw_table->space, 0));
	InternalTimeRange new_materialization_range = {
		.type = time_col_type,
		.start = ts_continuous_agg_get_completed_threshold(materialization_id),
		.end = materialization_invalidation_threshold,
	};
	InternalTimeRange new_invalidation_range = {
		.type = time_col_type,
		.start = invalidation_range_start,
		.end = invalidation_range_end,
	};
	Hypertable *materialization_table =
		ts_hypertable_cache_get_entry_by_id(hcache, materialization_id);
	NameData time_column_name;

	range_check(new_invalidation_range, bucket_width);
	range_check(new_materialization_range, bucket_width);

	if (materialization_table == NULL)
		elog(ERROR, "can only materialize continuous aggregates to a hypertable");

	/* The materialization table always stores our internal representation of time values, so get
	 * the real type of the time column from the original hypertable */
	new_materialization_range.type =
		ts_dimension_get_partition_type(hyperspace_get_open_dimension(raw_table->space, 0));

	time_column_name =
		hyperspace_get_open_dimension(materialization_table->space, 0)->fd.column_name;

	if (new_materialization_range.start > PG_INT64_MIN)
		Assert(new_materialization_range.start ==
			   ts_time_bucket_by_type(bucket_width,
									  new_materialization_range.start,
									  new_materialization_range.type));

	/* Since we store the invalidation threshold as an exclusive range (we invalidate any rows <
	 * invalidation_threshold) invalidations for rows containing INT64_MAX, the maximum values for
	 * that threshold, will never be processed. Since we expect it to be rare that such a row is
	 * needed, we avoid this by pinning the materialization end, and thus the invalidation threshold
	 * to be at most one time_bucket before INT64_MAX. Since we never materialize that last bucket,
	 * it never needs to be invalidated, and the invalidations can't be lost. */
	if (new_materialization_range.end < PG_INT64_MAX)
		Assert(new_materialization_range.end ==
			   ts_time_bucket_by_type(bucket_width,
									  new_materialization_range.end,
									  new_materialization_range.type));
	else
		new_materialization_range.end = ts_time_bucket_by_type(bucket_width,
															   new_materialization_range.end,
															   new_materialization_range.type);

	materialization_table_name.schema = &materialization_table->fd.schema_name;
	materialization_table_name.name = &materialization_table->fd.table_name;

	/* lock the table's we will be inserting to now, to prevent deadlocks later on */
	// TODO lock the materialization table, the and the completed_threshold with RowExclusive locks
	// TODO decide lock ordering

	update_materializations(partial_view,
							materialization_table_name,
							&time_column_name,
							new_materialization_range,
							bucket_width,
							new_invalidation_range);

	/* update the completed watermark */
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);

	continuous_aggs_completed_threshold_set(materialization_id, new_materialization_range.end);

	ts_catalog_restore_user(&sec_ctx);
	ts_cache_release(hcache);
	return;
}

static ScanTupleResult
scan_update_invalidation_threshold(TupleInfo *ti, void *data)
{
	int64 new_threshold = *(int64 *) data;
	HeapTuple new_tuple = heap_copytuple(ti->tuple);
	Form_continuous_aggs_invalidation_threshold form =
		(Form_continuous_aggs_invalidation_threshold) GETSTRUCT(new_tuple);
	if (new_threshold > form->watermark)
	{
		form->watermark = new_threshold;
		ts_catalog_update(ti->scanrel, new_tuple);
	}
	else
	{
		elog(DEBUG1,
			 "hypertable %d existing  watermark >= new invalidation threshold " INT64_FORMAT
			 " " INT64_FORMAT,
			 form->hypertable_id,
			 form->watermark,
			 new_threshold);
	}
	return SCAN_DONE;
}

/* every cont. agg calculates its invalidation_threshold point based on its
 *refresh_lag etc. We update the raw hypertable's invalidation threshold
 * only if this new value is greater than the existsing one.
 */
static void
invalidation_threshold_set(int32 raw_hypertable_id, int64 invalidation_threshold)
{
	bool updated_threshold;
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_continuous_aggs_invalidation_threshold_pkey_hypertable_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(raw_hypertable_id));

	/* NOTE: this function deliberately takes an AccessExclusiveLock when updating the invalidation
	 * threshold, instead of the weaker RowExclusiveLock lock normally held for such operations: in
	 * order to ensure we do not lose invalidations from concurrent mutations, we must ensure that
	 * all transactions which read the invalidation threshold have either completed, or not yet read
	 * the value; if we used a RowExclusiveLock we could race such a transaction and update the
	 * threshold between the time it is read but before the other transaction commits. This would
	 * cause us to lose the updates. The AccessExclusiveLock ensures no one else can possibly be
	 * reading the threshold.
	 */
	updated_threshold =
		ts_catalog_scan_one(CONTINUOUS_AGGS_INVALIDATION_THRESHOLD /*=table*/,
							CONTINUOUS_AGGS_INVALIDATION_THRESHOLD_PKEY /*=indexid*/,
							scankey /*=scankey*/,
							1 /*=num_keys*/,
							scan_update_invalidation_threshold /*=tuple_found*/,
							AccessExclusiveLock /*=lockmode*/,
							CONTINUOUS_AGGS_INVALIDATION_THRESHOLD_TABLE_NAME /*=table_name*/,
							&invalidation_threshold /*=data*/);

	if (!updated_threshold)
	{
		Catalog *catalog = ts_catalog_get();
		/* NOTE: this function deliberately takes a stronger lock than RowExclusive, see the comment
		 * above for the rationale
		 */
		Relation rel =
			table_open(catalog_get_table_id(catalog, CONTINUOUS_AGGS_INVALIDATION_THRESHOLD),
					   AccessExclusiveLock);
		TupleDesc desc = RelationGetDescr(rel);
		Datum values[Natts_continuous_aggs_invalidation_threshold];
		bool nulls[Natts_continuous_aggs_invalidation_threshold] = { false };

		values[AttrNumberGetAttrOffset(Anum_continuous_aggs_invalidation_threshold_hypertable_id)] =
			Int32GetDatum(raw_hypertable_id);
		values[AttrNumberGetAttrOffset(Anum_continuous_aggs_invalidation_threshold_watermark)] =
			Int64GetDatum(invalidation_threshold);

		ts_catalog_insert_values(rel, desc, values, nulls);
		table_close(rel, NoLock);
	}
}

static ScanTupleResult
invalidation_threshold_tuple_found(TupleInfo *ti, void *data)
{
	int64 *threshold = data;
	*threshold = ((Form_continuous_aggs_invalidation_threshold) GETSTRUCT(ti->tuple))->watermark;
	return SCAN_CONTINUE;
}

int64
invalidation_threshold_get(int32 hypertable_id)
{
	int64 threshold = 0;
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_continuous_aggs_invalidation_threshold_pkey_hypertable_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(hypertable_id));

	if (!ts_catalog_scan_one(CONTINUOUS_AGGS_INVALIDATION_THRESHOLD /*=table*/,
							 CONTINUOUS_AGGS_INVALIDATION_THRESHOLD_PKEY /*=indexid*/,
							 scankey /*=scankey*/,
							 1 /*=num_keys*/,
							 invalidation_threshold_tuple_found /*=tuple_found*/,
							 AccessShareLock /*=lockmode*/,
							 CONTINUOUS_AGGS_INVALIDATION_THRESHOLD_TABLE_NAME /*=table_name*/,
							 &threshold /*=data*/))
		elog(ERROR, "could not find invalidation threshold for hypertable %d", hypertable_id);

	return threshold;
}

void
update_materializations(SchemaAndName partial_view, SchemaAndName materialization_table,
						Name time_column_name, InternalTimeRange new_materialization_range,
						int64 bucket_width, InternalTimeRange invalidation_range)
{
	InternalTimeRange combined_materialization_range = new_materialization_range;
	bool materialize_invalidations_separately = range_length(invalidation_range) > 0;
	int res = SPI_connect();
	if (res != SPI_OK_CONNECT)
		elog(ERROR, "could not connect to SPI in materializer");
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
		spi_update_materializations(partial_view,
									materialization_table,
									time_column_name,
									internal_time_range_to_time_range(
										combined_materialization_range));
	}
	else
	{
		spi_update_materializations(partial_view,
									materialization_table,
									time_column_name,
									internal_time_range_to_time_range(invalidation_range));

		spi_update_materializations(partial_view,
									materialization_table,
									time_column_name,
									internal_time_range_to_time_range(new_materialization_range));
	}

	res = SPI_finish();
	Assert(res == SPI_OK_FINISH);
	return;
}

static bool
ranges_overlap(InternalTimeRange invalidation_range, InternalTimeRange new_materialization_range)
{
	Assert(invalidation_range.start <= invalidation_range.end);
	Assert(new_materialization_range.start <= new_materialization_range.end);
	return !(invalidation_range.end < new_materialization_range.start ||
			 new_materialization_range.end < invalidation_range.start);
}

/* Checks to ensure a valid length */
static void
range_check(const InternalTimeRange range, int64 bucket_width)
{
	if (range.start > range.end)
		elog(ERROR, "internal error: range start > end");

	/* empty range does not need to be time-bucketed */
	if (range_length(range) == 0)
		return;

	if (range.start != PG_INT64_MIN &&
		range.start != ts_time_bucket_by_type(bucket_width, range.start, range.type))
		elog(ERROR, "internal error: range start not aligned on bucket boundary");

	if (range.end != ts_time_bucket_by_type(bucket_width, range.end, range.type))
		elog(ERROR, "internal error: range end not aligned on bucket boundary");
}

static int64
range_length(const InternalTimeRange range)
{
	Assert(range.end >= range.start);

	return int64_saturating_sub(range.end, range.start);
}

/* Expand the range `range` to the right so that it covers as much of
 * `to_cover` as possible while still obeying `max_length` */
static bool
range_expand_right(InternalTimeRange *range, const InternalTimeRange to_cover, int64 max_length,
				   int64 bucket_width)
{
	int64 max_end = ts_time_bucket_by_type(bucket_width,
										   int64_saturating_add(range->start, max_length),
										   range->type);

	Assert(range->start <= to_cover.start);
	if (max_end > to_cover.start)
	{
		range->end = int64_min(to_cover.end, max_end);
		Assert(range_length(*range) <= max_length);
		return true;
	}
	return false;
}

static void
range_clamp_end(InternalTimeRange *range, int64 clamp_end, int64 bucket_width)
{
	if (clamp_end == PG_INT64_MAX)
		return;

	/* the end value has to be bucketed, we don't bucket here since sometimes you want to round down
	 * or round up and the caller should decide. */
	Assert(clamp_end == ts_time_bucket_by_type(bucket_width, clamp_end, range->type));
	if (clamp_end < range->start)
	{
		/* make an empty range */
		range->start = clamp_end;
		range->end = clamp_end;
		Assert(range_length(*range) == 0);
	}
	else if (range->end > clamp_end)
	{
		range->end = clamp_end;
	}
	range_check(*range, bucket_width);
}

static bool
range_cut_invalidation_entry_left(const InternalTimeRange invalidation_range_knife,
								  int64 *lowest_modified_value, int64 *greatest_modified_value,
								  bool *delete_entry_out)
{
	if (invalidation_range_knife.start > *lowest_modified_value)
		elog(ERROR, "internal error: cannot cut invalidation entry from the right");

	if (invalidation_range_knife.end <= *lowest_modified_value)
		return false;

	if (invalidation_range_knife.end > *greatest_modified_value)
	{
		*delete_entry_out = true;
		return true;
	}
	*delete_entry_out = false;
	*lowest_modified_value = invalidation_range_knife.end;
	return true;
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

static void
spi_update_materializations(SchemaAndName partial_view, SchemaAndName materialization_table,
							Name time_column_name, TimeRange invalidation_range)
{
	spi_delete_materializations(materialization_table, time_column_name, invalidation_range);
	spi_insert_materializations(partial_view,
								materialization_table,
								time_column_name,
								invalidation_range);
}

static void
spi_delete_materializations(SchemaAndName materialization_table, Name time_column_name,
							TimeRange invalidation_range)
{
	int res;
	StringInfo command = makeStringInfo();
	Oid out_fn;
	bool type_is_varlena;
	char *invalidation_start;
	char *invalidation_end;

	getTypeOutputInfo(invalidation_range.type, &out_fn, &type_is_varlena);

	invalidation_start = OidOutputFunctionCall(out_fn, invalidation_range.start);
	invalidation_end = OidOutputFunctionCall(out_fn, invalidation_range.end);

	appendStringInfo(command,
					 "DELETE FROM %s.%s AS D WHERE "
					 "D.%s >= %s AND D.%s < %s;",
					 quote_identifier(NameStr(*materialization_table.schema)),
					 quote_identifier(NameStr(*materialization_table.name)),
					 quote_identifier(NameStr(*time_column_name)),
					 quote_literal_cstr(invalidation_start),
					 quote_identifier(NameStr(*time_column_name)),
					 quote_literal_cstr(invalidation_end));

	res = SPI_execute_with_args(command->data,
								0 /*=nargs*/,
								NULL,
								NULL,
								NULL /*=Nulls*/,
								false /*=read_only*/,
								0 /*count*/);
	if (res < 0)
		elog(ERROR, "could not delete old values from materialization table");
}

static void
spi_insert_materializations(SchemaAndName partial_view, SchemaAndName materialization_table,
							Name time_column_name, TimeRange materialization_range)
{
	int res;
	StringInfo command = makeStringInfo();
	Oid out_fn;
	bool type_is_varlena;
	char *materialization_start;
	char *materialization_end;

	getTypeOutputInfo(materialization_range.type, &out_fn, &type_is_varlena);

	materialization_start = OidOutputFunctionCall(out_fn, materialization_range.start);
	materialization_end = OidOutputFunctionCall(out_fn, materialization_range.end);

	appendStringInfo(command,
					 "INSERT INTO %s.%s SELECT * FROM %s.%s AS I "
					 "WHERE I.%s >= %s AND I.%s < %s;",
					 quote_identifier(NameStr(*materialization_table.schema)),
					 quote_identifier(NameStr(*materialization_table.name)),
					 quote_identifier(NameStr(*partial_view.schema)),
					 quote_identifier(NameStr(*partial_view.name)),
					 quote_identifier(NameStr(*time_column_name)),
					 quote_literal_cstr(materialization_start),
					 quote_identifier(NameStr(*time_column_name)),
					 quote_literal_cstr(materialization_end));

	res = SPI_execute_with_args(command->data,
								0 /*=nargs*/,
								NULL /*=argtypes*/,
								NULL /*=Values*/,
								NULL /*=Nulls*/,
								false /*=read_only*/,
								0 /*count*/
	);
	if (res < 0)
		elog(ERROR, "could materialize values into the materialization table");
}

static ScanTupleResult
scan_update_completed_threshold(TupleInfo *ti, void *data)
{
	int64 new_threshold = *(int64 *) data;
	HeapTuple new_tuple = heap_copytuple(ti->tuple);
	Form_continuous_aggs_completed_threshold form =
		(Form_continuous_aggs_completed_threshold) GETSTRUCT(new_tuple);
	if (form->watermark > new_threshold)
		elog(ERROR, "Internal Error: new completion threshold must not be less than the old one");
	form->watermark = new_threshold;
	ts_catalog_update(ti->scanrel, new_tuple);
	return SCAN_DONE;
}

/* everything up to (but not including) the completion threshold has been materialized */
static void
continuous_aggs_completed_threshold_set(int32 materialization_id, int64 completed_threshold)
{
	bool updated_threshold;
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_continuous_aggs_completed_threshold_pkey_materialization_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(materialization_id));

	updated_threshold =
		ts_catalog_scan_one(CONTINUOUS_AGGS_COMPLETED_THRESHOLD /*=table*/,
							CONTINUOUS_AGGS_COMPLETED_THRESHOLD_PKEY /*=indexid*/,
							scankey /*=scankey*/,
							1 /*=num_keys*/,
							scan_update_completed_threshold /*=tuple_found*/,
							RowExclusiveLock /*=lockmode*/,
							CONTINUOUS_AGGS_COMPLETED_THRESHOLD_TABLE_NAME /*=table_name*/,
							&completed_threshold /*=data*/);

	if (!updated_threshold)
	{
		Catalog *catalog = ts_catalog_get();
		Relation rel =
			table_open(catalog_get_table_id(catalog, CONTINUOUS_AGGS_COMPLETED_THRESHOLD),
					   RowExclusiveLock);
		TupleDesc desc = RelationGetDescr(rel);
		Datum values[Natts_continuous_aggs_completed_threshold];
		bool nulls[Natts_continuous_aggs_completed_threshold] = { false };

		values[AttrNumberGetAttrOffset(
			Anum_continuous_aggs_completed_threshold_materialization_id)] =
			Int32GetDatum(materialization_id);
		values[AttrNumberGetAttrOffset(Anum_continuous_aggs_completed_threshold_watermark)] =
			Int64GetDatum(completed_threshold);

		ts_catalog_insert_values(rel, desc, values, nulls);
		table_close(rel, NoLock);
	}
}
