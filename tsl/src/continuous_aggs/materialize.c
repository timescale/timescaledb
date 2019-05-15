
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
#include <miscadmin.h>
#include <utils/hsearch.h>
#include <storage/lmgr.h>
#include <utils/builtins.h>
#include <utils/rel.h>
#include <utils/relcache.h>
#include <utils/date.h>
#include <utils/snapmgr.h>

#include <scanner.h>
#include <compat.h>

#include "chunk.h"
#include "dimension.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "export.h"
#include "partitioning.h"

#include "utils.h"
#include "time_bucket.h"

#include "continuous_aggs/materialize.h"

/***********************
 * materialization job *
 ***********************/

static Form_continuous_agg get_continuous_agg(int32 mat_hypertable_id);
static int64 get_materialization_end_point_for_table(int32 raw_hypertable_id,
													 int32 materialization_id, int64 refresh_lag,
													 int64 bucket_width, int64 max_interval_per_job,
													 bool *materializing_new_range,
													 bool *truncated_materialization, bool verbose);
static int64 invalidation_threshold_get(int32 materialization_id);
static void drain_invalidation_log(int32 raw_hypertable_id, List **invalidations_out);
static void invalidation_threshold_set(int32 raw_hypertable_id, int64 invalidation_threshold);
static int64 continuous_aggs_completed_threshold_get(int32 materialization_id);
static Datum internal_to_time_value_or_infinite(int64 internal, Oid time_type,
												bool *is_infinite_out);

/* must be called without a transaction started */
bool
continuous_agg_materialize(int32 materialization_id, bool verbose)
{
	Hypertable *raw_hypertable;
	Oid raw_table_oid;
	Relation raw_table_relation;
	Hypertable *materialization_table;
	Oid materialization_table_oid;
	Relation materialization_table_relation;
	Oid partial_view_oid;
	Relation partial_view_relation;
	LockRelId raw_lock_relid;
	LockRelId materialization_lock_relid;
	LockRelId partial_view_lock_relid;
	Form_continuous_agg cagg;
	FormData_continuous_agg cagg_data;
	int64 new_invalidation_threshold;
	bool materializing_new_range = false;
	bool truncated_materialization = false;
	List *invalidations = NIL;
	SchemaAndName partial_view;

	/*
	 * Transaction 1: discover the new range in the raw table we will materialize
	 *                and update the invalidation threshold to it, if needed
	 */
	StartTransactionCommand();
	/* we need a snapshot for the SPI commands in get_materialization_end_point_for_table */
	PushActiveSnapshot(GetTransactionSnapshot());

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
	partial_view_lock_relid = materialization_table_relation->rd_lockInfo.lockRelId;
	LockRelationIdForSession(&partial_view_lock_relid, ShareRowExclusiveLock);
	relation_close(partial_view_relation, NoLock);

	new_invalidation_threshold =
		get_materialization_end_point_for_table(cagg_data.raw_hypertable_id,
												materialization_id,
												cagg_data.refresh_lag,
												cagg_data.bucket_width,
												cagg_data.max_interval_per_job,
												&materializing_new_range,
												&truncated_materialization,
												verbose);

	if (verbose)
	{
		if (materializing_new_range)
			elog(INFO,
				 "materializing continuous aggregate %s.%s: new range up to " INT64_FORMAT,
				 NameStr(cagg_data.user_view_schema),
				 NameStr(cagg_data.user_view_name),
				 new_invalidation_threshold);
		else
			elog(INFO,
				 "materializing continuous aggregate %s.%s: no new range to materialize",
				 NameStr(cagg_data.user_view_schema),
				 NameStr(cagg_data.user_view_name));
	}

	if (materializing_new_range)
		invalidation_threshold_set(cagg_data.raw_hypertable_id, new_invalidation_threshold);

	PopActiveSnapshot();
	CommitTransactionCommand();

	/*
	 * Transaction 2: get the values from the invalidation table,
	 *                run the materialization, and update completed_threshold
	 * (eventually this will only read per-cagg log, and a different worker will
	 *  move from the hypertable log to the per-cagg log)
	 */
	StartTransactionCommand();
	/* we need a snapshot for the SPI commands within continuous_agg_execute_materialization */
	PushActiveSnapshot(GetTransactionSnapshot());

	drain_invalidation_log(cagg_data.raw_hypertable_id, &invalidations);

	/* if there's nothing to materialize, don't bother with the rest */
	if (!materializing_new_range && list_length(invalidations) == 0)
	{
		if (verbose)
			elog(INFO,
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

	continuous_agg_execute_materialization(cagg_data.bucket_width,
										   cagg_data.raw_hypertable_id,
										   cagg_data.mat_hypertable_id,
										   partial_view,
										   invalidations);

finish:
	UnlockRelationIdForSession(&partial_view_lock_relid, ShareRowExclusiveLock);
	UnlockRelationIdForSession(&materialization_lock_relid, ShareRowExclusiveLock);
	UnlockRelationIdForSession(&raw_lock_relid, AccessShareLock);
	PopActiveSnapshot();
	CommitTransactionCommand();
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

static bool hypertable_get_min_and_max(SchemaAndName hypertable, Name time_column,
									   int64 search_start, Oid time_type, int64 *min_out,
									   int64 *max_out);

static int64
get_materialization_end_point_for_table(int32 raw_hypertable_id, int32 materialization_id,
										int64 refresh_lag, int64 bucket_width,
										int64 max_interval_per_job, bool *materializing_new_range,
										bool *truncated_materialization, bool verbose)
{
	int64 start_time = PG_INT64_MIN;
	int64 end_time = PG_INT64_MIN;
	Hypertable *raw_table = ts_hypertable_get_by_id(raw_hypertable_id);
	SchemaAndName hypertable = {
		.schema = &raw_table->fd.schema_name,
		.name = &raw_table->fd.table_name,
	};
	int64 old_completed_threshold = continuous_aggs_completed_threshold_get(materialization_id);
	Dimension *time_column = hyperspace_get_open_dimension(raw_table->space, 0);
	NameData time_column_name = time_column->fd.column_name;
	Oid time_column_type = time_column->fd.column_type;
	bool found_new_tuples = false;

	found_new_tuples = hypertable_get_min_and_max(hypertable,
												  &time_column_name,
												  old_completed_threshold,
												  time_column_type,
												  &start_time,
												  &end_time);

	if (!found_new_tuples)
	{
		if (verbose)
			elog(INFO,
				 "new materialization range not found for %s.%s (time column %s): no new data",
				 NameStr(*hypertable.schema),
				 NameStr(*hypertable.name),
				 NameStr(time_column_name));
		*materializing_new_range = false;
		return old_completed_threshold;
	}

	Assert(end_time >= start_time);

	/* check for values which would overflow 64 bit subtraction*/
	if (refresh_lag >= 0 && end_time <= PG_INT64_MIN + refresh_lag)
	{
		if (verbose)
			elog(INFO,
				 "new materialization range not found for %s.%s (time column %s): not enough data "
				 "in table (" INT64_FORMAT ")",
				 NameStr(*hypertable.schema),
				 NameStr(*hypertable.name),
				 NameStr(time_column_name),
				 end_time);
		*materializing_new_range = false;
		return old_completed_threshold;
	}
	else if (refresh_lag < 0 && end_time >= PG_INT64_MAX + refresh_lag)
	{
		/* note since refresh_lag is negative
		 * PG_INT64_MAX + refresh_lag is smaller than PG_INT64_MAX
		 * pick a value so that end_time -= refresh_lag will be
		 * PG_INT64_MAX, we should materialize everything
		 */
		end_time = PG_INT64_MAX + refresh_lag;
	}

	end_time -= refresh_lag;
	end_time = ts_time_bucket_by_type(bucket_width, end_time, time_column_type);
	if (end_time <= old_completed_threshold || end_time < start_time)
	{
		if (verbose)
			elog(INFO,
				 "new materialization range not found for %s.%s (time column %s): "
				 "not enough new data past completion threshold (" INT64_FORMAT ")",
				 NameStr(*hypertable.schema),
				 NameStr(*hypertable.name),
				 NameStr(time_column_name),
				 end_time);
		*materializing_new_range = false;
		return old_completed_threshold;
	}

	/* pin the end time to start_time + max_interval_per_job
	 * so we don't materialize more than max_interval_per_job per run
	 */
	if (end_time - start_time > max_interval_per_job)
	{
		if (verbose)
			elog(INFO,
				 "new materialization range for %s.%s larger than allowed in one run, truncating "
				 "(time column %s) (" INT64_FORMAT ")",
				 NameStr(*hypertable.schema),
				 NameStr(*hypertable.name),
				 NameStr(time_column_name),
				 end_time);
		end_time = ts_time_bucket_by_type(bucket_width,
										  start_time + max_interval_per_job,
										  time_column_type);
		Assert(end_time > old_completed_threshold);
		*truncated_materialization = true;
	}
	else
		*truncated_materialization = false;

	if (verbose)
		elog(INFO,
			 "new materialization range for %s.%s (time column %s) (" INT64_FORMAT ")",
			 NameStr(*hypertable.schema),
			 NameStr(*hypertable.name),
			 NameStr(time_column_name),
			 end_time);

	Assert(end_time > old_completed_threshold);
	Assert(end_time >= start_time);

	*materializing_new_range = true;
	return end_time;
}

static bool
hypertable_get_min_and_max(SchemaAndName hypertable, Name time_column, int64 search_start,
						   Oid time_type, int64 *min_out, int64 *max_out)
{
	Datum last_time_value;
	Datum first_time_value;
	bool val_is_null;
	bool search_start_is_infinite = false;
	bool found_new_tuples = false;
	StringInfo command = makeStringInfo();
	int res;

	Datum search_start_val =
		internal_to_time_value_or_infinite(search_start, time_type, &search_start_is_infinite);

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
		elog(ERROR, "could not find new invalidation threshold");

	Assert(SPI_gettypeid(SPI_tuptable->tupdesc, 1) == time_type);
	Assert(SPI_gettypeid(SPI_tuptable->tupdesc, 2) == time_type);

	first_time_value = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &val_is_null);
	last_time_value = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &val_is_null);

	if (!val_is_null)
	{
		*min_out = ts_time_value_to_internal(first_time_value, time_type);
		*max_out = ts_time_value_to_internal(last_time_value, time_type);
		found_new_tuples = true;
	}

	res = SPI_finish();
	Assert(res == SPI_OK_FINISH);
	return found_new_tuples;
}

typedef struct InvalidationScanState
{
	List **invalidations;
	MemoryContext mctx;
} InvalidationScanState;

static ScanTupleResult
scan_take_invalidation_tuple(TupleInfo *ti, void *data)
{
	InvalidationScanState *scan_state = (InvalidationScanState *) data;

	MemoryContext old_ctx = MemoryContextSwitchTo(scan_state->mctx);
	Form_continuous_aggs_hypertable_invalidation_log invalidation_form =
		((Form_continuous_aggs_hypertable_invalidation_log) GETSTRUCT(ti->tuple));
	Invalidation *invalidation = palloc(sizeof(*invalidation));

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

/***************************
 * materialization support *
 ***************************/

typedef struct InternalTimeRange
{
	Oid type;
	int64 start;
	int64 end;
} InternalTimeRange;

typedef struct TimeRange
{
	Oid type;
	Datum start;
	Datum end;
} TimeRange;

static int64 invalidation_threshold_get(int32 materialization_id);
static int64 completed_threshold_get(int32 materialization_id);
static void update_materializations(SchemaAndName partial_view, SchemaAndName materialization_table,
									Name time_column_name,
									InternalTimeRange new_materialization_range, int64 bucket_width,
									List *invalidations);
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
static bool get_invalidation_range(List *invalidations, InternalTimeRange *invalidation_range);
static void time_bucket_range(InternalTimeRange *range, int64 bucket_width);
static bool ranges_overlap(InternalTimeRange invalidation_range,
						   InternalTimeRange new_materialization_range);
static TimeRange internal_time_range_to_time_range(InternalTimeRange internal);

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
									   List *invalidations)
{
	CatalogSecurityContext sec_ctx;
	SchemaAndName materialization_table_name;
	InternalTimeRange new_materialization_range = {
		.start = completed_threshold_get(materialization_id),
		.end = invalidation_threshold_get(hypertable_id),
	};
	Cache *hcache = ts_hypertable_cache_pin();
	Hypertable *raw_table = ts_hypertable_cache_get_entry_by_id(hcache, hypertable_id);
	Hypertable *materialization_table =
		ts_hypertable_cache_get_entry_by_id(hcache, materialization_id);
	NameData time_column_name;

	Assert(new_materialization_range.start <= new_materialization_range.end);

	if (raw_table == NULL)
		elog(ERROR, "can only materialize continuous aggregates on a hypertable");

	if (materialization_table == NULL)
		elog(ERROR, "can only materialize continuous aggregates to a hypertable");

	/* The materialization table always stores our internal representation of time values, so get
	 * the real type of the time column from the original hypertable */
	new_materialization_range.type =
		hyperspace_get_open_dimension(raw_table->space, 0)->fd.column_type;

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
							invalidations);

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
	if (form->watermark > new_threshold)
		elog(ERROR, "Internal Error: new invalidation threshold must not be less than the old one");
	form->watermark = new_threshold;
	ts_catalog_update(ti->scanrel, new_tuple);
	return SCAN_DONE;
}

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
			heap_open(catalog_get_table_id(catalog, CONTINUOUS_AGGS_INVALIDATION_THRESHOLD),
					  AccessExclusiveLock);
		TupleDesc desc = RelationGetDescr(rel);
		Datum values[Natts_continuous_aggs_invalidation_threshold];
		bool nulls[Natts_continuous_aggs_invalidation_threshold] = { false };

		values[AttrNumberGetAttrOffset(Anum_continuous_aggs_invalidation_threshold_hypertable_id)] =
			Int32GetDatum(raw_hypertable_id);
		values[AttrNumberGetAttrOffset(Anum_continuous_aggs_invalidation_threshold_watermark)] =
			Int64GetDatum(invalidation_threshold);

		ts_catalog_insert_values(rel, desc, values, nulls);
		relation_close(rel, NoLock);
	}
}

static ScanTupleResult
invalidation_threshold_tuple_found(TupleInfo *ti, void *data)
{
	int64 *threshold = data;
	*threshold = ((Form_continuous_aggs_invalidation_threshold) GETSTRUCT(ti->tuple))->watermark;
	return SCAN_CONTINUE;
}

static int64
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

static ScanTupleResult
completed_threshold_tuple_found(TupleInfo *ti, void *data)
{
	int64 *threshold = data;
	*threshold = ((Form_continuous_aggs_completed_threshold) GETSTRUCT(ti->tuple))->watermark;
	return SCAN_CONTINUE;
}

static int64
completed_threshold_get(int32 materialization_id)
{
	int64 threshold = 0;
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_continuous_aggs_completed_threshold_pkey_materialization_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(materialization_id));

	if (!ts_catalog_scan_one(CONTINUOUS_AGGS_COMPLETED_THRESHOLD /*=table*/,
							 CONTINUOUS_AGGS_COMPLETED_THRESHOLD_PKEY /*=indexid*/,
							 scankey /*=scankey*/,
							 1 /*=num_keys*/,
							 completed_threshold_tuple_found /*=tuple_found*/,
							 AccessShareLock /*=lockmode*/,
							 CONTINUOUS_AGGS_COMPLETED_THRESHOLD_TABLE_NAME /*=table_name*/,
							 &threshold /*=data*/))
		return PG_INT64_MIN;

	return threshold;
}

void
update_materializations(SchemaAndName partial_view, SchemaAndName materialization_table,
						Name time_column_name, InternalTimeRange new_materialization_range,
						int64 bucket_width, List *invalidations)
{
	InternalTimeRange invalidation_range = {
		.type = new_materialization_range.type,
	};
	InternalTimeRange combined_materialization_range = new_materialization_range;

	bool needs_invalidation = get_invalidation_range(invalidations, &invalidation_range);
	bool materialize_invalidations_separately = needs_invalidation;
	int res = SPI_connect();
	if (res != SPI_OK_CONNECT)
		elog(ERROR, "could not connect to SPI in materializer");

	/* pin the start of new_materialization to the end of new_materialization,
	 * we are not allowed to materialize beyond that point
	 */
	if (new_materialization_range.start > new_materialization_range.end)
		new_materialization_range.start = new_materialization_range.end;

	if (needs_invalidation)
	{
		Assert(invalidation_range.start <= invalidation_range.end);
		/* Align all invalidations to the bucket width */
		time_bucket_range(&invalidation_range, bucket_width);

		/* we never materialize beyond the new materialization range */
		invalidation_range.start =
			int64_min(invalidation_range.start, new_materialization_range.start);
		invalidation_range.end = int64_min(invalidation_range.end, new_materialization_range.end);

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
	if (!needs_invalidation || !materialize_invalidations_separately)
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
get_invalidation_range(List *invalidations, InternalTimeRange *invalidation_range)
{
	bool has_invalidations = false;
	ListCell *lc;

	invalidation_range->start = PG_INT64_MAX;
	invalidation_range->end = PG_INT64_MIN;
	foreach (lc, invalidations)
	{
		Invalidation *i = (Invalidation *) lfirst(lc);
		Assert(i->lowest_modified_value <= i->greatest_modified_value);
		if (i->lowest_modified_value < invalidation_range->start)
			invalidation_range->start = i->lowest_modified_value;
		if (i->greatest_modified_value > invalidation_range->end)
			invalidation_range->end = i->greatest_modified_value;

		has_invalidations = true;
	}

	return has_invalidations;
}

static void
time_bucket_range(InternalTimeRange *range, int64 bucket_width)
{
	range->start = ts_time_bucket_by_type(bucket_width, range->start, range->type);
	range->end = ts_time_bucket_by_type(bucket_width, range->end, range->type);

	if (range->end < PG_INT64_MAX - bucket_width)
		range->end += bucket_width;

	if (range->start > range->end)
		range->start = range->end;
}

static bool
ranges_overlap(InternalTimeRange invalidation_range, InternalTimeRange new_materialization_range)
{
	Assert(invalidation_range.start <= invalidation_range.end);
	Assert(new_materialization_range.start <= new_materialization_range.end);
	return !(invalidation_range.end < new_materialization_range.start ||
			 new_materialization_range.end < invalidation_range.start);
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
		Relation rel = heap_open(catalog_get_table_id(catalog, CONTINUOUS_AGGS_COMPLETED_THRESHOLD),
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
		relation_close(rel, NoLock);
	}
}

static int64
continuous_aggs_completed_threshold_get(int32 materialization_id)
{
	int64 threshold = 0;
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_continuous_aggs_completed_threshold_pkey_materialization_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(materialization_id));

	if (!ts_catalog_scan_one(CONTINUOUS_AGGS_COMPLETED_THRESHOLD /*=table*/,
							 CONTINUOUS_AGGS_COMPLETED_THRESHOLD_PKEY /*=indexid*/,
							 scankey /*=scankey*/,
							 1 /*=num_keys*/,
							 completed_threshold_tuple_found /*=tuple_found*/,
							 AccessShareLock /*=lockmode*/,
							 CONTINUOUS_AGGS_COMPLETED_THRESHOLD_TABLE_NAME /*=table_name*/,
							 &threshold /*=data*/))
		return PG_INT64_MIN;

	return threshold;
}
