
/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_type.h>
#include <executor/spi.h>
#include <fmgr.h>
#include <commands/trigger.h>
#include <lib/stringinfo.h>
#include <miscadmin.h>
#include <utils/hsearch.h>
#include <access/tupconvert.h>
#include <utils/builtins.h>
#include <utils/rel.h>
#include <utils/relcache.h>
#include <utils/date.h>
#include <access/xact.h>

#include <scanner.h>

#include "chunk.h"
#include "dimension.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "export.h"
#include "partitioning.h"

#include "utils.h"
#include "time_bucket.h"

#include "continuous_aggs/materialize.h"

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

	/* Align all invalidations to the bucket width */
	time_bucket_range(&invalidation_range, bucket_width);

	/* pin the start of new_materialization to the end of new_materialization,
	 * we are not allowed to materialize beyond that point
	 */
	if (new_materialization_range.start > new_materialization_range.end)
		new_materialization_range.start = new_materialization_range.end;

	if (needs_invalidation)
	{
		Assert(invalidation_range.start <= invalidation_range.end);

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
			return DatumGetTimestampTz(DT_NOBEGIN);
		case TIMESTAMPTZOID:
			return DatumGetTimestampTz(DT_NOBEGIN);
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
			return DatumGetTimestampTz(DT_NOEND);
		case TIMESTAMPTZOID:
			return DatumGetTimestampTz(DT_NOEND);
		case DATEOID:
			return DateADTGetDatum(DATEVAL_NOEND);
			break;
		default:
			return ts_internal_to_time_value(PG_INT64_MAX, type);
	}
}

static TimeRange
internal_time_range_to_time_range(InternalTimeRange internal)
{
	TimeRange range;
	range.type = internal.type;

	/* MIN and MAX can occur due to NULL thresholds, or due to a lack of invalidations. Since our
	 * regular conversion function errors in those cases, and we want to use those as markers for an
	 * open threshold in one direction, we special case this here*/
	if (internal.start == PG_INT64_MIN)
		range.start = time_range_internal_to_min_time_value(internal.type);
	else if (internal.start == PG_INT64_MAX)
		range.start = time_range_internal_to_max_time_value(internal.type);
	else
		range.start = ts_internal_to_time_value(internal.start, internal.type);

	if (internal.end == PG_INT64_MIN)
		range.end = time_range_internal_to_min_time_value(internal.type);
	else if (internal.end == PG_INT64_MAX)
		range.end = time_range_internal_to_max_time_value(internal.type);
	else
		range.end = ts_internal_to_time_value(internal.end, internal.type);

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
	Oid arg_types[2];
	Datum args[2];

	arg_types[0] = invalidation_range.type;
	args[0] = invalidation_range.start;

	arg_types[1] = invalidation_range.type;
	args[1] = invalidation_range.end;
	appendStringInfo(command,
					 "DELETE FROM %s.%s AS D WHERE "
					 "D.%s >= $1 AND D.%s < $2;",
					 quote_identifier(NameStr(*materialization_table.schema)),
					 quote_identifier(NameStr(*materialization_table.name)),
					 quote_identifier(NameStr(*time_column_name)),
					 quote_identifier(NameStr(*time_column_name)));
	res = SPI_execute_with_args(command->data,
								2 /*=nargs*/,
								arg_types,
								args,
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
	Oid arg_types[2];
	Datum args[2];

	arg_types[0] = materialization_range.type;
	args[0] = materialization_range.start;

	arg_types[1] = materialization_range.type;
	args[1] = materialization_range.end;
	appendStringInfo(command,
					 "INSERT INTO %s.%s SELECT * FROM %s.%s AS I "
					 "WHERE I.%s >= $1 AND I.%s < $2;",
					 quote_identifier(NameStr(*materialization_table.schema)),
					 quote_identifier(NameStr(*materialization_table.name)),
					 quote_identifier(NameStr(*partial_view.schema)),
					 quote_identifier(NameStr(*partial_view.name)),
					 quote_identifier(NameStr(*time_column_name)),
					 quote_identifier(NameStr(*time_column_name)));
	res = SPI_execute_with_args(command->data,
								2 /*=nargs*/,
								arg_types,
								args,
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
	Assert(form->watermark <= new_threshold);
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
