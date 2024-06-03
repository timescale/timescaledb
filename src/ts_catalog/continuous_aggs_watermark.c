/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * This file handles continuous aggs watermark functions.
 */

#include <postgres.h>
#include <access/xact.h>
#include <fmgr.h>
#include <miscadmin.h>
#include <utils/acl.h>
#include <utils/inval.h>
#include <utils/snapmgr.h>

#include "debug_point.h"
#include "hypertable.h"
#include "ts_catalog/continuous_agg.h"
#include "ts_catalog/continuous_aggs_watermark.h"

static void
cagg_watermark_init_scan_by_mat_hypertable_id(ScanIterator *iterator, const int32 mat_hypertable_id)
{
	iterator->ctx.index = catalog_get_index(ts_catalog_get(),
											CONTINUOUS_AGGS_WATERMARK,
											CONTINUOUS_AGGS_WATERMARK_PKEY);

	ts_scan_iterator_scan_key_init(iterator,
								   Anum_continuous_aggs_watermark_pkey_mat_hypertable_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(mat_hypertable_id));
}

int64
ts_cagg_watermark_get(int32 hypertable_id)
{
	PG_USED_FOR_ASSERTS_ONLY short count = 0;
	Datum watermark = (Datum) 0;
	bool value_isnull = true;
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGGS_WATERMARK, AccessShareLock, CurrentMemoryContext);

	/*
	 * The watermark of a CAGG has to be fetched by using the transaction snapshot.
	 *
	 * By default, the ts_scanner uses the SnapshotSelf to perform a scan. However, reading the
	 * watermark must be done using the transaction snapshot in order to ensure that the view on the
	 * watermark and the materialized part of the CAGG match.
	 */
	iterator.ctx.snapshot = GetTransactionSnapshot();
	Assert(iterator.ctx.snapshot != NULL);

	cagg_watermark_init_scan_by_mat_hypertable_id(&iterator, hypertable_id);

	ts_scanner_foreach(&iterator)
	{
		watermark = slot_getattr(ts_scan_iterator_slot(&iterator),
								 Anum_continuous_aggs_watermark_watermark,
								 &value_isnull);
		count++;
	}
	Assert(count <= 1);
	ts_scan_iterator_close(&iterator);

	if (value_isnull)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("watermark not defined for continuous aggregate: %d", hypertable_id)));

	/* Log the read watermark, needed for MVCC tap tests */
	ereport(DEBUG5,
			(errcode(ERRCODE_SUCCESSFUL_COMPLETION),
			 errmsg("watermark for continuous aggregate, '%d' is: " INT64_FORMAT,
					hypertable_id,
					DatumGetInt64(watermark))));

	return DatumGetInt64(watermark);
}

TS_FUNCTION_INFO_V1(ts_continuous_agg_watermark);

/*
 * Get the watermark for a real-time aggregation query on a continuous
 * aggregate.
 *
 * The watermark determines where the materialization ends for a continuous
 * aggregate. It is used by real-time aggregation as the threshold between the
 * materialized data and real-time data in the UNION query.
 *
 * The watermark is stored into `_timescaledb_catalog.continuous_aggs_watermark`
 * catalog table by the `refresh_continuous_agregate` procedure. It is defined
 * as the end of the last (highest) bucket in the materialized hypertable of a
 * continuous aggregate.
 *
 * The materialized hypertable ID is given as input argument.
 */
Datum
ts_continuous_agg_watermark(PG_FUNCTION_ARGS)
{
	const int32 mat_hypertable_id = PG_GETARG_INT32(0);
	ContinuousAgg *cagg;
	AclResult aclresult;

	cagg = ts_continuous_agg_find_by_mat_hypertable_id(mat_hypertable_id, false);

	/*
	 * Preemptive permission check to ensure the function complains about lack
	 * of permissions on the cagg rather than the materialized hypertable
	 */
	aclresult = pg_class_aclcheck(cagg->relid, GetUserId(), ACL_SELECT);
	aclcheck_error(aclresult, OBJECT_MATVIEW, get_rel_name(cagg->relid));

	int64 watermark = ts_cagg_watermark_get(cagg->data.mat_hypertable_id);

	PG_RETURN_INT64(watermark);
}

static int64
cagg_compute_watermark(ContinuousAgg *cagg, int64 watermark, bool isnull)
{
	if (isnull)
	{
		watermark = ts_time_get_min(cagg->partition_type);
	}
	else
	{
		/*
		 * The materialized hypertable is already bucketed, which means the
		 * max is the start of the last bucket. Add one bucket to move to the
		 * point where the materialized data ends.
		 */
		if (cagg->bucket_function->bucket_fixed_interval == false)
		{
			/*
			 * Since `value` is already bucketed, `bucketed = true` flag can
			 * be added to ts_compute_beginning_of_the_next_bucket_variable() as
			 * an optimization, if necessary.
			 */
			watermark =
				ts_compute_beginning_of_the_next_bucket_variable(watermark, cagg->bucket_function);
		}
		else
		{
			watermark =
				ts_time_saturating_add(watermark,
									   ts_continuous_agg_fixed_bucket_width(cagg->bucket_function),
									   cagg->partition_type);
		}
	}

	return watermark;
}

TS_FUNCTION_INFO_V1(ts_continuous_agg_watermark_materialized);

/*
 * Get the materialized watermark for a real-time aggregation query on a
 * continuous aggregate.
 *
 * The difference between this function and `ts_continuous_agg_watermark` is
 * that this one get the max open dimension of the materialization hypertable
 * instead of get the stored value in the catalog table.
 */
Datum
ts_continuous_agg_watermark_materialized(PG_FUNCTION_ARGS)
{
	const int32 mat_hypertable_id = PG_GETARG_INT32(0);
	ContinuousAgg *cagg;
	AclResult aclresult;
	bool isnull;
	Hypertable *ht;
	int64 watermark;

	cagg = ts_continuous_agg_find_by_mat_hypertable_id(mat_hypertable_id, false);

	/*
	 * Preemptive permission check to ensure the function complains about lack
	 * of permissions on the cagg rather than the materialized hypertable
	 */
	aclresult = pg_class_aclcheck(cagg->relid, GetUserId(), ACL_SELECT);
	aclcheck_error(aclresult, OBJECT_MATVIEW, get_rel_name(cagg->relid));

	ht = ts_hypertable_get_by_id(cagg->data.mat_hypertable_id);
	watermark = ts_hypertable_get_open_dim_max_value(ht, 0, &isnull);

	watermark = cagg_compute_watermark(cagg, watermark, isnull);

	PG_RETURN_INT64(watermark);
}

TSDLLEXPORT void
ts_cagg_watermark_insert(Hypertable *mat_ht, int64 watermark, bool watermark_isnull)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel =
		table_open(catalog_get_table_id(catalog, CONTINUOUS_AGGS_WATERMARK), RowExclusiveLock);
	TupleDesc desc = RelationGetDescr(rel);
	Datum values[Natts_continuous_aggs_watermark];
	bool nulls[Natts_continuous_aggs_watermark] = { false };
	CatalogSecurityContext sec_ctx;

	/* if trying to insert a NULL watermark then get the MIN value for the time dimension */
	if (watermark_isnull)
	{
		const Dimension *dim = hyperspace_get_open_dimension(mat_ht->space, 0);

		if (NULL == dim)
			elog(ERROR, "invalid open dimension index %d", 0);

		watermark = ts_time_get_min(ts_dimension_get_partition_type(dim));
	}

	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_watermark_mat_hypertable_id)] =
		Int32GetDatum(mat_ht->fd.id);
	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_watermark_watermark)] =
		Int64GetDatum(watermark);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_values(rel, desc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);

	table_close(rel, NoLock);
}

typedef struct WatermarkUpdate
{
	int64 watermark;
	bool force_update;
	bool invalidate_rel_cache;
	Oid ht_relid;
} WatermarkUpdate;

static ScanTupleResult
cagg_watermark_update_scan_internal(TupleInfo *ti, void *data)
{
	WatermarkUpdate *watermark_update = data;
	bool should_free;
	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
	Form_continuous_aggs_watermark form = (Form_continuous_aggs_watermark) GETSTRUCT(tuple);

	if (watermark_update->watermark > form->watermark || watermark_update->force_update)
	{
		HeapTuple new_tuple = heap_copytuple(tuple);
		form = (Form_continuous_aggs_watermark) GETSTRUCT(new_tuple);
		form->watermark = watermark_update->watermark;
		ts_catalog_update(ti->scanrel, new_tuple);
		heap_freetuple(new_tuple);

		/*
		 * During query planning, the values of the watermark function are constified using the
		 * constify_cagg_watermark() function. However, this function's value changes when we update
		 * the Cagg (the volatility of the function is STABLE not IMMUTABLE). To ensure that caches,
		 * such as the query plan cache, are properly evicted, we send an invalidation message for
		 * the hypertable.
		 */
		if (watermark_update->invalidate_rel_cache)
		{
			DEBUG_WAITPOINT("cagg_watermark_update_internal_before_refresh");
			CacheInvalidateRelcacheByRelid(watermark_update->ht_relid);
		}
	}
	else
	{
		elog(DEBUG1,
			 "hypertable %d existing watermark >= new watermark " INT64_FORMAT " " INT64_FORMAT,
			 form->mat_hypertable_id,
			 form->watermark,
			 watermark_update->watermark);
		watermark_update->watermark = form->watermark;
	}

	if (should_free)
		heap_freetuple(tuple);

	return SCAN_DONE;
}

static void
cagg_watermark_update_internal(int32 mat_hypertable_id, Oid ht_relid, int64 new_watermark,
							   bool force_update, bool invalidate_rel_cache)
{
	bool watermark_updated;
	ScanKeyData scankey[1];
	WatermarkUpdate data = { .watermark = new_watermark,
							 .force_update = force_update,
							 .invalidate_rel_cache = invalidate_rel_cache,
							 .ht_relid = ht_relid };

	ScanKeyInit(&scankey[0],
				Anum_continuous_aggs_watermark_mat_hypertable_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(mat_hypertable_id));

	watermark_updated = ts_catalog_scan_one(CONTINUOUS_AGGS_WATERMARK /*=table*/,
											CONTINUOUS_AGGS_WATERMARK_PKEY /*=indexid*/,
											scankey /*=scankey*/,
											1 /*=num_keys*/,
											cagg_watermark_update_scan_internal /*=tuple_found*/,
											RowExclusiveLock /*=lockmode*/,
											CONTINUOUS_AGGS_WATERMARK_TABLE_NAME /*=table_name*/,
											&data /*=data*/);

	if (!watermark_updated)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("watermark not defined for continuous aggregate: %d", mat_hypertable_id)));
	}
}

TSDLLEXPORT void
ts_cagg_watermark_update(Hypertable *mat_ht, int64 watermark, bool watermark_isnull,
						 bool force_update)
{
	ContinuousAgg *cagg = ts_continuous_agg_find_by_mat_hypertable_id(mat_ht->fd.id, false);

	/* If we have a real-time CAgg, it uses a watermark function. So, we have to invalidate the rel
	 * cache to force a replanning of prepared statements. See cagg_watermark_update_internal for
	 * more information. */
	bool invalidate_rel_cache = !cagg->data.materialized_only;

	watermark = cagg_compute_watermark(cagg, watermark, watermark_isnull);
	cagg_watermark_update_internal(mat_ht->fd.id,
								   mat_ht->main_table_relid,
								   watermark,
								   force_update,
								   invalidate_rel_cache);
}

TSDLLEXPORT void
ts_cagg_watermark_delete_by_mat_hypertable_id(int32 mat_hypertable_id)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGGS_WATERMARK, RowExclusiveLock, CurrentMemoryContext);

	cagg_watermark_init_scan_by_mat_hypertable_id(&iterator, mat_hypertable_id);

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
	}
	ts_scan_iterator_close(&iterator);
}
