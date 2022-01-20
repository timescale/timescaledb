/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_type.h>
#include <catalog/namespace.h>
#include <access/relscan.h>
#include <commands/tablecmds.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <utils/builtins.h>
#include <utils/timestamp.h>
#include <funcapi.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>

#include "ts_catalog/catalog.h"
#include "compat/compat.h"
#include "cross_module_fn.h"
#include "dimension.h"
#include "dimension_slice.h"
#include "dimension_vector.h"
#include "hypertable.h"
#include "indexing.h"
#include "hypertable_cache.h"
#include "partitioning.h"
#include "scanner.h"
#include "time_utils.h"
#include "utils.h"
#include "errors.h"

/* add_dimension record attribute numbers */
enum Anum_add_dimension
{
	Anum_add_dimension_id = 1,
	Anum_add_dimension_schema_name,
	Anum_add_dimension_table_name,
	Anum_add_dimension_column_name,
	Anum_add_dimension_created,
	_Anum_add_dimension_max,
};

#define Natts_add_dimension (_Anum_add_dimension_max - 1)

static int
cmp_dimension_id(const void *left, const void *right)
{
	const Dimension *diml = (Dimension *) left;
	const Dimension *dimr = (Dimension *) right;

	if (diml->fd.id < dimr->fd.id)
		return -1;

	if (diml->fd.id > dimr->fd.id)
		return 1;

	return 0;
}

const Dimension *
ts_hyperspace_get_dimension_by_id(const Hyperspace *hs, int32 id)
{
	Dimension dim = {
		.fd.id = id,
	};

	return bsearch(&dim, hs->dimensions, hs->num_dimensions, sizeof(Dimension), cmp_dimension_id);
}

Dimension *
ts_hyperspace_get_mutable_dimension_by_name(Hyperspace *hs, DimensionType type, const char *name)
{
	int i;

	for (i = 0; i < hs->num_dimensions; i++)
	{
		Dimension *dim = &hs->dimensions[i];

		if ((type == DIMENSION_TYPE_ANY || dim->type == type) &&
			namestrcmp(&dim->fd.column_name, name) == 0)
			return dim;
	}

	return NULL;
}

const Dimension *
ts_hyperspace_get_dimension_by_name(const Hyperspace *hs, DimensionType type, const char *name)
{
	return ts_hyperspace_get_mutable_dimension_by_name((Hyperspace *) hs, type, name);
}

Dimension *
ts_hyperspace_get_mutable_dimension(Hyperspace *hs, DimensionType type, Index n)
{
	int i;

	for (i = 0; i < hs->num_dimensions; i++)
	{
		if (type == DIMENSION_TYPE_ANY || hs->dimensions[i].type == type)
		{
			if (n == 0)
				return &hs->dimensions[i];
			n--;
		}
	}

	return NULL;
}

const Dimension *
ts_hyperspace_get_dimension(const Hyperspace *hs, DimensionType type, Index n)
{
	return ts_hyperspace_get_mutable_dimension((Hyperspace *) hs, type, n);
}

static int
hyperspace_get_num_dimensions_by_type(Hyperspace *hs, DimensionType type)
{
	int i;
	int n = 0;

	for (i = 0; i < hs->num_dimensions; i++)
	{
		if (type == DIMENSION_TYPE_ANY || hs->dimensions[i].type == type)
			n++;
	}

	return n;
}

static inline DimensionType
dimension_type(TupleInfo *ti)
{
	if (slot_attisnull(ti->slot, Anum_dimension_interval_length) &&
		!slot_attisnull(ti->slot, Anum_dimension_num_slices))
		return DIMENSION_TYPE_CLOSED;

	if (!slot_attisnull(ti->slot, Anum_dimension_interval_length) &&
		slot_attisnull(ti->slot, Anum_dimension_num_slices))
		return DIMENSION_TYPE_OPEN;

	elog(ERROR, "invalid partitioning dimension");
	/* suppress compiler warning on MSVC */
	return DIMENSION_TYPE_ANY;
}

static void
dimension_fill_in_from_tuple(Dimension *d, TupleInfo *ti, Oid main_table_relid)
{
	Datum values[Natts_dimension];
	bool isnull[Natts_dimension];
	bool should_free;
	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);

	/*
	 * With need to use heap_deform_tuple() rather than GETSTRUCT(), since
	 * optional values may be omitted from the tuple.
	 */
	heap_deform_tuple(tuple, ts_scanner_get_tupledesc(ti), values, isnull);

	d->type = dimension_type(ti);
	d->fd.id = DatumGetInt32(values[AttrNumberGetAttrOffset(Anum_dimension_id)]);
	d->fd.hypertable_id =
		DatumGetInt32(values[AttrNumberGetAttrOffset(Anum_dimension_hypertable_id)]);
	d->fd.aligned = DatumGetBool(values[AttrNumberGetAttrOffset(Anum_dimension_aligned)]);
	d->fd.column_type =
		DatumGetObjectId(values[AttrNumberGetAttrOffset(Anum_dimension_column_type)]);
	memcpy(&d->fd.column_name,
		   DatumGetName(values[AttrNumberGetAttrOffset(Anum_dimension_column_name)]),
		   NAMEDATALEN);

	if (!isnull[Anum_dimension_partitioning_func_schema - 1] &&
		!isnull[Anum_dimension_partitioning_func - 1])
	{
		MemoryContext old;

		d->fd.num_slices =
			DatumGetInt16(values[AttrNumberGetAttrOffset(Anum_dimension_num_slices)]);

		memcpy(&d->fd.partitioning_func_schema,
			   DatumGetName(
				   values[AttrNumberGetAttrOffset(Anum_dimension_partitioning_func_schema)]),
			   NAMEDATALEN);
		memcpy(&d->fd.partitioning_func,
			   DatumGetName(values[AttrNumberGetAttrOffset(Anum_dimension_partitioning_func)]),
			   NAMEDATALEN);

		old = MemoryContextSwitchTo(ti->mctx);
		d->partitioning = ts_partitioning_info_create(NameStr(d->fd.partitioning_func_schema),
													  NameStr(d->fd.partitioning_func),
													  NameStr(d->fd.column_name),
													  d->type,
													  main_table_relid);
		MemoryContextSwitchTo(old);
	}

	if (!isnull[Anum_dimension_integer_now_func_schema - 1] &&
		!isnull[Anum_dimension_integer_now_func - 1])
	{
		namestrcpy(&d->fd.integer_now_func_schema,
				   DatumGetCString(
					   values[AttrNumberGetAttrOffset(Anum_dimension_integer_now_func_schema)]));
		namestrcpy(&d->fd.integer_now_func,
				   DatumGetCString(
					   values[AttrNumberGetAttrOffset(Anum_dimension_integer_now_func)]));
	}

	if (d->type == DIMENSION_TYPE_CLOSED)
		d->fd.num_slices = DatumGetInt16(values[Anum_dimension_num_slices - 1]);
	else
		d->fd.interval_length =
			DatumGetInt64(values[AttrNumberGetAttrOffset(Anum_dimension_interval_length)]);

	d->column_attno = get_attnum(main_table_relid, NameStr(d->fd.column_name));
	d->main_table_relid = main_table_relid;

	if (should_free)
		heap_freetuple(tuple);
}

static Datum
create_range_datum(FunctionCallInfo fcinfo, DimensionSlice *slice)
{
	TupleDesc tupdesc;
	Datum values[2];
	bool nulls[2] = { false };
	HeapTuple tuple;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "function returning record called in context that cannot accept type record");

	tupdesc = BlessTupleDesc(tupdesc);

	values[0] = Int64GetDatum(slice->fd.range_start);
	values[1] = Int64GetDatum(slice->fd.range_end);
	tuple = heap_form_tuple(tupdesc, values, nulls);

	return HeapTupleGetDatum(tuple);
}

static DimensionSlice *
calculate_open_range_default(const Dimension *dim, int64 value)
{
	int64 range_start, range_end;
	Oid dimtype = ts_dimension_get_partition_type(dim);

	if (value < 0)
	{
		const int64 dim_min = ts_time_get_min(dimtype);

		range_end = ((value + 1) / dim->fd.interval_length) * dim->fd.interval_length;

		/* prevent integer underflow */
		if (dim_min - range_end > -dim->fd.interval_length)
			range_start = DIMENSION_SLICE_MINVALUE;
		else
			range_start = range_end - dim->fd.interval_length;
	}
	else
	{
		const int64 dim_end = ts_time_get_max(dimtype);

		range_start = (value / dim->fd.interval_length) * dim->fd.interval_length;

		/* prevent integer overflow */
		if (dim_end - range_start < dim->fd.interval_length)
			range_end = DIMENSION_SLICE_MAXVALUE;
		else
			range_end = range_start + dim->fd.interval_length;
	}

	return ts_dimension_slice_create(dim->fd.id, range_start, range_end);
}

TS_FUNCTION_INFO_V1(ts_dimension_calculate_open_range_default);

/*
 * Expose open dimension range calculation for testing purposes.
 */
Datum
ts_dimension_calculate_open_range_default(PG_FUNCTION_ARGS)
{
	int64 value = PG_GETARG_INT64(0);
	Dimension dim = {
		.type = DIMENSION_TYPE_OPEN,
		.fd.id = 0,
		.fd.interval_length = PG_GETARG_INT64(1),
		.fd.column_type = TypenameGetTypid(PG_GETARG_CSTRING(2)),
	};
	DimensionSlice *slice = calculate_open_range_default(&dim, value);

	PG_RETURN_DATUM(create_range_datum(fcinfo, slice));
}

static int64
calculate_closed_range_interval(const Dimension *dim)
{
	Assert(NULL != dim && IS_CLOSED_DIMENSION(dim));

	return DIMENSION_SLICE_CLOSED_MAX / ((int64) dim->fd.num_slices);
}

static DimensionSlice *
calculate_closed_range_default(const Dimension *dim, int64 value)
{
	int64 range_start, range_end;

	/* The interval that divides the dimension into N equal sized slices */
	int64 interval = calculate_closed_range_interval(dim);
	int64 last_start = interval * (dim->fd.num_slices - 1);

	if (value < 0)
		elog(ERROR, "invalid value " INT64_FORMAT " for closed dimension", value);

	if (value >= last_start)
	{
		/* put overflow from integer-division errors in last range */
		range_start = last_start;
		range_end = DIMENSION_SLICE_MAXVALUE;
	}
	else
	{
		range_start = (value / interval) * interval;
		range_end = range_start + interval;
	}

	if (0 == range_start)
	{
		range_start = DIMENSION_SLICE_MINVALUE;
	}

	return ts_dimension_slice_create(dim->fd.id, range_start, range_end);
}

TS_FUNCTION_INFO_V1(ts_dimension_calculate_closed_range_default);

/*
 * Exposed closed dimension range calculation for testing purposes.
 */
Datum
ts_dimension_calculate_closed_range_default(PG_FUNCTION_ARGS)
{
	int64 value = PG_GETARG_INT64(0);
	Dimension dim = {
		.type = DIMENSION_TYPE_CLOSED,
		.fd.id = 0,
		.fd.num_slices = PG_GETARG_INT16(1),
	};
	DimensionSlice *slice = calculate_closed_range_default(&dim, value);

	PG_RETURN_DATUM(create_range_datum(fcinfo, slice));
}

DimensionSlice *
ts_dimension_calculate_default_slice(const Dimension *dim, int64 value)
{
	if (IS_OPEN_DIMENSION(dim))
		return calculate_open_range_default(dim, value);

	return calculate_closed_range_default(dim, value);
}

/*
 * Get the ordinal value of a slice in an open dimension.
 *
 * Note that, for an open dimension, we can only deal with already created
 * slices and cannot account for, e.g., gaps in the dimension where future
 * slices might be created and thus changing the ordinal value for a slice.
 *
 * For instance, the ordinal value of slice D below is 2 (zero indexed):
 *
 * ' | A | B | <gap> | D | E |
 *
 * but when slice C is later created the ordinal value of D will be 3:
 *
 * ' | A | B | C | D | E |
 */
static int
ts_dimension_get_open_slice_ordinal(const Dimension *dim, const DimensionSlice *slice)
{
	DimensionVec *vec;
	int i;

	Assert(NULL != dim && IS_OPEN_DIMENSION(dim));
	Assert(NULL != slice);

	vec = ts_dimension_get_slices(dim);

	Assert(NULL != vec);

	/* Find the index (ordinal) of the chunk's slice in the open dimension */
	i = ts_dimension_vec_find_slice_index(vec, slice->fd.id);

	if (i >= 0)
		return i;
	else
	{
		/*
		 * Returns the number of slices if the slice not found, i.e., i = -1.
		 * Dimension slice might not exist if a chunk table is created without
		 * modifying metadata. It happens only during copy/move chunk for distributed
		 * hypertable, thus this code, which is used when no space dimension exists,
		 * is unlikely to be used.
		 */
		return vec->num_slices;
	}
}

/*
 * Get the ordinal value of a slice in a closed dimension.
 *
 * For closed dimensions, we calculate the ordinal value of a slice based on
 * the assumption that the dimension is fully partitioned in equal size slices
 * as given by the current partitioning configuration. In reality, though,
 * slices are created lazily so a closed dimension might have less slices in
 * time interval than the configuration suggests. Further, during time
 * intervals where repartitioning happens, there might be an unexpected number
 * of slices due to a mix of slices from both the old and the new partitioning
 * configuration. As a result, the ordinal value of a given slice might not
 * actually match the partitioning settings at a given point in time. In this case, we will return
 * the ordinal of current slice most overlapping the given slice (or first fully overlapped slice).
 */
static int
ts_dimension_get_closed_slice_ordinal(const Dimension *dim, const DimensionSlice *target_slice)
{
	int64 current_slice_size;
	int64 target_slice_size;
	int candidate_slice_ordinal;
	int64 target_overlap_with_candidate_slice;

	Assert(NULL != dim && IS_CLOSED_DIMENSION(dim));
	Assert(NULL != target_slice);
	Assert(dim->fd.num_slices > 0);

	/* Slicing assumes partitioning functions use the range [0, INT32_MAX], though the first slice
	 * uses INT64_MIN as its lower bound, and the last slice uses INT64_MAX as its upper bound. */
	if (target_slice->fd.range_start == DIMENSION_SLICE_MINVALUE)
		return 0;

	if (target_slice->fd.range_end == DIMENSION_SLICE_MAXVALUE)
		return dim->fd.num_slices - 1;

	Assert(target_slice->fd.range_start > 0);
	Assert(target_slice->fd.range_end < DIMENSION_SLICE_CLOSED_MAX);

	/* Given a target slice starting from some point p, determine a candidate slice in the current
	 * partitioning configuration that contains p. If that slice contains over half of our target
	 * slice, return it's ordinal. Otherwise return the ordinal for the next slice. */
	current_slice_size = calculate_closed_range_interval(dim);
	target_slice_size = target_slice->fd.range_end - target_slice->fd.range_start;
	candidate_slice_ordinal = target_slice->fd.range_start / current_slice_size;
	target_overlap_with_candidate_slice =
		current_slice_size - (target_slice->fd.range_start % current_slice_size);

	/* Note that if the candidate slice wholly contains the target slice,
	 * target_overlap_with_candidate_slice will actually be greater than target_slice_size.  This
	 * doesn't affect the correctness of the following check. */
	if (target_overlap_with_candidate_slice >= target_slice_size / 2)
		return candidate_slice_ordinal;
	else
		return candidate_slice_ordinal + 1;
}

/*
 * Get the ordinal value of a slice in a dimension.
 *
 * This function returns the ordinal value of a slice (starting at 0) in the
 * dimension it belongs to. In other words, the "earliest" slice along the
 * dimensional axis gets the lowest ordinal value and the "latest" the largest.
 */
int
ts_dimension_get_slice_ordinal(const Dimension *dim, const DimensionSlice *slice)
{
	Assert(NULL != dim);
	Assert(NULL != slice);
	Assert(dim->fd.id == slice->fd.dimension_id);

	switch (dim->type)
	{
		case DIMENSION_TYPE_OPEN:
			return ts_dimension_get_open_slice_ordinal(dim, slice);
		case DIMENSION_TYPE_CLOSED:
			return ts_dimension_get_closed_slice_ordinal(dim, slice);
		default:
			Assert(false);
			break;
	}

	pg_unreachable();

	return -1;
}

static Hyperspace *
hyperspace_create(int32 hypertable_id, Oid main_table_relid, uint16 num_dimensions,
				  MemoryContext mctx)
{
	Hyperspace *hs = MemoryContextAllocZero(mctx, HYPERSPACE_SIZE(num_dimensions));

	hs->hypertable_id = hypertable_id;
	hs->main_table_relid = main_table_relid;
	hs->capacity = num_dimensions;
	hs->num_dimensions = 0;
	return hs;
}

static ScanTupleResult
dimension_tuple_found(TupleInfo *ti, void *data)
{
	Hyperspace *hs = data;
	Dimension *d = &hs->dimensions[hs->num_dimensions++];

	dimension_fill_in_from_tuple(d, ti, hs->main_table_relid);

	return SCAN_CONTINUE;
}

static int
dimension_scan_internal(ScanKeyData *scankey, int nkeys, tuple_found_func tuple_found, void *data,
						int limit, int dimension_index, LOCKMODE lockmode, MemoryContext mctx)
{
	Catalog *catalog = ts_catalog_get();
	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, DIMENSION),
		.index = catalog_get_index(catalog, DIMENSION, dimension_index),
		.nkeys = nkeys,
		.limit = limit,
		.scankey = scankey,
		.data = data,
		.tuple_found = tuple_found,
		.lockmode = lockmode,
		.scandirection = ForwardScanDirection,
		.result_mctx = mctx,
	};

	return ts_scanner_scan(&scanctx);
}

Hyperspace *
ts_dimension_scan(int32 hypertable_id, Oid main_table_relid, int16 num_dimensions,
				  MemoryContext mctx)
{
	Hyperspace *space = hyperspace_create(hypertable_id, main_table_relid, num_dimensions, mctx);
	ScanKeyData scankey[1];

	/* Perform an index scan on hypertable_id. */
	ScanKeyInit(&scankey[0],
				Anum_dimension_hypertable_id_column_name_idx_hypertable_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(hypertable_id));

	dimension_scan_internal(scankey,
							1,
							dimension_tuple_found,
							space,
							num_dimensions,
							DIMENSION_HYPERTABLE_ID_COLUMN_NAME_IDX,
							AccessShareLock,
							mctx);

	/* Sort dimensions in ascending order to allow binary search lookups */
	qsort(space->dimensions, space->num_dimensions, sizeof(Dimension), cmp_dimension_id);

	return space;
}

static ScanTupleResult
dimension_find_hypertable_id_tuple_found(TupleInfo *ti, void *data)
{
	int32 *hypertable_id = data;
	bool isnull = false;
	Datum datum = slot_getattr(ti->slot, Anum_dimension_hypertable_id, &isnull);

	Assert(!isnull);
	*hypertable_id = DatumGetInt32(datum);

	return SCAN_DONE;
}

int32
ts_dimension_get_hypertable_id(int32 dimension_id)
{
	int32 hypertable_id;
	ScanKeyData scankey[1];
	int ret;

	/* Perform an index scan dimension_id. */
	ScanKeyInit(&scankey[0],
				Anum_dimension_id_idx_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(dimension_id));

	ret = dimension_scan_internal(scankey,
								  1,
								  dimension_find_hypertable_id_tuple_found,
								  &hypertable_id,
								  1,
								  DIMENSION_ID_IDX,
								  AccessShareLock,
								  CurrentMemoryContext);

	if (ret == 1)
		return hypertable_id;

	return -1;
}

DimensionVec *
ts_dimension_get_slices(const Dimension *dim)
{
	return ts_dimension_slice_scan_by_dimension(dim->fd.id, 0);
}

static int
dimension_scan_update(int32 dimension_id, tuple_found_func tuple_found, void *data,
					  LOCKMODE lockmode)
{
	Catalog *catalog = ts_catalog_get();
	ScanKeyData scankey[1];
	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, DIMENSION),
		.index = catalog_get_index(catalog, DIMENSION, DIMENSION_ID_IDX),
		.nkeys = 1,
		.limit = 1,
		.scankey = scankey,
		.data = data,
		.tuple_found = tuple_found,
		.lockmode = lockmode,
		.scandirection = ForwardScanDirection,
	};

	ScanKeyInit(&scankey[0],
				Anum_dimension_id_idx_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(dimension_id));

	return ts_scanner_scan(&scanctx);
}

static ScanTupleResult
dimension_tuple_delete(TupleInfo *ti, void *data)
{
	CatalogSecurityContext sec_ctx;
	bool isnull;
	Datum dimension_id = slot_getattr(ti->slot, Anum_dimension_id, &isnull);
	bool *delete_slices = data;

	Assert(!isnull);

	/* delete dimension slices */
	if (NULL != delete_slices && *delete_slices)
		ts_dimension_slice_delete_by_dimension_id(DatumGetInt32(dimension_id), false);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
	ts_catalog_restore_user(&sec_ctx);

	return SCAN_CONTINUE;
}

int
ts_dimension_delete_by_hypertable_id(int32 hypertable_id, bool delete_slices)
{
	ScanKeyData scankey[1];

	/* Perform an index scan to delete based on hypertable_id */
	ScanKeyInit(&scankey[0],
				Anum_dimension_hypertable_id_column_name_idx_hypertable_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(hypertable_id));

	return dimension_scan_internal(scankey,
								   1,
								   dimension_tuple_delete,
								   &delete_slices,
								   0,
								   DIMENSION_HYPERTABLE_ID_COLUMN_NAME_IDX,
								   RowExclusiveLock,
								   CurrentMemoryContext);
}

static ScanTupleResult
dimension_tuple_update(TupleInfo *ti, void *data)
{
	Dimension *dim = data;
	Datum values[Natts_dimension];
	bool nulls[Natts_dimension];
	CatalogSecurityContext sec_ctx;
	bool should_free;
	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
	HeapTuple new_tuple;

	heap_deform_tuple(tuple, ts_scanner_get_tupledesc(ti), values, nulls);

	Assert((dim->fd.num_slices <= 0 && dim->fd.interval_length > 0) ||
		   (dim->fd.num_slices > 0 && dim->fd.interval_length <= 0));

	values[AttrNumberGetAttrOffset(Anum_dimension_column_name)] =
		NameGetDatum(&dim->fd.column_name);
	values[AttrNumberGetAttrOffset(Anum_dimension_column_type)] =
		ObjectIdGetDatum(dim->fd.column_type);
	values[AttrNumberGetAttrOffset(Anum_dimension_num_slices)] = Int16GetDatum(dim->fd.num_slices);

	if (!nulls[AttrNumberGetAttrOffset(Anum_dimension_partitioning_func)] &&
		!nulls[AttrNumberGetAttrOffset(Anum_dimension_partitioning_func_schema)])
	{
		values[AttrNumberGetAttrOffset(Anum_dimension_partitioning_func)] =
			NameGetDatum(&dim->fd.partitioning_func);
		values[AttrNumberGetAttrOffset(Anum_dimension_partitioning_func_schema)] =
			NameGetDatum(&dim->fd.partitioning_func_schema);
	}

	if (*NameStr(dim->fd.integer_now_func) != '\0' &&
		*NameStr(dim->fd.integer_now_func_schema) != '\0')
	{
		values[AttrNumberGetAttrOffset(Anum_dimension_integer_now_func)] =
			NameGetDatum(&dim->fd.integer_now_func);
		values[AttrNumberGetAttrOffset(Anum_dimension_integer_now_func_schema)] =
			NameGetDatum(&dim->fd.integer_now_func_schema);
		nulls[AttrNumberGetAttrOffset(Anum_dimension_integer_now_func)] = false;
		nulls[AttrNumberGetAttrOffset(Anum_dimension_integer_now_func_schema)] = false;
	}

	if (!nulls[AttrNumberGetAttrOffset(Anum_dimension_interval_length)])
		values[AttrNumberGetAttrOffset(Anum_dimension_interval_length)] =
			Int64GetDatum(dim->fd.interval_length);

	new_tuple = heap_form_tuple(ts_scanner_get_tupledesc(ti), values, nulls);
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_update_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti), new_tuple);
	ts_catalog_restore_user(&sec_ctx);
	heap_freetuple(new_tuple);

	if (should_free)
		heap_freetuple(tuple);

	return SCAN_DONE;
}

static int32
dimension_insert_relation(Relation rel, int32 hypertable_id, Name colname, Oid coltype,
						  int16 num_slices, regproc partitioning_func, int64 interval_length)
{
	TupleDesc desc = RelationGetDescr(rel);
	Datum values[Natts_dimension];
	bool nulls[Natts_dimension] = { false };
	CatalogSecurityContext sec_ctx;
	int32 dimension_id;

	values[AttrNumberGetAttrOffset(Anum_dimension_hypertable_id)] = Int32GetDatum(hypertable_id);
	values[AttrNumberGetAttrOffset(Anum_dimension_column_name)] = NameGetDatum(colname);
	values[AttrNumberGetAttrOffset(Anum_dimension_column_type)] = ObjectIdGetDatum(coltype);

	if (OidIsValid(partitioning_func))
	{
		Oid pronamespace = get_func_namespace(partitioning_func);

		values[AttrNumberGetAttrOffset(Anum_dimension_partitioning_func)] =
			DirectFunctionCall1(namein, CStringGetDatum(get_func_name(partitioning_func)));
		values[AttrNumberGetAttrOffset(Anum_dimension_partitioning_func_schema)] =
			DirectFunctionCall1(namein, CStringGetDatum(get_namespace_name(pronamespace)));
	}
	else
	{
		nulls[AttrNumberGetAttrOffset(Anum_dimension_partitioning_func)] = true;
		nulls[AttrNumberGetAttrOffset(Anum_dimension_partitioning_func_schema)] = true;
	}

	if (num_slices > 0)
	{
		/* Closed (hash) dimension */
		Assert(num_slices > 0 && interval_length <= 0);
		values[AttrNumberGetAttrOffset(Anum_dimension_num_slices)] = Int16GetDatum(num_slices);
		values[AttrNumberGetAttrOffset(Anum_dimension_aligned)] = BoolGetDatum(false);
		nulls[AttrNumberGetAttrOffset(Anum_dimension_interval_length)] = true;
	}
	else
	{
		/* Open (time) dimension */
		Assert(num_slices <= 0 && interval_length > 0);
		values[AttrNumberGetAttrOffset(Anum_dimension_interval_length)] =
			Int64GetDatum(interval_length);
		values[AttrNumberGetAttrOffset(Anum_dimension_aligned)] = BoolGetDatum(true);
		nulls[AttrNumberGetAttrOffset(Anum_dimension_num_slices)] = true;
	}

	/* no integer_now function by default */
	nulls[AttrNumberGetAttrOffset(Anum_dimension_integer_now_func_schema)] = true;
	nulls[AttrNumberGetAttrOffset(Anum_dimension_integer_now_func)] = true;

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	dimension_id = Int32GetDatum(ts_catalog_table_next_seq_id(ts_catalog_get(), DIMENSION));
	values[AttrNumberGetAttrOffset(Anum_dimension_id)] = dimension_id;
	ts_catalog_insert_values(rel, desc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);

	return dimension_id;
}

static int32
dimension_insert(int32 hypertable_id, Name colname, Oid coltype, int16 num_slices,
				 regproc partitioning_func, int64 interval_length)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;
	int32 dimension_id;

	rel = table_open(catalog_get_table_id(catalog, DIMENSION), RowExclusiveLock);
	dimension_id = dimension_insert_relation(rel,
											 hypertable_id,
											 colname,
											 coltype,
											 num_slices,
											 partitioning_func,
											 interval_length);
	table_close(rel, RowExclusiveLock);
	return dimension_id;
}

int
ts_dimension_set_type(Dimension *dim, Oid newtype)
{
	if (!IS_VALID_OPEN_DIM_TYPE(newtype))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("cannot change data type of hypertable column \"%s\" from %s to %s",
						NameStr(dim->fd.column_name),
						format_type_be(dim->fd.column_type),
						format_type_be(newtype)),
				 errhint("Use an integer, timestamp, or date type.")));

	dim->fd.column_type = newtype;

	return dimension_scan_update(dim->fd.id, dimension_tuple_update, dim, RowExclusiveLock);
}

TSDLLEXPORT Oid
ts_dimension_get_partition_type(const Dimension *dim)
{
	Assert(dim != NULL);
	return dim->partitioning != NULL ? dim->partitioning->partfunc.rettype : dim->fd.column_type;
}

int
ts_dimension_set_name(Dimension *dim, const char *newname)
{
	namestrcpy(&dim->fd.column_name, newname);

	return dimension_scan_update(dim->fd.id, dimension_tuple_update, dim, RowExclusiveLock);
}

int
ts_dimension_set_chunk_interval(Dimension *dim, int64 chunk_interval)
{
	Assert(IS_OPEN_DIMENSION(dim));

	dim->fd.interval_length = chunk_interval;

	return dimension_scan_update(dim->fd.id, dimension_tuple_update, dim, RowExclusiveLock);
}

int
ts_dimension_set_number_of_slices(Dimension *dim, int16 num_slices)
{
	Assert(IS_CLOSED_DIMENSION(dim));

	dim->fd.num_slices = num_slices;

	return dimension_scan_update(dim->fd.id, dimension_tuple_update, dim, RowExclusiveLock);
}

/*
 * Apply any dimension-specific transformations on a value, i.e., apply
 * partitioning function. Optionally get the type of the resulting value via
 * the restype parameter.
 */
Datum
ts_dimension_transform_value(const Dimension *dim, Oid collation, Datum value, Oid const_datum_type,
							 Oid *restype)
{
	if (NULL != dim->partitioning)
		value = ts_partitioning_func_apply(dim->partitioning, collation, value);

	if (NULL != restype)
	{
		if (NULL != dim->partitioning)
			*restype = dim->partitioning->partfunc.rettype;
		else if (const_datum_type != InvalidOid)
			*restype = const_datum_type;
		else
			*restype = dim->fd.column_type;
	}

	return value;
}

static Point *
point_create(int16 num_dimensions)
{
	Point *p = palloc0(POINT_SIZE(num_dimensions));

	p->cardinality = num_dimensions;
	p->num_coords = 0;

	return p;
}

TSDLLEXPORT Point *
ts_hyperspace_calculate_point(const Hyperspace *hs, TupleTableSlot *slot)
{
	Point *p = point_create(hs->num_dimensions);
	int i;

	for (i = 0; i < hs->num_dimensions; i++)
	{
		const Dimension *d = &hs->dimensions[i];
		Datum datum;
		bool isnull;
		Oid dimtype;

		if (NULL != d->partitioning)
			datum = ts_partitioning_func_apply_slot(d->partitioning, slot, &isnull);
		else
			datum = slot_getattr(slot, d->column_attno, &isnull);

		switch (d->type)
		{
			case DIMENSION_TYPE_OPEN:
				dimtype = ts_dimension_get_partition_type(d);

				if (isnull)
					ereport(ERROR,
							(errcode(ERRCODE_NOT_NULL_VIOLATION),
							 errmsg("NULL value in column \"%s\" violates not-null constraint",
									NameStr(d->fd.column_name)),
							 errhint("Columns used for time partitioning cannot be NULL.")));

				p->coordinates[p->num_coords++] = ts_time_value_to_internal(datum, dimtype);
				break;
			case DIMENSION_TYPE_CLOSED:
				p->coordinates[p->num_coords++] = (int64) DatumGetInt32(datum);
				break;
			case DIMENSION_TYPE_ANY:
				elog(ERROR, "invalid dimension type when inserting tuple");
				break;
		}
	}

	return p;
}

static inline int64
interval_to_usec(Interval *interval)
{
	return (interval->month * DAYS_PER_MONTH * USECS_PER_DAY) + (interval->day * USECS_PER_DAY) +
		   interval->time;
}

#define INT_TYPE_MAX(type)                                                                         \
	(int64)(((type) == INT2OID) ? PG_INT16_MAX :                                                   \
								  (((type) == INT4OID) ? PG_INT32_MAX : PG_INT64_MAX))

#define IS_VALID_NUM_SLICES(num_slices) ((num_slices) >= 1 && (num_slices) <= PG_INT16_MAX)

static int64
get_validated_integer_interval(Oid dimtype, int64 value)
{
	if (value < 1 || value > INT_TYPE_MAX(dimtype))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid interval: must be between 1 and " INT64_FORMAT,
						INT_TYPE_MAX(dimtype))));

	if (IS_TIMESTAMP_TYPE(dimtype) && value < USECS_PER_SEC)
		ereport(WARNING,
				(errcode(ERRCODE_AMBIGUOUS_PARAMETER),
				 errmsg("unexpected interval: smaller than one second"),
				 errhint("The interval is specified in microseconds.")));

	return value;
}

static int64
dimension_interval_to_internal(const char *colname, Oid dimtype, Oid valuetype, Datum value,
							   bool adaptive_chunking)
{
	int64 interval;

	if (!IS_VALID_OPEN_DIM_TYPE(dimtype))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("invalid type for dimension \"%s\"", colname),
				 errhint("Use an integer, timestamp, or date type.")));

	if (!OidIsValid(valuetype))
	{
		if (IS_INTEGER_TYPE(dimtype))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("integer dimensions require an explicit interval")));

		value = Int64GetDatum(adaptive_chunking ? DEFAULT_CHUNK_TIME_INTERVAL_ADAPTIVE :
												  DEFAULT_CHUNK_TIME_INTERVAL);
		valuetype = INT8OID;
	}

	switch (valuetype)
	{
		case INT2OID:
			interval = get_validated_integer_interval(dimtype, DatumGetInt16(value));
			break;
		case INT4OID:
			interval = get_validated_integer_interval(dimtype, DatumGetInt32(value));
			break;
		case INT8OID:
			interval = get_validated_integer_interval(dimtype, DatumGetInt64(value));
			break;
		case INTERVALOID:
			if (!IS_TIMESTAMP_TYPE(dimtype))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid interval type for %s dimension", format_type_be(dimtype)),
						 errhint("Use an interval of type integer.")));

			interval = interval_to_usec(DatumGetIntervalP(value));
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid interval type for %s dimension", format_type_be(dimtype)),
					 IS_TIMESTAMP_TYPE(dimtype) ?
						 errhint("Use an interval of type integer or interval.") :
						 errhint("Use an interval of type integer.")));
	}

	if (dimtype == DATEOID && (interval <= 0 || interval % USECS_PER_DAY != 0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid interval for %s dimension", format_type_be(dimtype)),
				 errhint("Use an interval that is a multiple of one day.")));

	return interval;
}

TS_FUNCTION_INFO_V1(ts_dimension_interval_to_internal_test);

/*
 * Exposed for testing purposes.
 */
Datum
ts_dimension_interval_to_internal_test(PG_FUNCTION_ARGS)
{
	Oid dimtype = PG_GETARG_OID(0);
	Datum value = PG_GETARG_DATUM(1);
	Oid valuetype = PG_ARGISNULL(1) ? InvalidOid : get_fn_expr_argtype(fcinfo->flinfo, 1);

	PG_RETURN_INT64(dimension_interval_to_internal("testcol", dimtype, valuetype, value, false));
}

static void
dimension_add_not_null_on_column(Oid table_relid, char *colname)
{
	AlterTableCmd cmd = {
		.type = T_AlterTableCmd,
		.subtype = AT_SetNotNull,
		.name = colname,
		.missing_ok = false,
	};

	ereport(NOTICE,
			(errmsg("adding not-null constraint to column \"%s\"", colname),
			 errdetail("Time dimensions cannot have NULL values.")));

	EventTriggerAlterTableStart((Node *) &cmd);
	AlterTableInternal(table_relid, list_make1(&cmd), false);
	EventTriggerAlterTableEnd();
}

void
ts_dimension_update(const Hypertable *ht, const NameData *dimname, DimensionType dimtype,
					Datum *interval, Oid *intervaltype, int16 *num_slices, Oid *integer_now_func)
{
	Dimension *dim;

	if (NULL == ht)
		ereport(ERROR, (errcode(ERRCODE_TS_HYPERTABLE_NOT_EXIST), errmsg("invalid hypertable")));

	if (dimtype == DIMENSION_TYPE_ANY)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid dimension type")));

	if (NULL == dimname)
	{
		if (hyperspace_get_num_dimensions_by_type(ht->space, dimtype) > 1)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("hypertable \"%s\" has multiple %s dimensions",
							get_rel_name(ht->main_table_relid),
							dimtype == DIMENSION_TYPE_OPEN ? "time" : "space"),
					 errhint("An explicit dimension name must be specified.")));

		dim = ts_hyperspace_get_mutable_dimension(ht->space, dimtype, 0);
	}
	else
		dim = ts_hyperspace_get_mutable_dimension_by_name(ht->space, dimtype, NameStr(*dimname));

	if (NULL == dim)
		ereport(ERROR,
				(errcode(ERRCODE_TS_DIMENSION_NOT_EXIST),
				 errmsg("hypertable \"%s\" does not have a matching dimension",
						get_rel_name(ht->main_table_relid))));

	Assert(dim->type == dimtype);

	if (NULL != interval)
	{
		Oid dimtype = ts_dimension_get_partition_type(dim);
		Assert(NULL != intervaltype);

		Assert(IS_OPEN_DIMENSION(dim));

		dim->fd.interval_length =
			dimension_interval_to_internal(NameStr(dim->fd.column_name),
										   dimtype,
										   *intervaltype,
										   *interval,
										   hypertable_adaptive_chunking_enabled(ht));
	}

	if (NULL != num_slices)
	{
		Assert(IS_CLOSED_DIMENSION(dim));
		dim->fd.num_slices = *num_slices;
	}

	if (NULL != integer_now_func)
	{
		Oid pronamespace = get_func_namespace(*integer_now_func);
		namestrcpy(&dim->fd.integer_now_func_schema, get_namespace_name(pronamespace));
		namestrcpy(&dim->fd.integer_now_func, get_func_name(*integer_now_func));
	}

	dimension_scan_update(dim->fd.id, dimension_tuple_update, dim, RowExclusiveLock);
	ts_hypertable_check_partitioning(ht, dim->fd.id);
}

TS_FUNCTION_INFO_V1(ts_dimension_set_num_slices);

Datum
ts_dimension_set_num_slices(PG_FUNCTION_ARGS)
{
	Oid table_relid = PG_GETARG_OID(0);
	int32 num_slices_arg = PG_ARGISNULL(1) ? -1 : PG_GETARG_INT32(1);
	Name colname = PG_ARGISNULL(2) ? NULL : PG_GETARG_NAME(2);
	Cache *hcache = ts_hypertable_cache_pin();
	int16 num_slices;
	Hypertable *ht;

	TS_PREVENT_FUNC_IF_READ_ONLY();

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("hypertable cannot be NULL")));

	/* Verify that we're dealing with a hypertable or fail */
	ht = ts_hypertable_cache_get_entry(hcache, table_relid, CACHE_FLAG_NONE);
	ts_hypertable_permissions_check(table_relid, GetUserId());

	if (PG_ARGISNULL(1) || !IS_VALID_NUM_SLICES(num_slices_arg))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid number of partitions: must be between 1 and %d", PG_INT16_MAX)));

	/*
	 * Our catalog stores num_slices as a smallint (int16). However, function
	 * argument is an integer (int32) so that the user need not cast it to a
	 * smallint. We therefore convert to int16 here after checking that
	 * num_slices cannot be > INT16_MAX.
	 */
	num_slices = num_slices_arg & 0xffff;
	ts_dimension_update(ht, colname, DIMENSION_TYPE_CLOSED, NULL, NULL, &num_slices, NULL);
	ts_hypertable_func_call_on_data_nodes(ht, fcinfo);
	ts_cache_release(hcache);

	PG_RETURN_VOID();
}

TS_FUNCTION_INFO_V1(ts_dimension_set_interval);

/*
 * Update chunk_time_interval for a hypertable.
 *
 * hypertable - The OID of the table corresponding to a hypertable whose time
 *     interval should be updated
 * chunk_time_interval - The new time interval. For hypertables with integral
 *     time columns, this must be an integral type. For hypertables with a
 *     TIMESTAMP/TIMESTAMPTZ/DATE type, it can be integral which is treated as
 *     microseconds, or an INTERVAL type.
 * dimension_name - The name of the dimension
 */
Datum
ts_dimension_set_interval(PG_FUNCTION_ARGS)
{
	Oid table_relid = PG_GETARG_OID(0);
	Datum interval = PG_GETARG_DATUM(1);
	Oid intervaltype = InvalidOid;
	Name colname = PG_ARGISNULL(2) ? NULL : PG_GETARG_NAME(2);
	Cache *hcache = ts_hypertable_cache_pin();
	Hypertable *ht;

	TS_PREVENT_FUNC_IF_READ_ONLY();

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("hypertable cannot be NULL")));

	ht = ts_hypertable_cache_get_entry(hcache, table_relid, CACHE_FLAG_NONE);
	ts_hypertable_permissions_check(table_relid, GetUserId());

	if (PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid interval: an explicit interval must be specified")));

	intervaltype = get_fn_expr_argtype(fcinfo->flinfo, 1);
	ts_dimension_update(ht, colname, DIMENSION_TYPE_OPEN, &interval, &intervaltype, NULL, NULL);
	ts_hypertable_func_call_on_data_nodes(ht, fcinfo);
	ts_cache_release(hcache);

	PG_RETURN_VOID();
}

DimensionInfo *
ts_dimension_info_create_open(Oid table_relid, Name column_name, Datum interval, Oid interval_type,
							  regproc partitioning_func)
{
	DimensionInfo *info = palloc(sizeof(*info));
	*info = (DimensionInfo){
		.type = DIMENSION_TYPE_OPEN,
		.table_relid = table_relid,
		.colname = column_name,
		.interval_datum = interval,
		.interval_type = interval_type,
		.partitioning_func = partitioning_func,
	};
	return info;
}

DimensionInfo *
ts_dimension_info_create_closed(Oid table_relid, Name column_name, int32 num_slices,
								regproc partitioning_func)
{
	DimensionInfo *info = palloc(sizeof(*info));
	*info = (DimensionInfo){
		.type = DIMENSION_TYPE_CLOSED,
		.table_relid = table_relid,
		.colname = column_name,
		.num_slices = num_slices,
		.num_slices_is_set = true,
		.partitioning_func = partitioning_func,
	};
	return info;
}

/* Validate the configuration of an open ("time") dimension */
static void
dimension_info_validate_open(DimensionInfo *info)
{
	Oid dimtype = info->coltype;

	Assert(info->type == DIMENSION_TYPE_OPEN);

	if (OidIsValid(info->partitioning_func))
	{
		if (!ts_partitioning_func_is_valid(info->partitioning_func, info->type, info->coltype))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
					 errmsg("invalid partitioning function"),
					 errhint("A valid partitioning function for open (time) dimensions must be "
							 "IMMUTABLE, "
							 "take the column type as input, and return an integer or "
							 "timestamp type.")));

		dimtype = get_func_rettype(info->partitioning_func);
	}

	info->interval = dimension_interval_to_internal(NameStr(*info->colname),
													dimtype,
													info->interval_type,
													info->interval_datum,
													info->adaptive_chunking);
}

/* Validate the configuration of a closed ("space") dimension */
static void
dimension_info_validate_closed(DimensionInfo *info)
{
	Assert(info->type == DIMENSION_TYPE_CLOSED);

	if (!OidIsValid(info->partitioning_func))
		info->partitioning_func = ts_partitioning_func_get_closed_default();
	else if (!ts_partitioning_func_is_valid(info->partitioning_func, info->type, info->coltype))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
				 errmsg("invalid partitioning function"),
				 errhint("A valid partitioning function for closed (space) dimensions must be "
						 "IMMUTABLE "
						 "and have the signature (anyelement) -> integer.")));

	if (!info->num_slices_is_set || !IS_VALID_NUM_SLICES(info->num_slices))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid number of partitions for dimension \"%s\"",
						NameStr(*info->colname)),
				 errhint("A closed (space) dimension must specify between 1 and %d partitions.",
						 PG_INT16_MAX)));
}

void
ts_dimension_info_validate(DimensionInfo *info)
{
	const Dimension *dim;
	HeapTuple tuple;
	Datum datum;
	bool isnull = false;
	bool isgenerated;

	if (!DIMENSION_INFO_IS_SET(info))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid dimension info")));

	if (info->num_slices_is_set && OidIsValid(info->interval_type))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot specify both the number of partitions and an interval")));

	/* Check that the column exists and get its NOT NULL status */
	tuple = SearchSysCacheAttName(info->table_relid, NameStr(*info->colname));

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" does not exist", NameStr(*info->colname))));

	datum = SysCacheGetAttr(ATTNAME, tuple, Anum_pg_attribute_atttypid, &isnull);
	Assert(!isnull);

	info->coltype = DatumGetObjectId(datum);

	datum = SysCacheGetAttr(ATTNAME, tuple, Anum_pg_attribute_attnotnull, &isnull);
	Assert(!isnull);

	info->set_not_null = !DatumGetBool(datum);

	/* check that the column is not generated */
	datum = SysCacheGetAttr(ATTNAME, tuple, Anum_pg_attribute_attgenerated, &isnull);
	Assert(!isnull);
	isgenerated = (DatumGetChar(datum) == ATTRIBUTE_GENERATED_STORED);

	if (isgenerated)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid partitioning column"),
				 errhint("Generated columns cannot be used as partitioning dimensions.")));

	ReleaseSysCache(tuple);

	if (NULL != info->ht)
	{
		/* Check if the dimension already exists */
		dim = ts_hyperspace_get_dimension_by_name(info->ht->space,
												  DIMENSION_TYPE_ANY,
												  NameStr(*info->colname));

		if (NULL != dim)
		{
			if (!info->if_not_exists)
				ereport(ERROR,
						(errcode(ERRCODE_TS_DUPLICATE_DIMENSION),
						 errmsg("column \"%s\" is already a dimension", NameStr(*info->colname))));

			info->dimension_id = dim->fd.id;
			info->skip = true;

			ereport(NOTICE,
					(errmsg("column \"%s\" is already a dimension, skipping",
							NameStr(*info->colname))));
			return;
		}
	}

	switch (info->type)
	{
		case DIMENSION_TYPE_CLOSED:
			dimension_info_validate_closed(info);
			break;
		case DIMENSION_TYPE_OPEN:
			dimension_info_validate_open(info);
			break;
		case DIMENSION_TYPE_ANY:
			elog(ERROR, "invalid dimension type in configuration");
			break;
	}
}

int32
ts_dimension_add_from_info(DimensionInfo *info)
{
	if (info->set_not_null && info->type == DIMENSION_TYPE_OPEN)
		dimension_add_not_null_on_column(info->table_relid, NameStr(*info->colname));

	Assert(info->ht != NULL);

	info->dimension_id = dimension_insert(info->ht->fd.id,
										  info->colname,
										  info->coltype,
										  info->num_slices,
										  info->partitioning_func,
										  info->interval);

	return info->dimension_id;
}

/*
 * Create a datum to be returned by add_dimension DDL function
 */
static Datum
dimension_create_datum(FunctionCallInfo fcinfo, DimensionInfo *info)
{
	TupleDesc tupdesc;
	HeapTuple tuple;
	Datum values[Natts_add_dimension];
	bool nulls[Natts_add_dimension] = { false };

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in "
						"context that cannot accept type record")));

	tupdesc = BlessTupleDesc(tupdesc);
	values[AttrNumberGetAttrOffset(Anum_add_dimension_id)] = info->dimension_id;
	values[AttrNumberGetAttrOffset(Anum_add_dimension_schema_name)] =
		NameGetDatum(&info->ht->fd.schema_name);
	values[AttrNumberGetAttrOffset(Anum_add_dimension_table_name)] =
		NameGetDatum(&info->ht->fd.table_name);
	values[AttrNumberGetAttrOffset(Anum_add_dimension_column_name)] = NameGetDatum(info->colname);
	values[AttrNumberGetAttrOffset(Anum_add_dimension_created)] = BoolGetDatum(!info->skip);
	tuple = heap_form_tuple(tupdesc, values, nulls);

	return HeapTupleGetDatum(tuple);
}

TS_FUNCTION_INFO_V1(ts_dimension_add);

/*
 * Add a new dimension to a hypertable.
 *
 * Arguments:
 * 0. Relation ID of table
 * 1. Column name
 * 2. Number of partitions / slices in close ('space') dimensions
 * 3. Interval for open ('time') dimensions
 * 4. Partitioning function
 * 5. IF NOT EXISTS option (bool)
 */
Datum
ts_dimension_add(PG_FUNCTION_ARGS)
{
	Cache *hcache;
	DimensionInfo info = {
		.type = PG_ARGISNULL(2) ? DIMENSION_TYPE_OPEN : DIMENSION_TYPE_CLOSED,
		.table_relid = PG_GETARG_OID(0),
		.colname = PG_ARGISNULL(1) ? NULL : PG_GETARG_NAME(1),
		.num_slices = PG_ARGISNULL(2) ? DatumGetInt32(-1) : PG_GETARG_INT32(2),
		.num_slices_is_set = !PG_ARGISNULL(2),
		.interval_datum = PG_ARGISNULL(3) ? DatumGetInt32(-1) : PG_GETARG_DATUM(3),
		.interval_type = PG_ARGISNULL(3) ? InvalidOid : get_fn_expr_argtype(fcinfo->flinfo, 3),
		.partitioning_func = PG_ARGISNULL(4) ? InvalidOid : PG_GETARG_OID(4),
		.if_not_exists = PG_ARGISNULL(5) ? false : PG_GETARG_BOOL(5),
	};
	Datum retval = 0;

	TS_PREVENT_FUNC_IF_READ_ONLY();

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("hypertable cannot be NULL")));

	if (!info.num_slices_is_set && !OidIsValid(info.interval_type))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("must specify either the number of partitions or an interval")));

	ts_hypertable_permissions_check(info.table_relid, GetUserId());

	/*
	 * The hypertable catalog table has a CHECK(num_dimensions > 0), which
	 * means, that when this function is called from create_hypertable()
	 * instead of directly, num_dimension is already set to one. We therefore
	 * need to lock the hypertable tuple here so that we can set the correct
	 * number of dimensions once we've added the new dimension
	 */
	if (!ts_hypertable_lock_tuple_simple(info.table_relid))
		ereport(ERROR,
				(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
				 errmsg("could not lock hypertable \"%s\" for update",
						get_rel_name(info.table_relid))));

	info.ht = ts_hypertable_cache_get_cache_and_entry(info.table_relid, CACHE_FLAG_NONE, &hcache);

	if (info.num_slices_is_set && OidIsValid(info.interval_type))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot specify both the number of partitions and an interval")));

	if (!info.num_slices_is_set && !OidIsValid(info.interval_type))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot omit both the number of partitions and the interval")));

	ts_dimension_info_validate(&info);

	if (!info.skip)
	{
		int32 dimension_id;

		if (ts_hypertable_has_chunks(info.table_relid, AccessShareLock))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("hypertable \"%s\" has data or empty chunks",
							get_rel_name(info.table_relid)),
					 errdetail("It is not possible to add dimensions to a hypertable that has "
							   "chunks. Please truncate the table.")));

		/*
		 * Note that space->num_dimensions reflects the actual number of
		 * dimension rows and not the num_dimensions in the hypertable catalog
		 * table.
		 */
		ts_hypertable_set_num_dimensions(info.ht, info.ht->space->num_dimensions + 1);
		dimension_id = ts_dimension_add_from_info(&info);

		/* Verify that existing indexes are compatible with a hypertable */

		/*
		 * Need to get a fresh copy of hypertable from the database as cache
		 * does not reflect the changes in the previous 2 lines which add a
		 * new dimension
		 */
		info.ht = ts_hypertable_get_by_id(info.ht->fd.id);
		ts_indexing_verify_indexes(info.ht);

		/* Check that partitioning is sane */
		ts_hypertable_check_partitioning(info.ht, dimension_id);
	}

	ts_hypertable_func_call_on_data_nodes(info.ht, fcinfo);

	retval = dimension_create_datum(fcinfo, &info);
	ts_cache_release(hcache);

	PG_RETURN_DATUM(retval);
}

/* Used as a tuple found function */
static ScanTupleResult
dimension_rename_schema_name(TupleInfo *ti, void *data)
{
	/* Dimension table may contain null valued columns that is why we do not use
	 * FormData_dimension *dimension = (FormData_dimension *) GETSTRUCT(tuple);
	 * pattern here
	 */
	Datum values[Natts_dimension];
	bool nulls[Natts_dimension];
	bool doReplace[Natts_dimension] = { false };
	bool should_free;
	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
	HeapTuple new_tuple;
	/* contains [old_name,new_name] in that order */
	char **names = (char **) data;
	Name schemaname;

	heap_deform_tuple(tuple, ts_scanner_get_tupledesc(ti), values, nulls);
	Assert(!nulls[AttrNumberGetAttrOffset(Anum_dimension_partitioning_func_schema)] ||
		   !nulls[AttrNumberGetAttrOffset(Anum_dimension_integer_now_func_schema)]);

	/* Rename schema names */
	if (!nulls[AttrNumberGetAttrOffset(Anum_dimension_partitioning_func_schema)])
	{
		schemaname =
			DatumGetName(values[AttrNumberGetAttrOffset(Anum_dimension_partitioning_func_schema)]);

		if (namestrcmp(schemaname, names[0]) == 0)
		{
			namestrcpy(schemaname, (const char *) names[1]);
			values[AttrNumberGetAttrOffset(Anum_dimension_partitioning_func_schema)] =
				NameGetDatum(schemaname);
			doReplace[AttrNumberGetAttrOffset(Anum_dimension_partitioning_func_schema)] = true;
		}
	}

	if (!nulls[AttrNumberGetAttrOffset(Anum_dimension_integer_now_func_schema)])
	{
		schemaname =
			DatumGetName(values[AttrNumberGetAttrOffset(Anum_dimension_integer_now_func_schema)]);
		if (namestrcmp(schemaname, names[0]) == 0)
		{
			namestrcpy(schemaname, (const char *) names[1]);
			values[AttrNumberGetAttrOffset(Anum_dimension_integer_now_func_schema)] =
				NameGetDatum(schemaname);
			doReplace[AttrNumberGetAttrOffset(Anum_dimension_integer_now_func_schema)] = true;
		}
	}

	new_tuple = heap_modify_tuple(tuple, ts_scanner_get_tupledesc(ti), values, nulls, doReplace);
	ts_catalog_update(ti->scanrel, new_tuple);
	heap_freetuple(new_tuple);

	if (should_free)
		heap_freetuple(tuple);

	return SCAN_CONTINUE;
}

/* Go through internal dimensions table and rename all relevant schema */
void
ts_dimensions_rename_schema_name(const char *old_name, const char *new_name)
{
	NameData old_schema_name;
	ScanKeyData scankey[1];
	Catalog *catalog = ts_catalog_get();
	char *names[2] = { (char *) old_name, (char *) new_name };
	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, DIMENSION),
		.index = InvalidOid,
		.nkeys = 1,
		.scankey = scankey,
		.tuple_found = dimension_rename_schema_name,
		.data = names,
		.lockmode = RowExclusiveLock,
		.scandirection = ForwardScanDirection,
	};

	namestrcpy(&old_schema_name, old_name);

	ScanKeyInit(&scankey[0],
				Anum_dimension_partitioning_func_schema,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				NameGetDatum(&old_schema_name));

	ts_scanner_scan(&scanctx);

	ScanKeyInit(&scankey[0],
				Anum_dimension_integer_now_func_schema,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				NameGetDatum(&old_schema_name));

	ts_scanner_scan(&scanctx);
}

/*
 * Create partitioning key expressions for a dimension.
 *
 * This function returns a list of Expr nodes that represent the dimension as
 * a partitioning key.
 */
List *
ts_dimension_get_partexprs(const Dimension *dim, Index hyper_varno)
{
	Expr *expr = NULL;
	HeapTuple tuple;
	Form_pg_attribute att;
	List *exprs = NIL;

	tuple = SearchSysCache2(ATTNUM,
							ObjectIdGetDatum(dim->main_table_relid),
							Int16GetDatum(dim->column_attno));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for attribute");

	att = (Form_pg_attribute) GETSTRUCT(tuple);

	if (!att->attisdropped)
		expr = (Expr *) makeVar(hyper_varno,
								dim->column_attno,
								att->atttypid,
								att->atttypmod,
								att->attcollation,
								0);

	ReleaseSysCache(tuple);

	/* The expression on the partitioning key can be the raw key or the
	 * partitioning function on the key */
	if (NULL != dim->partitioning)
		exprs = list_make2(expr, dim->partitioning->partfunc.func_fmgr.fn_expr);
	else
		exprs = list_make1(expr);

	return exprs;
}
