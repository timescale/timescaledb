#include <postgres.h>
#include <access/relscan.h>
#include <utils/lsyscache.h>
#include <funcapi.h>

#include "catalog.h"
#include "dimension.h"
#include "hypertable.h"
#include "scanner.h"
#include "partitioning.h"
#include "utils.h"
#include "dimension_slice.h"

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

Dimension *
hyperspace_get_dimension_by_id(Hyperspace *hs, int32 id)
{
	Dimension	dim = {
		.fd.id = id,
	};

	return bsearch(&dim, hs->dimensions, hs->num_dimensions,
				   sizeof(Dimension), cmp_dimension_id);
}

Dimension *
hyperspace_get_dimension(Hyperspace *hs, DimensionType type, Index n)
{
	int			i;

	for (i = 0; i < hs->num_dimensions; i++)
	{
		if (hs->dimensions[i].type == type)
		{
			if (n == 0)
				return &hs->dimensions[i];
			n--;
		}
	}

	return NULL;
}

static inline DimensionType
dimension_type(HeapTuple tuple)
{
	/* If there is no partitioning func set we assume open dimension */
	if (heap_attisnull(tuple, Anum_dimension_partitioning_func))
		return DIMENSION_TYPE_OPEN;
	return DIMENSION_TYPE_CLOSED;
}

static void
dimension_fill_in_from_tuple(Dimension *d, TupleInfo *ti, Oid main_table_relid)
{
	Datum		values[Natts_dimension];
	bool		isnull[Natts_dimension];

	/*
	 * With need to use heap_deform_tuple() rather than GETSTRUCT(), since
	 * optional values may be omitted from the tuple.
	 */
	heap_deform_tuple(ti->tuple, ti->desc, values, isnull);

	d->type = dimension_type(ti->tuple);
	d->fd.id = DatumGetInt32(values[Anum_dimension_id - 1]);
	d->fd.hypertable_id = DatumGetInt32(values[Anum_dimension_hypertable_id - 1]);
	d->fd.aligned = DatumGetBool(values[Anum_dimension_aligned - 1]);
	d->fd.column_type = DatumGetObjectId(values[Anum_dimension_column_type - 1]);
	memcpy(&d->fd.column_name,
		   DatumGetName(values[Anum_dimension_column_name - 1]),
		   NAMEDATALEN);

	if (d->type == DIMENSION_TYPE_CLOSED)
	{
		d->fd.num_slices = DatumGetInt16(values[Anum_dimension_num_slices - 1]);
		memcpy(&d->fd.partitioning_func_schema,
		   DatumGetName(values[Anum_dimension_partitioning_func_schema - 1]),
			   NAMEDATALEN);
		memcpy(&d->fd.partitioning_func,
			   DatumGetName(values[Anum_dimension_partitioning_func - 1]),
			   NAMEDATALEN);

		d->partitioning = partitioning_info_create(d->fd.num_slices,
									 NameStr(d->fd.partitioning_func_schema),
											NameStr(d->fd.partitioning_func),
												   NameStr(d->fd.column_name),
												   main_table_relid);
	}
	else
		d->fd.interval_length = DatumGetInt64(values[Anum_dimension_interval_length - 1]);

	d->column_attno = get_attnum(main_table_relid, NameStr(d->fd.column_name));
}

static Datum
create_range_datum(FunctionCallInfo fcinfo, DimensionSlice *slice)
{
	TupleDesc	tupdesc;
	Datum		values[2];
	bool		nulls[2] = {false};
	HeapTuple	tuple;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "Function returning record called in context that cannot accept type record");

	tupdesc = BlessTupleDesc(tupdesc);

	values[0] = Int64GetDatum(slice->fd.range_start);
	values[1] = Int64GetDatum(slice->fd.range_end);
	tuple = heap_form_tuple(tupdesc, values, nulls);

	return HeapTupleGetDatum(tuple);
}

#define RANGE_VALUE_MAX PG_INT32_MAX

static DimensionSlice *
calculate_open_range_default(Dimension *dim, int64 value)
{
	int64		range_start,
				range_end;

	if (value < 0)
	{
		range_end = ((value + 1) / dim->fd.interval_length) * dim->fd.interval_length;
		range_start = range_end - dim->fd.interval_length;
	}
	else
	{
		range_start = (value / dim->fd.interval_length) * dim->fd.interval_length;
		range_end = range_start + dim->fd.interval_length;
	}

	return dimension_slice_create(dim->fd.id, range_start, range_end);
}

PGDLLEXPORT Datum dimension_calculate_open_range_default(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(dimension_calculate_open_range_default);

/*
 * Expose open dimension range calculation for testing purposes.
 */
Datum
dimension_calculate_open_range_default(PG_FUNCTION_ARGS)
{
	int64		value = PG_GETARG_INT64(0);
	Dimension	dim = {
		.fd.id = 0,
		.fd.interval_length = PG_GETARG_INT64(1),
	};
	DimensionSlice *slice = calculate_open_range_default(&dim, value);

	PG_RETURN_DATUM(create_range_datum(fcinfo, slice));
}

static DimensionSlice *
calculate_closed_range_default(Dimension *dim, int64 value)
{
	int64		range_start,
				range_end;

	/* The interval that divides the dimension into N equal sized slices */
	int32		interval = RANGE_VALUE_MAX / dim->fd.num_slices;

	if (value < 0)
		elog(ERROR, "Invalid value " INT64_FORMAT " for closed dimension", value);

	if (value >= (interval * (dim->fd.num_slices - 1)))
	{
		/* put overflow from integer-division errors in last range */
		range_start = interval * (dim->fd.num_slices - 1);
		range_end = RANGE_VALUE_MAX;
	}
	else
	{
		range_start = (value / interval) * interval;
		range_end = range_start + interval;
	}

	return dimension_slice_create(dim->fd.id, range_start, range_end);
}

PGDLLEXPORT Datum dimension_calculate_closed_range_default(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(dimension_calculate_closed_range_default);

/*
 * Exposed closed dimension range calculation for testing purposes.
 */
Datum
dimension_calculate_closed_range_default(PG_FUNCTION_ARGS)
{
	int64		value = PG_GETARG_INT64(0);
	Dimension	dim = {
		.fd.id = 0,
		.fd.num_slices = PG_GETARG_INT16(1),
	};
	DimensionSlice *slice = calculate_closed_range_default(&dim, value);

	PG_RETURN_DATUM(create_range_datum(fcinfo, slice));
}

DimensionSlice *
dimension_calculate_default_slice(Dimension *dim, int64 value)
{
	if (IS_OPEN_DIMENSION(dim))
		return calculate_open_range_default(dim, value);

	return calculate_closed_range_default(dim, value);
}

static Hyperspace *
hyperspace_create(int32 hypertable_id, Oid main_table_relid, uint16 num_dimensions)
{
	Hyperspace *hs = palloc0(HYPERSPACE_SIZE(num_dimensions));

	hs->hypertable_id = hypertable_id;
	hs->main_table_relid = main_table_relid;
	hs->capacity = num_dimensions;
	hs->num_dimensions = 0;
	return hs;
}

static bool
dimension_tuple_found(TupleInfo *ti, void *data)
{
	Hyperspace *hs = data;
	Dimension  *d = &hs->dimensions[hs->num_dimensions++];

	dimension_fill_in_from_tuple(d, ti, hs->main_table_relid);

	return true;
}

Hyperspace *
dimension_scan(int32 hypertable_id, Oid main_table_relid, int16 num_dimensions)
{
	Catalog    *catalog = catalog_get();
	Hyperspace *space = hyperspace_create(hypertable_id, main_table_relid, num_dimensions);
	ScanKeyData scankey[1];
	ScannerCtx	scanCtx = {
		.table = catalog->tables[DIMENSION].id,
		.index = catalog->tables[DIMENSION].index_ids[DIMENSION_HYPERTABLE_ID_IDX],
		.scantype = ScannerTypeIndex,
		.nkeys = 1,
		.limit = num_dimensions,
		.scankey = scankey,
		.data = space,
		.tuple_found = dimension_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	/* Perform an index scan on hypertable_id. */
	ScanKeyInit(&scankey[0], Anum_dimension_hypertable_id_idx_hypertable_id,
			  BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(hypertable_id));

	scanner_scan(&scanCtx);

	/* Sort dimensions in ascending order to allow binary search lookups */
	qsort(space->dimensions, space->num_dimensions, sizeof(Dimension), cmp_dimension_id);

	return space;
}

static Point *
point_create(int16 num_dimensions)
{
	Point	   *p = palloc0(POINT_SIZE(num_dimensions));

	p->cardinality = num_dimensions;
	p->num_coords = 0;

	return p;
}

Point *
hyperspace_calculate_point(Hyperspace *hs, HeapTuple tuple, TupleDesc tupdesc)
{
	Point	   *p = point_create(hs->num_dimensions);
	int			i;

	for (i = 0; i < hs->num_dimensions; i++)
	{
		Dimension  *d = &hs->dimensions[i];

		if (IS_OPEN_DIMENSION(d))
		{
			Datum		datum;
			bool		isnull;

			datum = heap_getattr(tuple, d->column_attno, tupdesc, &isnull);

			if (isnull)
				elog(ERROR, "Time attribute not found in tuple");

			p->coordinates[p->num_coords++] = time_value_to_internal(datum, d->fd.column_type);
		}
		else
		{
			p->coordinates[p->num_coords++] =
				partitioning_func_apply_tuple(d->partitioning, tuple, tupdesc);
		}
	}

	return p;
}
