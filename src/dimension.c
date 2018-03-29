#include <postgres.h>
#include <catalog/pg_type.h>
#include <access/relscan.h>
#include <commands/tablecmds.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <utils/builtins.h>
#include <utils/timestamp.h>
#include <funcapi.h>
#include <miscadmin.h>
#ifdef _WIN32
#include <stdint.h>
#endif

#include "catalog.h"
#include "compat.h"
#include "dimension.h"
#include "dimension_slice.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "partitioning.h"
#include "scanner.h"
#include "utils.h"
#include "errors.h"

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
hyperspace_get_dimension_by_name(Hyperspace *hs, DimensionType type, const char *name)
{
	int			i;

	for (i = 0; i < hs->num_dimensions; i++)
	{
		Dimension  *dim = &hs->dimensions[i];

		if ((type == DIMENSION_TYPE_ANY || dim->type == type) &&
			namestrcmp(&dim->fd.column_name, name) == 0)
			return dim;
	}

	return NULL;
}

Dimension *
hyperspace_get_dimension(Hyperspace *hs, DimensionType type, Index n)
{
	int			i;

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

static int
hyperspace_get_num_dimensions_by_type(Hyperspace *hs, DimensionType type)
{
	int			i;
	int			n = 0;

	for (i = 0; i < hs->num_dimensions; i++)
	{
		if (type == DIMENSION_TYPE_ANY || hs->dimensions[i].type == type)
			n++;
	}

	return n;
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

		d->partitioning = partitioning_info_create(NameStr(d->fd.partitioning_func_schema),
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

static DimensionSlice *
calculate_open_range_default(Dimension *dim, int64 value)
{
	int64		range_start,
				range_end;

	if (value < 0)
	{
		range_end = ((value + 1) / dim->fd.interval_length) * dim->fd.interval_length;

		/* prevent integer underflow */
		if (DIMENSION_SLICE_MINVALUE - range_end > -dim->fd.interval_length)
		{
			range_start = DIMENSION_SLICE_MINVALUE;
		}
		else
		{
			range_start = range_end - dim->fd.interval_length;
		}
	}
	else
	{
		range_start = (value / dim->fd.interval_length) * dim->fd.interval_length;

		/* prevent integer overflow */
		if (DIMENSION_SLICE_MAXVALUE - range_start < dim->fd.interval_length)
		{
			range_end = DIMENSION_SLICE_MAXVALUE;
		}
		else
		{
			range_end = range_start + dim->fd.interval_length;
		}
	}

	return dimension_slice_create(dim->fd.id, range_start, range_end);
}

TS_FUNCTION_INFO_V1(dimension_calculate_open_range_default);

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
	int64		interval = DIMENSION_SLICE_CLOSED_MAX / ((int64) dim->fd.num_slices);
	int64		last_start = interval * (dim->fd.num_slices - 1);

	if (value < 0)
		elog(ERROR, "Invalid value " INT64_FORMAT " for closed dimension", value);

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

	return dimension_slice_create(dim->fd.id, range_start, range_end);
}

TS_FUNCTION_INFO_V1(dimension_calculate_closed_range_default);

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

static int
dimension_scan_internal(ScanKeyData *scankey,
						int nkeys,
						tuple_found_func tuple_found,
						void *data,
						int limit,
						LOCKMODE lockmode)
{
	Catalog    *catalog = catalog_get();
	ScannerCtx	scanctx = {
		.table = catalog->tables[DIMENSION].id,
		.index = catalog->tables[DIMENSION].index_ids[DIMENSION_HYPERTABLE_ID_IDX],
		.nkeys = nkeys,
		.limit = limit,
		.scankey = scankey,
		.data = data,
		.tuple_found = tuple_found,
		.lockmode = lockmode,
		.scandirection = ForwardScanDirection,
	};

	return scanner_scan(&scanctx);
}

Hyperspace *
dimension_scan(int32 hypertable_id, Oid main_table_relid, int16 num_dimensions)
{
	Hyperspace *space = hyperspace_create(hypertable_id, main_table_relid, num_dimensions);
	ScanKeyData scankey[1];

	/* Perform an index scan on hypertable_id. */
	ScanKeyInit(&scankey[0], Anum_dimension_hypertable_id_idx_hypertable_id,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(hypertable_id));

	dimension_scan_internal(scankey, 1, dimension_tuple_found,
							space, num_dimensions, AccessShareLock);

	/* Sort dimensions in ascending order to allow binary search lookups */
	qsort(space->dimensions, space->num_dimensions, sizeof(Dimension), cmp_dimension_id);

	return space;
}

DimensionVec *
dimension_get_slices(Dimension *dim)
{
	return dimension_slice_scan_by_dimension(dim->fd.id, 0);
}

static int
dimension_scan_update(int32 dimension_id, tuple_found_func tuple_found, void *data, LOCKMODE lockmode)
{
	Catalog    *catalog = catalog_get();
	ScanKeyData scankey[1];
	ScannerCtx	scanctx = {
		.table = catalog->tables[DIMENSION].id,
		.index = catalog->tables[DIMENSION].index_ids[DIMENSION_ID_IDX],
		.nkeys = 1,
		.limit = 1,
		.scankey = scankey,
		.data = data,
		.tuple_found = tuple_found,
		.lockmode = lockmode,
		.scandirection = ForwardScanDirection,
	};

	ScanKeyInit(&scankey[0], Anum_dimension_id_idx_id,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(dimension_id));

	return scanner_scan(&scanctx);
}

static bool
dimension_tuple_delete(TupleInfo *ti, void *data)
{
	CatalogSecurityContext sec_ctx;
	bool		isnull;
	Datum		dimension_id = heap_getattr(ti->tuple, Anum_dimension_id, ti->desc, &isnull);
	bool	   *delete_slices = data;

	Assert(!isnull);

	/* delete dimension slices */
	if (NULL != delete_slices && *delete_slices)
		dimension_slice_delete_by_dimension_id(DatumGetInt32(dimension_id), false);

	catalog_become_owner(catalog_get(), &sec_ctx);
	catalog_delete(ti->scanrel, ti->tuple);
	catalog_restore_user(&sec_ctx);

	return true;
}

int
dimension_delete_by_hypertable_id(int32 hypertable_id, bool delete_slices)
{
	ScanKeyData scankey[1];

	/* Perform an index scan to delete based on hypertable_id */
	ScanKeyInit(&scankey[0], Anum_dimension_hypertable_id_idx_hypertable_id,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(hypertable_id));

	return dimension_scan_internal(scankey, 1, dimension_tuple_delete,
								   &delete_slices, 0, RowExclusiveLock);
}

static bool
dimension_tuple_update(TupleInfo *ti, void *data)
{
	Dimension  *dim = data;
	HeapTuple	tuple;
	Datum		values[Natts_dimension];
	bool		nulls[Natts_dimension];
	CatalogSecurityContext sec_ctx;

	heap_deform_tuple(ti->tuple, ti->desc, values, nulls);

	values[Anum_dimension_column_name - 1] = NameGetDatum(&dim->fd.column_name);
	values[Anum_dimension_column_type - 1] = ObjectIdGetDatum(dim->fd.column_type);
	values[Anum_dimension_num_slices - 1] = Int16GetDatum(dim->fd.num_slices);

	if (!nulls[Anum_dimension_partitioning_func - 1] &&
		!nulls[Anum_dimension_partitioning_func_schema - 1])
	{
		values[Anum_dimension_partitioning_func - 1] = NameGetDatum(&dim->fd.partitioning_func);
		values[Anum_dimension_partitioning_func_schema - 1] = NameGetDatum(&dim->fd.partitioning_func_schema);
	}

	if (!nulls[Anum_dimension_interval_length - 1])
		values[Anum_dimension_interval_length - 1] = Int64GetDatum(dim->fd.interval_length);

	tuple = heap_form_tuple(ti->desc, values, nulls);

	catalog_become_owner(catalog_get(), &sec_ctx);
	catalog_update_tid(ti->scanrel, &ti->tuple->t_self, tuple);
	catalog_restore_user(&sec_ctx);

	return false;
}

static void
dimension_insert_relation(Relation rel, int32 hypertable_id,
						  Name colname, Oid coltype, int16 num_slices,
						  regproc partitioning_func, int64 interval_length)
{
	TupleDesc	desc = RelationGetDescr(rel);
	Datum		values[Natts_dimension];
	bool		nulls[Natts_dimension] = {false};
	CatalogSecurityContext sec_ctx;

	values[Anum_dimension_hypertable_id - 1] = Int32GetDatum(hypertable_id);
	values[Anum_dimension_column_name - 1] = NameGetDatum(colname);
	values[Anum_dimension_column_type - 1] = ObjectIdGetDatum(coltype);

	if (OidIsValid(partitioning_func))
	{
		Oid			pronamespace = get_func_namespace(partitioning_func);

		values[Anum_dimension_partitioning_func - 1] =
			DirectFunctionCall1(namein, CStringGetDatum(get_func_name(partitioning_func)));
		values[Anum_dimension_partitioning_func_schema - 1] =
			DirectFunctionCall1(namein, CStringGetDatum(get_namespace_name(pronamespace)));
		values[Anum_dimension_num_slices - 1] = Int16GetDatum(num_slices);
		values[Anum_dimension_aligned - 1] = BoolGetDatum(false);
		nulls[Anum_dimension_interval_length - 1] = true;
	}
	else
	{
		values[Anum_dimension_interval_length - 1] = Int64GetDatum(interval_length);
		values[Anum_dimension_aligned - 1] = BoolGetDatum(true);
		nulls[Anum_dimension_num_slices - 1] = true;
		nulls[Anum_dimension_partitioning_func - 1] = true;
		nulls[Anum_dimension_partitioning_func_schema - 1] = true;
	}

	catalog_become_owner(catalog_get(), &sec_ctx);
	values[Anum_dimension_id - 1] = Int32GetDatum(catalog_table_next_seq_id(catalog_get(), DIMENSION));
	catalog_insert_values(rel, desc, values, nulls);
	catalog_restore_user(&sec_ctx);
}

static void
dimension_insert(int32 hypertable_id,
				 Name colname,
				 Oid coltype,
				 int16 num_slices,
				 regproc partitioning_func,
				 int64 interval_length)
{
	Catalog    *catalog = catalog_get();
	Relation	rel;

	rel = heap_open(catalog->tables[DIMENSION].id, RowExclusiveLock);
	dimension_insert_relation(rel, hypertable_id, colname, coltype, num_slices, partitioning_func, interval_length);
	heap_close(rel, RowExclusiveLock);
}

int
dimension_set_type(Dimension *dim, Oid newtype)
{
	dim->fd.column_type = newtype;

	return dimension_scan_update(dim->fd.id, dimension_tuple_update, dim, RowExclusiveLock);
}

int
dimension_set_name(Dimension *dim, const char *newname)
{
	namestrcpy(&dim->fd.column_name, newname);

	return dimension_scan_update(dim->fd.id, dimension_tuple_update, dim, RowExclusiveLock);
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
				ereport(ERROR,
						(errcode(ERRCODE_NOT_NULL_VIOLATION),
						 errmsg("null value in column \"%s\" violates not-null constraint",
								NameStr(d->fd.column_name)),
						 errhint("Columns used for time partitioning can not be NULL")));

			p->coordinates[p->num_coords++] = time_value_to_internal(datum, d->fd.column_type, false);
		}
		else
		{
			p->coordinates[p->num_coords++] =
				partitioning_func_apply_tuple(d->partitioning, tuple, tupdesc);
		}
	}

	return p;
}

static inline int64
interval_to_usec(Interval *interval)
{
	return (interval->month * DAYS_PER_MONTH * USECS_PER_DAY)
		+ (interval->day * USECS_PER_DAY)
		+ interval->time;
}

#define IS_INTEGER_TYPE(type)							\
	(type == INT2OID || type == INT4OID || type == INT8OID)

#define IS_TIMESTAMP_TYPE(type)									\
	(type == TIMESTAMPOID || type == TIMESTAMPTZOID || type == DATEOID)

#define IS_VALID_OPEN_DIM_TYPE(type)					\
	(IS_INTEGER_TYPE(type) || IS_TIMESTAMP_TYPE(type))

#define INT_TYPE_MAX(type)												\
	(int64)((type == INT2OID) ? INT16_MAX : ((type == INT4OID) ? INT32_MAX : INT64_MAX))

#define IS_VALID_NUM_SLICES(num_slices)					\
	((num_slices) >= 1 && (num_slices) <= INT16_MAX)

static int64
get_validated_integer_interval(Oid coltype, int64 value)
{
	if (value < 1 || value > INT_TYPE_MAX(coltype))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid interval: must be between 1 and " INT64_FORMAT,
						INT_TYPE_MAX(coltype))));

	if (IS_TIMESTAMP_TYPE(coltype) && value < USECS_PER_SEC)
		ereport(WARNING,
				(errcode(ERRCODE_AMBIGUOUS_PARAMETER),
				 errmsg("unexpected interval: smaller than one second"),
				 errhint("The interval is specified in microseconds")));

	return value;
}

static int64
dimension_interval_to_internal(const char *colname, Oid coltype, Oid valuetype, Datum value)
{
	int64		interval;

	if (!IS_VALID_OPEN_DIM_TYPE(coltype))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("invalid dimension type: \"%s\" must be an integer, date or timestamp",
						colname)));

	if (!OidIsValid(valuetype))
	{
		if (IS_INTEGER_TYPE(coltype))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("integer dimensions require an explicit interval")));

		value = Int64GetDatum(DEFAULT_CHUNK_TIME_INTERVAL);
		valuetype = INT8OID;
	}

	switch (valuetype)
	{
		case INT2OID:
			interval = get_validated_integer_interval(coltype, DatumGetInt16(value));
			break;
		case INT4OID:
			interval = get_validated_integer_interval(coltype, DatumGetInt32(value));
			break;
		case INT8OID:
			interval = get_validated_integer_interval(coltype, DatumGetInt64(value));
			break;
		case INTERVALOID:
			if (IS_INTEGER_TYPE(coltype))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid interval: must be an integer type for integer dimensions")));

			interval = interval_to_usec(DatumGetIntervalP(value));
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid interval: must be an interval or integer type")));
	}

	if (coltype == DATEOID &&
		(interval <= 0 || interval % USECS_PER_DAY != 0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid interval: must be multiples of one day")));

	return interval;
}

TS_FUNCTION_INFO_V1(dimension_interval_to_internal_test);

/*
 * Exposed for testing purposes.
 */
Datum
dimension_interval_to_internal_test(PG_FUNCTION_ARGS)
{
	Oid			coltype = PG_GETARG_OID(0);
	Datum		value = PG_GETARG_DATUM(1);
	Oid			valuetype = PG_ARGISNULL(1) ? InvalidOid : get_fn_expr_argtype(fcinfo->flinfo, 1);

	PG_RETURN_INT64(dimension_interval_to_internal("testcol", coltype, valuetype, value));
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
			(errmsg("adding NOT NULL constraint to column \"%s\"", colname),
			 errdetail("Time dimensions cannot have NULL values")));

	AlterTableInternal(table_relid, list_make1(&cmd), false);
}

static void
dimension_update(FunctionCallInfo fcinfo,
				 Oid table_relid,
				 Name dimname,
				 DimensionType dimtype,
				 Datum *interval,
				 int16 *num_slices)
{
	Cache	   *hcache = hypertable_cache_pin();
	Hypertable *ht;
	Dimension  *dim;

	if (dimtype == DIMENSION_TYPE_ANY)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid dimension type")));

	ht = hypertable_cache_get_entry(hcache, table_relid);

	if (NULL == ht)
		ereport(ERROR,
				(errcode(ERRCODE_IO_HYPERTABLE_NOT_EXIST),
				 errmsg("table \"%s\" is not a hypertable",
						get_rel_name(table_relid))));

	if (NULL == dimname)
	{
		if (hyperspace_get_num_dimensions_by_type(ht->space, dimtype) > 1)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("hypertable \"%s\" has multiple %s dimensions",
							get_rel_name(table_relid),
							dimtype == DIMENSION_TYPE_OPEN ? "time" : "space"),
					 errhint("An explicit dimension name needs to be specified")));

		dim = hyperspace_get_dimension(ht->space, dimtype, 0);
	}
	else
		dim = hyperspace_get_dimension_by_name(ht->space, dimtype, NameStr(*dimname));

	if (NULL == dim)
		ereport(ERROR,
				(errcode(ERRCODE_IO_DIMENSION_NOT_EXIST),
				 errmsg("hypertable \"%s\" does not have a matching dimension",
						get_rel_name(table_relid))));

	Assert(dim->type == dimtype);

	if (NULL != interval)
	{
		Oid			intervaltype = get_fn_expr_argtype(fcinfo->flinfo, 1);

		dim->fd.interval_length = dimension_interval_to_internal(NameStr(dim->fd.column_name),
																 dim->fd.column_type,
																 intervaltype,
																 *interval);
	}

	if (NULL != num_slices)
		dim->fd.num_slices = *num_slices;

	dimension_scan_update(dim->fd.id, dimension_tuple_update, dim, RowExclusiveLock);

	cache_release(hcache);
}

TS_FUNCTION_INFO_V1(dimension_set_num_slices);

Datum
dimension_set_num_slices(PG_FUNCTION_ARGS)
{
	Oid			table_relid = PG_GETARG_OID(0);
	int32		num_slices_arg = PG_ARGISNULL(1) ? -1 : PG_GETARG_INT32(1);
	Name		colname = PG_ARGISNULL(2) ? NULL : PG_GETARG_NAME(2);
	int16		num_slices;

	hypertable_permissions_check(table_relid, GetUserId());

	if (PG_ARGISNULL(1) || !IS_VALID_NUM_SLICES(num_slices_arg))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid number of partitions: must be between 1 and %d", INT16_MAX)));

	/*
	 * Our catalog stores num_slices as a smallint (int16). However, function
	 * argument is an integer (int32) so that the user need not cast it to a
	 * smallint. We therefore convert to int16 here after checking that
	 * num_slices cannot be > INT16_MAX.
	 */
	num_slices = num_slices_arg & 0xffff;

	dimension_update(fcinfo, table_relid, colname, DIMENSION_TYPE_CLOSED, NULL, &num_slices);

	PG_RETURN_VOID();
}

TS_FUNCTION_INFO_V1(dimension_set_interval);

Datum
dimension_set_interval(PG_FUNCTION_ARGS)
{
	Oid			table_relid = PG_GETARG_OID(0);
	Datum		interval = PG_GETARG_DATUM(1);
	Name		colname = PG_ARGISNULL(2) ? NULL : PG_GETARG_NAME(2);

	hypertable_permissions_check(table_relid, GetUserId());

	if (PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid interval: an explicit interval must be specified")));

	dimension_update(fcinfo, table_relid, colname, DIMENSION_TYPE_OPEN, &interval, NULL);

	PG_RETURN_VOID();
}

void
dimension_validate_info(DimensionInfo *info)
{
	Dimension  *dim;
	HeapTuple	tuple;
	Datum		datum;
	bool		isnull = false;
	bool		not_null_is_set;

	if (!DIMENSION_INFO_IS_SET(info))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid dimension info")));

	/* Check that the column exists and get its NOT NULL status */
	tuple = SearchSysCacheAttName(info->table_relid, NameStr(*info->colname));

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" does not exist",
						NameStr(*info->colname))));

	datum = SysCacheGetAttr(ATTNAME,
							tuple,
							Anum_pg_attribute_atttypid,
							&isnull);
	Assert(!isnull);

	info->coltype = DatumGetObjectId(datum);

	datum = SysCacheGetAttr(ATTNAME,
							tuple,
							Anum_pg_attribute_attnotnull,
							&isnull);
	Assert(!isnull);

	not_null_is_set = DatumGetBool(datum);

	ReleaseSysCache(tuple);

	if (NULL != info->ht)
	{
		/* Check if the dimension already exists */
		dim = hyperspace_get_dimension_by_name(info->ht->space,
											   DIMENSION_TYPE_ANY,
											   NameStr(*info->colname));

		if (NULL != dim)
		{
			if (!info->if_not_exists)
				ereport(ERROR,
						(errcode(ERRCODE_IO_DUPLICATE_DIMENSION),
						 errmsg("column \"%s\" is already a dimension",
								NameStr(*info->colname))));

			info->skip = true;

			ereport(NOTICE,
					(errmsg("column \"%s\" is already a dimension, skipping",
							NameStr(*info->colname))));
			return;
		}
	}

	if (info->num_slices_is_set)
	{
		/* Closed ("space") dimension */
		info->type = DIMENSION_TYPE_CLOSED;

		if (!OidIsValid(info->partitioning_func))
			info->partitioning_func = partitioning_func_get_default();
		else if (!partitioning_func_is_valid(info->partitioning_func))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
					 errmsg("invalid partitioning function: must have the signature (anyelement) -> integer")));

		if (!IS_VALID_NUM_SLICES(info->num_slices))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid number of partitions: must be between 1 and %d", INT16_MAX)));
	}
	else
	{
		/* Open ("time") dimension */
		info->type = DIMENSION_TYPE_OPEN;
		info->set_not_null = !not_null_is_set;
		info->interval = dimension_interval_to_internal(NameStr(*info->colname),
														info->coltype,
														info->interval_type,
														info->interval_datum);
	}
}

void
dimension_add_from_info(DimensionInfo *info)
{
	if (info->set_not_null)
		dimension_add_not_null_on_column(info->table_relid, NameStr(*info->colname));

	Assert(info->ht != NULL);

	dimension_insert(info->ht->fd.id, info->colname, info->coltype,
					 info->num_slices, info->partitioning_func, info->interval);
}

TS_FUNCTION_INFO_V1(dimension_add);

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
dimension_add(PG_FUNCTION_ARGS)
{
	Cache	   *hcache = hypertable_cache_pin();
	DimensionInfo info = {
		.table_relid = PG_GETARG_OID(0),
		.colname = PG_ARGISNULL(1) ? NULL : PG_GETARG_NAME(1),
		.num_slices = PG_ARGISNULL(2) ? DatumGetInt32(-1) : PG_GETARG_INT32(2),
		.num_slices_is_set = !PG_ARGISNULL(2),
		.interval_datum = PG_ARGISNULL(3) ? DatumGetInt32(-1) : PG_GETARG_DATUM(3),
		.interval_type = PG_ARGISNULL(3) ? InvalidOid : get_fn_expr_argtype(fcinfo->flinfo, 3),
		.partitioning_func = PG_ARGISNULL(4) ? InvalidOid : PG_GETARG_OID(4),
		.if_not_exists = PG_ARGISNULL(5) ? false : PG_GETARG_BOOL(5),
	};

	hypertable_permissions_check(info.table_relid, GetUserId());

	/*
	 * The hypertable catalog table has a CHECK(num_dimensions > 0), which
	 * means, that when this function is called from create_hypertable()
	 * instaed of directly, num_dimension is already set to one. We therefore
	 * need to lock the hypertable tuple here so that we can set the correct
	 * number of dimensions once we've added the new dimension
	 */
	if (!hypertable_lock_tuple_simple(info.table_relid))
		ereport(ERROR,
				(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
				 errmsg("could not lock hypertable \"%s\" for update",
						get_rel_name(info.table_relid))));

	info.ht = hypertable_cache_get_entry(hcache, info.table_relid);

	if (NULL == info.ht)
		ereport(ERROR,
				(errcode(ERRCODE_IO_HYPERTABLE_NOT_EXIST),
				 errmsg("table \"%s\" is not a hypertable",
						get_rel_name(info.table_relid))));

	if ((!info.num_slices_is_set && !OidIsValid(info.interval_type)) ||
		(info.num_slices_is_set && OidIsValid(info.interval_type)))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot specify both the number of partitions and an interval")));

	dimension_validate_info(&info);

	if (!info.skip)
	{
		if (hypertable_has_tuples(info.table_relid, AccessShareLock))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("hypertable \"%s\" is not empty", get_rel_name(info.table_relid)),
					 errdetail("It is not possible to add dimensions to a non-empty hypertable")));

		/*
		 * Note that space->num_dimensions reflects the actual number of
		 * dimension rows and not the num_dimensions in the hypertable catalog
		 * table.
		 */
		hypertable_set_num_dimensions(info.ht, info.ht->space->num_dimensions + 1);
		dimension_add_from_info(&info);
	}

	cache_release(hcache);

	PG_RETURN_VOID();
}
