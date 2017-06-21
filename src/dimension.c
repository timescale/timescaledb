#include <postgres.h>
#include <access/relscan.h>
#include <utils/lsyscache.h>

#include "catalog.h"
#include "dimension.h"
#include "hypertable.h"
#include "scanner.h"
#include "partitioning.h"
#include "utils.h"


static inline DimensionType
dimension_type(HeapTuple tuple)
{
	/* If there is no partitioning func set we assume open dimension */
	if (heap_attisnull(tuple, Anum_dimension_partitioning_func))
		return DIMENSION_TYPE_OPEN;
	return DIMENSION_TYPE_CLOSED;
}

static void
dimension_fill_in_from_tuple(Dimension *d, HeapTuple tuple, Oid main_table_relid)
{
	memcpy(&d->fd, GETSTRUCT(tuple), sizeof(FormData_dimension));
	d->type = dimension_type(tuple);

	if (d->type == DIMENSION_TYPE_CLOSED)
		d->partitioning = partitioning_info_create(d->fd.num_slices,
												   NameStr(d->fd.partitioning_func_schema),
												   NameStr(d->fd.partitioning_func),
												   NameStr(d->fd.column_name),
												   main_table_relid);
	d->column_attno = get_attnum(main_table_relid, NameStr(d->fd.column_name));
}

static Hyperspace *
hyperspace_create(int32 hypertable_id, Oid main_table_relid, uint16 num_dimensions)
{
	Hyperspace *hs = palloc0(HYPERSPACE_SIZE(num_dimensions));
	hs->hypertable_id = hypertable_id;
	hs->main_table_relid = main_table_relid;
	hs->capacity = num_dimensions;
	hs->num_closed_dimensions = hs->num_open_dimensions = 0;
	return hs;
}

static bool
dimension_tuple_found(TupleInfo *ti, void *data)
{
	Hyperspace *hs = data;
	DimensionType type = dimension_type(ti->tuple);
	Dimension *d;

	if (type == DIMENSION_TYPE_OPEN)
		d = &hs->dimensions[hs->num_open_dimensions++];
	else
		d = &hs->dimensions[hs->capacity - 1 - hs->num_closed_dimensions++];

	dimension_fill_in_from_tuple(d, ti->tuple, hs->main_table_relid);

	return true;
}

Hyperspace *
dimension_scan(int32 hypertable_id, Oid main_table_relid, int16 num_dimensions)
{
	Catalog    *catalog = catalog_get();
	Hyperspace *space = hyperspace_create(hypertable_id, main_table_relid, num_dimensions);
	ScanKeyData scankey[1];
	ScannerCtx  scanCtx = {
		.table = catalog->tables[DIMENSION].id,
		.index = catalog->tables[DIMENSION].index_ids[DIMENSION_HYPERTABLE_ID_IDX],
		.scantype = ScannerTypeIndex,
		.nkeys = 1,
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

	return space;
}

static Point *
point_create(int16 num_dimensions)
{
	Point *p = palloc0(POINT_SIZE(num_dimensions));
	p->cardinality = num_dimensions;
	p->num_closed = p->num_open = 0;
	return p;
}

const char *
point_to_string(Point *p)
{
	char *buf = palloc(100);
	int i, j = 1;

	buf[0] = '(';

	for (i = 0; i < p->cardinality; i++)
		j += snprintf(buf + j, 100, "" INT64_FORMAT ",", p->coordinates[i]);

	buf[j-1] = ')';

	return buf;
}

Point *
hyperspace_calculate_point(Hyperspace *hs, HeapTuple tuple, TupleDesc tupdesc)
{
	Point *p = point_create(HYPERSPACE_NUM_DIMENSIONS(hs));
	int i;

	for (i = 0; i < HYPERSPACE_NUM_DIMENSIONS(hs); i++)
	{
		Dimension *d = &hs->dimensions[i];

		if (IS_OPEN_DIMENSION(d))
		{
			Datum       datum;
			bool        isnull;

			datum = heap_getattr(tuple, d->column_attno, tupdesc, &isnull);

			if (isnull)
				elog(ERROR, "Time attribute not found in tuple");

			p->coordinates[p->num_open++] = time_value_to_internal(datum, d->fd.column_type);
		}
		else
		{
			p->coordinates[p->num_open + p->num_closed++] =
				partitioning_func_apply_tuple(d->partitioning, tuple, tupdesc);
		}
	}

	return p;
}
