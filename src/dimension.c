#include <postgres.h>
#include <access/relscan.h>

#include "catalog.h"
#include "dimension.h"
#include "hypertable.h"
#include "scanner.h"

static Dimension *
dimension_from_tuple(HeapTuple tuple)
{
	Dimension *d;
	d = palloc0(sizeof(Dimension));
	memcpy(&d->fd, GETSTRUCT(tuple), sizeof(FormData_dimension));

	if (OidIsValid((d)->fd.time_type))
		d->type = DIMENSION_TYPE_TIME;
	else
		d->type = DIMENSION_TYPE_SPACE;
	
	return d;
}

static bool
dimension_tuple_found(TupleInfo *ti, void *data)
{
	Hyperspace *s = data;
	Dimension *d = dimension_from_tuple(ti->tuple);

	if (IS_TIME_DIMENSION(d))
		s->time_dimensions[s->num_time_dimensions++] = d;
	else
	   	s->space_dimensions[s->num_space_dimensions++] = d;
	
	return true;
}

Hyperspace *
dimension_scan(int32 hypertable_id)
{
	Catalog    *catalog = catalog_get();
	Hyperspace *space = palloc0(sizeof(Hyperspace));
	ScanKeyData scankey[1];   
	ScannerCtx	scanCtx = {
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

	space->num_space_dimensions = space->num_space_dimensions = 0;
	
	/* Perform an index scan on schema and table. */
	ScanKeyInit(&scankey[0], Anum_dimension_hypertable_id_idx_hypertable_id,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(hypertable_id));

	scanner_scan(&scanCtx);

	return space;
}

#if defined(__MAIN__)
#include <stdio.h>

int main(int argc, char **argv)
{

	printf("Sizeof(Dimension)=%zu\n", sizeof(Dimension));
	return 0;
}
#endif
