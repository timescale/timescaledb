#ifndef TIMESCALEDB_HYPERTABLE_H
#define TIMESCALEDB_HYPERTABLE_H

#include <postgres.h>

#include "catalog.h"

typedef struct PartitionEpoch PartitionEpoch;
typedef struct Hyperspace Hyperspace;
typedef struct Dimension Dimension;

#define MAX_EPOCHS_PER_HYPERTABLE 20

typedef struct Hypertable
{
	FormData_hypertable fd;
	Oid         main_table_relid;
	int			num_epochs;
	/* Array of PartitionEpoch. Order by start_time */
	PartitionEpoch *epochs[MAX_EPOCHS_PER_HYPERTABLE];
	Hyperspace *space;
} Hypertable;

typedef struct HeapTupleData *HeapTuple;

extern Hypertable *hypertable_from_tuple(HeapTuple tuple);
extern Dimension *hypertable_get_open_dimension(Hypertable *h);
extern Dimension *hypertable_get_closed_dimension(Hypertable *h);

#endif /* TIMESCALEDB_HYPERTABLE_H */
