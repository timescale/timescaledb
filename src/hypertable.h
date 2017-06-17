#ifndef TIMESCALEDB_HYPERTABLE_H
#define TIMESCALEDB_HYPERTABLE_H

#include <postgres.h>

#include "catalog.h"
#include "subspace_store.h"

typedef struct PartitionEpoch PartitionEpoch;
typedef struct Hyperspace Hyperspace;
typedef struct Dimension Dimension;

#define MAX_EPOCHS_PER_HYPERTABLE 20

typedef struct Hypertable
{
	FormData_hypertable fd;
	Oid         main_table_relid;
	Hyperspace *space;
	SubspaceStore *chunk_cache;
} Hypertable;

typedef struct HeapTupleData *HeapTuple;

extern Hypertable *hypertable_from_tuple(HeapTuple tuple);
extern Dimension *hypertable_get_open_dimension(Hypertable *h);
extern Dimension *hypertable_get_closed_dimension(Hypertable *h);

extern Chunk *hypertable_get_chunk(Hypertable *h, Point *point);



#endif /* TIMESCALEDB_HYPERTABLE_H */
