#ifndef TIMESCALEDB_HYPERTABLE_H
#define TIMESCALEDB_HYPERTABLE_H

#include <postgres.h>

#include "catalog.h"

typedef struct Hyperspace Hyperspace;
typedef struct SubspaceStore SubspaceStore;
typedef struct Point Point;
typedef struct Chunk Chunk;
typedef struct HeapTupleData *HeapTuple;

typedef struct Hypertable
{
	FormData_hypertable fd;
	Oid         main_table_relid;
	Hyperspace *space;
	SubspaceStore *chunk_cache;
} Hypertable;

extern Hypertable *hypertable_from_tuple(HeapTuple tuple);
extern Chunk *hypertable_get_chunk(Hypertable *h, Point *point);

#endif /* TIMESCALEDB_HYPERTABLE_H */
