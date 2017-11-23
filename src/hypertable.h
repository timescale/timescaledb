#ifndef TIMESCALEDB_HYPERTABLE_H
#define TIMESCALEDB_HYPERTABLE_H

#include <postgres.h>
#include <nodes/primnodes.h>

#include "catalog.h"
#include "dimension.h"

typedef struct SubspaceStore SubspaceStore;
typedef struct Chunk Chunk;
typedef struct HeapTupleData *HeapTuple;

typedef struct Hypertable
{
	FormData_hypertable fd;
	Oid			main_table_relid;
	Hyperspace *space;
	SubspaceStore *chunk_cache;
} Hypertable;

extern Hypertable *hypertable_from_tuple(HeapTuple tuple);
extern int	hypertable_set_name(Hypertable *ht, const char *newname);
extern int	hypertable_set_schema(Hypertable *ht, const char *newname);
extern Oid	hypertable_id_to_relid(int32 hypertable_id);
extern Chunk *hypertable_get_chunk(Hypertable *h, Point *point);
extern Oid	hypertable_relid(RangeVar *rv);
extern bool is_hypertable(Oid relid);

#endif   /* TIMESCALEDB_HYPERTABLE_H */
