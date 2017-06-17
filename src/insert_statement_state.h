#ifndef TIMESCALEDB_INSERT_STATEMENT_STATE_H
#define TIMESCALEDB_INSERT_STATEMENT_STATE_H

#include "postgres.h"
#include "insert_chunk_state.h"
#include "hypertable_cache.h"
#include "cache.h"
#include "subspace_store.h"


typedef struct Hyperspace Hyperspace;
typedef struct DimensionSlice DimensionSlice;
typedef struct DimensionAxis DimensionAxis;
typedef struct Point Point;
typedef struct InsertStateCache InsertStateCache;
/* State used for every tuple in an insert statement */
typedef struct
{
	InsertChunkState **cstates; /* keep an open state for the most recently
								 * accessed chunk per partition */
	Cache	   *chunk_cache;
	MemoryContext mctx;
	Cache	   *hypertable_cache;
	Hypertable *hypertable;
	int			num_partitions;
	int num_open_dimensions;
	DimensionSlice *open_dimensions_slices[0];
	SubspaceStore *cache; 
} InsertStatementState;

InsertStatementState *insert_statement_state_new(Oid);
void		insert_statement_state_destroy(InsertStatementState *);
InsertChunkState *insert_statement_state_get_insert_chunk_state(InsertStatementState *cache, Hyperspace *hs, Point *point);




#endif   /* TIMESCALEDB_INSERT_STATEMENT_STATE_H */
