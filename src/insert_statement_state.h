#ifndef TIMESCALEDB_INSERT_STATEMENT_STATE_H
#define TIMESCALEDB_INSERT_STATEMENT_STATE_H

#include "postgres.h"
#include "insert_chunk_state.h"
#include "hypertable_cache.h"
#include "cache.h"

/* State used for every tuple in an insert statement */
typedef struct
{
	InsertChunkState **cstates; /* keep an open state for the most recently
								 * accessed chunk per partition */
	Cache	   *chunk_cache;
	MemoryContext mctx;
	Cache	   *hypertable_cache;
	Hypertable *hypertable;
	AttrNumber	time_attno;
	int			num_partitions;
} InsertStatementState;

typedef struct Partition Partition;
InsertStatementState *insert_statement_state_new(Oid);
void		insert_statement_state_destroy(InsertStatementState *);
InsertChunkState *insert_statement_state_get_insert_chunk_state(InsertStatementState *cache, Partition *partition, struct PartitionEpoch *epoch, int64 timepoint);

#endif   /* TIMESCALEDB_INSERT_STATEMENT_STATE_H */
