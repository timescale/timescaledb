#include <postgres.h>
#include <utils/lsyscache.h>

#include "insert_statement_state.h"
#include "insert_chunk_state.h"
#include "chunk.h"
#include "cache.h"
#include "hypertable_cache.h"
#include "dimension.h"
#include "dimension_slice.h"
#include "hypertable.h"
#include "chunk_constraint.h"

InsertStatementState *
insert_statement_state_new(Oid relid)
{
	MemoryContext oldctx;
	MemoryContext mctx = AllocSetContextCreate(CacheMemoryContext,
											   "Insert statement state",
											   ALLOCSET_DEFAULT_SIZES);
	InsertStatementState *state;
	Hypertable *ht;
	Cache *hypertable_cache;

	oldctx = MemoryContextSwitchTo(mctx);

	hypertable_cache = hypertable_cache_pin();

	ht = hypertable_cache_get_entry(hypertable_cache, relid);

	if (NULL == ht)
		elog(ERROR, "No hypertable for relid %d", relid);

	state = palloc(sizeof(InsertStatementState));
	state->mctx = mctx;
	state->hypertable_cache = hypertable_cache;
	state->hypertable = ht;
	state->cache = subspace_store_init(HYPERSPACE_NUM_DIMENSIONS(ht->space), mctx);

	MemoryContextSwitchTo(oldctx);

	return state;
}

void
insert_statement_state_destroy(InsertStatementState *state)
{
	subspace_store_free(state->cache);
	cache_release(state->hypertable_cache);
	MemoryContextDelete(state->mctx);
}

static void destroy_insert_chunk_state(void *ics_ptr)
{
	InsertChunkState *ics = ics_ptr;
	insert_chunk_state_destroy(ics);
}

/*
 * Get an insert context to the chunk corresponding to the partition and
 * timepoint of a tuple.
 */
extern InsertChunkState *
insert_statement_state_get_insert_chunk_state(InsertStatementState *state, Hyperspace *hs, Point *point)
{
	InsertChunkState *ics;

	ics = subspace_store_get(state->cache, point);

	if (NULL == ics)
	{
		Chunk *new_chunk;
		MemoryContext old;

		new_chunk = hypertable_get_chunk(state->hypertable, point);

		if (NULL == new_chunk)
			elog(ERROR, "No chunk found or created");

		old = MemoryContextSwitchTo(state->mctx);
		ics = insert_chunk_state_new(new_chunk);
		subspace_store_add(state->cache, new_chunk->cube, ics, destroy_insert_chunk_state);
		MemoryContextSwitchTo(old);
	}

	return ics;
}
