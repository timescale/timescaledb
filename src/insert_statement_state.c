#include <postgres.h>
#include <utils/lsyscache.h>

#include "insert_statement_state.h"
#include "insert_chunk_state.h"
#include "chunk_cache.h"
#include "chunk.h"
#include "cache.h"
#include "hypertable_cache.h"
#include "partitioning.h"
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

	state = palloc(sizeof(InsertStatementState) +
				   sizeof(DimensionSlice *) * ht->space->num_open_dimensions);
	state->mctx = mctx;
	state->chunk_cache = chunk_cache_pin();
	state->hypertable_cache = hypertable_cache;
	state->hypertable = ht;

	/* Find hypertable and the time field column */
	state->num_open_dimensions = ht->space->num_open_dimensions;
	state->num_partitions = 0;
	state->cache = NULL;

	MemoryContextSwitchTo(oldctx);
	return state;
}

void
insert_statement_state_destroy(InsertStatementState *state)
{
	int			i;

	for (i = 0; i < state->num_partitions; i++)
	{
		if (state->cstates[i] != NULL)
			insert_chunk_state_destroy(state->cstates[i]);
	}

	subspace_store_free(state->cache);

	cache_release(state->chunk_cache);
	cache_release(state->hypertable_cache);

	MemoryContextDelete(state->mctx);
}

/*
 * The insert statement state is valid iif the point is in all open dimension
 * slices.
 */
static bool
insert_statement_state_is_valid_for_point(InsertStatementState *state, Point *p)
{
	int i;

	for (i = 0; i < state->num_open_dimensions; i++)
	{
		int64 coord = point_get_open_dimension_coordinate(p, i);
		DimensionSlice *slice = state->open_dimensions_slices[i];

		if (!point_coordinate_is_in_slice(&slice->fd, coord))
			return false;
	}
	return true;
}

static void destroy_ics(void *ics_ptr)
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

	if (NULL == state->cache)
		state->cache = subspace_store_init(point->cardinality);
	
	ics = subspace_store_get(state->cache, point);

	if (NULL == ics)
	{
		Chunk *new_chunk;

		new_chunk = hypertable_get_chunk(state->hypertable, point);

		if (NULL == new_chunk)
			elog(ERROR, "No chunk found or created");

		dimension_slice_scan(hs->open_dimensions[0]->fd.id, point->coordinates[0]);

		ics = insert_chunk_state_new(new_chunk);
        subspace_store_add(state->cache, new_chunk->cube, ics, destroy_ics);
	}

	return ics;
}
