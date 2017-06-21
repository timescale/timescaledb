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

/*
 * TODO
 */

typedef struct InsertStateCache {
  int16 num_dimensions;
  DimensionAxis *origin; //origin of the tree
} InsertStateCache;

static DimensionAxis *
insert_state_cache_dimension_create() 
{
	/* TODO remove type from axis */
	return dimension_axis_create(DIMENSION_TYPE_OPEN, 10);
}

static void
insert_state_cache_init(InsertStateCache *cache, int16 num_dimensions) 
{
	cache->origin = insert_state_cache_dimension_create();
	cache->num_dimensions = num_dimensions;
}

static void 
insert_state_cache_free_internal_node(void * node) 
{
	dimension_axis_free((DimensionAxis *)node);
}

static void insert_state_cache_add(InsertStateCache *cache, Hypercube *hc,
								   void *end_store, void (*end_store_free)(void *))
{
	DimensionAxis *axis = cache->origin;
	DimensionSlice *last = NULL;
	int i;
	
	Assert(hc->num_slices == cache->num_dimensions);

	for (i = 0; i < hc->num_slices; i++)
	{
		DimensionSlice *target = hc->slices[i];
		DimensionSlice *match;

		Assert(target->storage == NULL);
		
		if (axis == NULL)
		{
			last->storage = insert_state_cache_dimension_create();
			last->storage_free = insert_state_cache_free_internal_node;
			axis = last->storage;
		}
		
		match = dimension_axis_find_slice(axis, target->fd.range_start);
		
		if (match == NULL) 
		{
			dimension_axis_add_slice_sort(&axis, target);
			match = target;
		}

		last = match;
		axis = last->storage; /* Internal nodes point to the next Dimension's Axis */ 
	}
	
	last->storage = end_store; /* at the end we store the object */
	last->storage_free = end_store_free;
}

static void *
insert_state_cache_get(InsertStateCache *cache, Point *target)
{
	int16 i;
	DimensionAxis *axis = cache->origin;
	DimensionSlice *match = NULL;
	
	Assert(target->cardinality == cache->num_dimensions);

	for (i = 0; i < target->cardinality; i++)
	{
		match = dimension_axis_find_slice(axis, target->coordinates[i]);

		if (NULL == match) 
			return NULL;

		axis = match->storage;
	}
	return match->storage;
}

static bool
insert_state_cache_match_first(InsertStateCache *cache, Point *target)
{
	Assert(target->cardinality == cache->num_dimensions);
	return (dimension_axis_find_slice(cache->origin, target->coordinates[0]) != NULL);
}

static void
insert_state_cache_free(InsertStateCache *cache)
{
	dimension_axis_free(cache->origin);
	pfree(cache);
}


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

	insert_state_cache_free(state->cache);

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
	{
		state->cache = palloc(sizeof(InsertStateCache));
		insert_state_cache_init(state->cache, point->cardinality);
	}
	
	ics = insert_state_cache_get(state->cache, point);

	if (NULL == ics) 
	{
		Chunk * new_chunk;
		Hypercube *hc;

		elog(WARNING, "LOOKUP");
		
		/* NOTE: assumes 1 or 2 dims */
		new_chunk = chunk_get_or_create(hs, point);

		if (NULL == new_chunk)
			elog(ERROR, "No chunk found or created");
				
		ics = insert_chunk_state_new(new_chunk);
		chunk_constraint_scan(new_chunk);
		hc = hypercube_from_constraints(new_chunk->constraints, new_chunk->num_constraints);
        insert_state_cache_add(state->cache, hc, ics, destroy_ics);
	}
	
	return ics;
}
