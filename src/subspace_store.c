#include <postgres.h>

#include "dimension.h"
#include "dimension_slice.h"
#include "subspace_store.h"

typedef struct SubspaceStore {
	MemoryContext mcxt;
	int16 num_dimensions;
	DimensionVec *origin; /* origin of the tree */
} SubspaceStore;

static inline DimensionVec *
subspace_store_dimension_create()
{
	return dimension_vec_create(DIMENSION_VEC_DEFAULT_SIZE);
}

SubspaceStore *
subspace_store_init(int16 num_dimensions, MemoryContext mcxt)
{
	MemoryContext old = MemoryContextSwitchTo(mcxt);
	SubspaceStore *sst = palloc(sizeof(SubspaceStore));
	sst->origin = subspace_store_dimension_create();
	sst->num_dimensions = num_dimensions;
	sst->mcxt = mcxt;
	MemoryContextSwitchTo(old);
	return sst;
}

static inline void
subspace_store_free_internal_node(void * node)
{
	dimension_vec_free((DimensionVec *)node);
}

void subspace_store_add(SubspaceStore *cache, const Hypercube *hc,
						void *end_store, void (*end_store_free)(void *))
{
	DimensionVec **vecptr = &cache->origin;
	DimensionSlice *last = NULL;
	MemoryContext old = MemoryContextSwitchTo(cache->mcxt);
	int i;

	Assert(hc->num_slices == cache->num_dimensions);

	for (i = 0; i < hc->num_slices; i++)
	{
		const DimensionSlice *target = hc->slices[i];
		DimensionSlice *match;
		DimensionVec *vec = *vecptr;

		Assert(target->storage == NULL);

		if (vec == NULL)
		{
			last->storage = subspace_store_dimension_create();
			last->storage_free = subspace_store_free_internal_node;
			vec = last->storage;
		}

		if (vec->num_slices > 0)
		{
			Assert(vec->slices[0]->fd.dimension_id == target->fd.dimension_id);
		}

		match = dimension_vec_find_slice(vec, target->fd.range_start);

		if (match == NULL)
		{
			DimensionSlice *copy = dimension_slice_copy(target);
			dimension_vec_add_slice_sort(vecptr, copy);
			match = copy;
		}

		last = match;
		/* internal nodes point to the next dimension's vector */
		vecptr = (DimensionVec **)&last->storage;
	}

	Assert(last->storage == NULL);
	last->storage = end_store; /* at the end we store the object */
	last->storage_free = end_store_free;
	MemoryContextSwitchTo(old);
}

void *
subspace_store_get(SubspaceStore *cache, Point *target)
{
	int i;
	DimensionVec *vec = cache->origin;
	DimensionSlice *match = NULL;

	Assert(target->cardinality == cache->num_dimensions);

	for (i = 0; i < target->cardinality; i++)
	{
		match = dimension_vec_find_slice(vec, target->coordinates[i]);

		if (NULL == match)
			return NULL;

		vec = match->storage;
	}
	return match->storage;
}

void
subspace_store_free(SubspaceStore *cache)
{
	dimension_vec_free(cache->origin);
	pfree(cache);
}

MemoryContext subspace_store_mcxt(SubspaceStore *cache)
{
	return cache->mcxt;
}
