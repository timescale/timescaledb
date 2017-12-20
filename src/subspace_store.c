#include <postgres.h>

#include "dimension.h"
#include "dimension_slice.h"
#include "dimension_vector.h"
#include "hypercube.h"
#include "subspace_store.h"

/*
 * In terms of datastructures, the subspace store is actually a tree. At the
 * root of a tree is a DimensionVec representing the different DimensionSlices
 * for the first dimension. Each of the DimensionSlices of the
 * first dimension point to a DimensionVec of the second dimension. This recurses
 * for the N dimensions. The leaf DimensionSlice points to the data being stored.
 *
 * */
typedef struct SubspaceStore
{
	MemoryContext mcxt;
	int16		num_dimensions;
/* limit growth of store by  limiting number of slices in first dimension,	0 for no limit */
	int16		max_slices_first_dimension;
	DimensionVec *origin;		/* origin of the tree */
} SubspaceStore;

static inline DimensionVec *
subspace_store_dimension_create()
{
	return dimension_vec_create(DIMENSION_VEC_DEFAULT_SIZE);
}

SubspaceStore *
subspace_store_init(int16 num_dimensions, MemoryContext mcxt, int16 max_slices_first_dimension)
{
	MemoryContext old = MemoryContextSwitchTo(mcxt);
	SubspaceStore *sst = palloc(sizeof(SubspaceStore));

	sst->origin = subspace_store_dimension_create();
	sst->num_dimensions = num_dimensions;
	sst->max_slices_first_dimension = max_slices_first_dimension;
	sst->mcxt = mcxt;
	MemoryContextSwitchTo(old);
	return sst;
}

static inline void
subspace_store_free_internal_node(void *node)
{
	dimension_vec_free((DimensionVec *) node);
}

void
subspace_store_add(SubspaceStore *store, const Hypercube *hc,
				   void *object, void (*object_free) (void *))
{
	DimensionVec **vecptr = &store->origin;
	DimensionSlice *last = NULL;
	MemoryContext old = MemoryContextSwitchTo(store->mcxt);
	int			i;

	Assert(hc->num_slices == store->num_dimensions);

	for (i = 0; i < hc->num_slices; i++)
	{
		const DimensionSlice *target = hc->slices[i];
		DimensionSlice *match;
		DimensionVec *vec = *vecptr;

		Assert(target->storage == NULL);

		if (vec == NULL)
		{
			Assert(last != NULL);
			last->storage = subspace_store_dimension_create();
			last->storage_free = subspace_store_free_internal_node;
			vec = last->storage;
		}

		Assert(0 == vec->num_slices ||
			   vec->slices[0]->fd.dimension_id == target->fd.dimension_id);

		match = dimension_vec_find_slice(vec, target->fd.range_start);

		if (match == NULL)
		{
			DimensionSlice *copy;

			if (store->max_slices_first_dimension > 0 && i == 0 && vec->num_slices >= store->max_slices_first_dimension)
			{
				/*
				 * At dimension 0 only keep store->max_slices_first_dimension
				 * slices. This is to prevent this store from growing too
				 * large. Always delete the oldest.
				 */
				Assert(store->max_slices_first_dimension == vec->num_slices);
				dimension_vec_remove_slice(vecptr, 0);
			}
			copy = dimension_slice_copy(target);

			dimension_vec_add_slice_sort(vecptr, copy);
			match = copy;
		}

		last = match;
		/* internal nodes point to the next dimension's vector */
		vecptr = (DimensionVec **) &last->storage;
	}

	Assert(last != NULL && last->storage == NULL);
	last->storage = object;		/* at the end we store the object */
	last->storage_free = object_free;
	MemoryContextSwitchTo(old);
}


void *
subspace_store_get(SubspaceStore *store, Point *target)
{
	int			i;
	DimensionVec *vec = store->origin;
	DimensionSlice *match = NULL;

	Assert(target->cardinality == store->num_dimensions);

	for (i = 0; i < target->cardinality; i++)
	{
		match = dimension_vec_find_slice(vec, target->coordinates[i]);

		if (NULL == match)
			return NULL;

		vec = match->storage;
	}
	Assert(match != NULL);
	return match->storage;
}

void
subspace_store_free(SubspaceStore *store)
{
	dimension_vec_free(store->origin);
	pfree(store);
}

MemoryContext
subspace_store_mcxt(SubspaceStore *store)
{
	return store->mcxt;
}
