/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <utils/jsonb.h>
#include <utils/numeric.h>

#include "dimension_vector.h"
#include "export.h"
#include "hypercube.h"

/*
 * A hypercube represents the partition bounds of a hypertable chunk.
 *
 * A hypercube consists of N slices that each represent a range in a particular
 * dimension that make up the hypercube. When a new tuple is inserted into a
 * hypertable, and no chunk exists that can hold that tuple, we need to
 * calculate a new hypercube that encloses the point corresponding to the
 * tuple. When calculating the hypercube, we need to account for alignment
 * requirements in dimensions marked as "aligned" and also ensure that there are
 * no collisions with existing chunks. Alignment issues and collisions can occur
 * when the partitioning configuration has changed (e.g., the time interval or
 * number of partitions in a particular dimension changed).
 */
Hypercube *
ts_hypercube_alloc(int16 num_dimensions)
{
	Hypercube *hc = palloc0(HYPERCUBE_SIZE(num_dimensions));

	hc->capacity = num_dimensions;
	return hc;
}

void
ts_hypercube_free(Hypercube *hc)
{
	int i;

	for (i = 0; i < hc->num_slices; i++)
	{
		ts_dimension_slice_free(hc->slices[i]);
	}

	pfree(hc);
}

#if defined(USE_ASSERT_CHECKING)
static inline bool
hypercube_is_sorted(const Hypercube *hc)
{
	int i;

	if (hc->num_slices < 2)
	{
		return true;
	}

	for (i = 1; i < hc->num_slices; i++)
	{
		if (hc->slices[i]->fd.dimension_id < hc->slices[i - 1]->fd.dimension_id)
		{
			return false;
		}
	}

	return true;
}
#endif

Hypercube *
ts_hypercube_copy(const Hypercube *hc)
{
	Hypercube *copy;
	size_t nbytes = HYPERCUBE_SIZE(hc->capacity);
	int i;

	copy = palloc(nbytes);
	memcpy(copy, hc, nbytes);

	for (i = 0; i < hc->num_slices; i++)
	{
		copy->slices[i] = ts_dimension_slice_copy(hc->slices[i]);
	}

	return copy;
}

bool
ts_hypercube_equal(const Hypercube *hc1, const Hypercube *hc2)
{
	int i;

	if (hc1->num_slices != hc2->num_slices)
	{
		return false;
	}

	for (i = 0; i < hc1->num_slices; i++)
	{
		if (ts_dimension_slice_cmp(hc1->slices[i], hc2->slices[i]) != 0)
		{
			return false;
		}
	}

	return true;
}

static int
cmp_slices_by_dimension_id(const void *left, const void *right)
{
	const DimensionSlice *left_slice = *((DimensionSlice **) left);
	const DimensionSlice *right_slice = *((DimensionSlice **) right);

	if (left_slice->fd.dimension_id == right_slice->fd.dimension_id)
	{
		return 0;
	}
	if (left_slice->fd.dimension_id < right_slice->fd.dimension_id)
	{
		return -1;
	}
	return 1;
}

DimensionSlice *
ts_hypercube_add_slice_from_range(Hypercube *hc, int32 dimension_id, int64 start, int64 end)
{
	DimensionSlice *slice;

	Assert(hc->capacity > hc->num_slices);

	slice = ts_dimension_slice_create(dimension_id, start, end);
	hc->slices[hc->num_slices++] = slice;

	/* Check if we require a sort to maintain dimension order */
	if (hc->num_slices > 1 &&
		slice->fd.dimension_id < hc->slices[hc->num_slices - 2]->fd.dimension_id)
	{
		ts_hypercube_slice_sort(hc);
	}

	Assert(hypercube_is_sorted(hc));

	return slice;
}

DimensionSlice *
ts_hypercube_add_slice(Hypercube *hc, const DimensionSlice *slice)
{
	DimensionSlice *new_slice;

	new_slice = ts_hypercube_add_slice_from_range(hc,
												  slice->fd.dimension_id,
												  slice->fd.range_start,
												  slice->fd.range_end);
	new_slice->fd.id = slice->fd.id;
	new_slice->fd.chunk_id = slice->fd.chunk_id;

	return new_slice;
}

/*
 * Sort the hypercubes slices in ascending dimension ID order. This allows us to
 * iterate slices in a consistent order.
 */
void
ts_hypercube_slice_sort(Hypercube *hc)
{
	qsort((void *) hc->slices,
		  hc->num_slices,
		  sizeof(DimensionSlice *),
		  cmp_slices_by_dimension_id);
}

const DimensionSlice *
ts_hypercube_get_slice_by_dimension_id(const Hypercube *hc, int32 dimension_id)
{
	DimensionSlice slice = {
		.fd.dimension_id = dimension_id,
	};
	void *ptr = &slice;

	if (hc->num_slices == 0)
	{
		return NULL;
	}

	Assert(hypercube_is_sorted(hc));

	ptr = bsearch((void *) &ptr,
				  (void *) hc->slices,
				  hc->num_slices,
				  sizeof(DimensionSlice *),
				  cmp_slices_by_dimension_id);

	if (NULL == ptr)
	{
		return NULL;
	}

	return *((DimensionSlice **) ptr);
}

/*
 * Calculate the hypercube enclosing the given point.
 *
 * For aligned dimensions, take the range of any existing slice that covers
 * the point so the new chunk aligns with that range. For unaligned
 * dimensions, derive the range from the dimension's current chunk interval.
 * Each chunk owns its own dimension_slice rows; the cube only carries
 * ranges here and fresh slice rows are inserted later when the chunk is
 * created. Alignment cuts to resolve collisions happen in a later step.
 */
Hypercube *
ts_hypercube_calculate_from_point(const Hyperspace *hs, const Point *p, const ScanTupLock *tuplock)
{
	Hypercube *cube = ts_hypercube_alloc(hs->num_dimensions);

	for (int i = 0; i < hs->num_dimensions; i++)
	{
		const Dimension *dim = &hs->dimensions[i];
		int64 value = p->coordinates[i];
		bool found = false;

		/* Assert that dimensions are in ascending order */
		Assert(i == 0 || dim->fd.id > hs->dimensions[i - 1].fd.id);

		if (dim->fd.aligned)
		{
			DimensionVec *vec = ts_dimension_slice_scan_limit(dim->fd.id, value, 1, tuplock);

			if (vec->num_slices > 0)
			{
				cube->slices[i] = vec->slices[0];
				found = true;
			}
		}

		if (!found)
		{
			cube->slices[i] = ts_dimension_calculate_default_slice(dim, value);
		}
	}

	cube->num_slices = hs->num_dimensions;

	Assert(hypercube_is_sorted(cube));

	return cube;
}

/*
 * Check if two hypercubes collide (overlap).
 *
 * This is basically an axis-aligned bounding box collision detection,
 * generalized to N dimensions. We check for dimension slice collisions in each
 * dimension and only if all dimensions collide there is a hypercube collision.
 */
bool
ts_hypercubes_collide(const Hypercube *cube1, const Hypercube *cube2)
{
	int i;

	Assert(cube1->num_slices == cube2->num_slices);

	for (i = 0; i < cube1->num_slices; i++)
	{
		if (!ts_dimension_slices_collide(cube1->slices[i], cube2->slices[i]))
		{
			return false;
		}
	}

	return true;
}
