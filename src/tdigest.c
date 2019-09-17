/*-------------------------------------------------------------------------
 *
 * tdigest.c
 *
 *	  t-digest implementation based on: https://github.com/tdunning/t-digest
 *
 * Implementation is based on: https://github.com/tdunning/t-digest/blob/master/src/main/java/com/tdunning/math/stats/MergingDigest.java
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include <float.h>
#include <math.h>
#include <stdlib.h>

#include "postgres.h"
#include "tdigest.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/palloc.h"

#define DEFAULT_COMPRESSION 200
#define MIN_COMPRESSION 20
#define MAX_COMPRESSION 1000

#define interpolate(x, x0, x1) (((x) - (x0)) / ((x1) - (x0)))
#define integrated_location(compression, q) ((compression) * (asin(2 * (q) - 1) + M_PI / 2) / M_PI)
#define float_eq(f1, f2) (fabs((f1) - (f2)) <= FLT_EPSILON)

typedef struct mergeArgs
{
	TDigest *t;
	Centroid *centroids;
	int idx;
	float8 weight_so_far;
	float8 k1;
	float8 min;
	float8 max;
} mergeArgs;

/*
 * estimate_compression_threshold
 */
static uint32
estimate_compression_threshold(int compression)
{
	compression = Min(1000, Max(20, compression));
	return (uint32) (7.5 + 0.37 * compression - 2e-4 * compression * compression);
}

/*
 * TDigestCreate
 */
TDigest *
TDigestCreate(void)
{
	return TDigestCreateWithCompression(DEFAULT_COMPRESSION);
}

/*
 * TDigestCreateWithCompression
 */
TDigest *
TDigestCreateWithCompression(int compression)
{
	uint32 size = ceil(compression * M_PI / 2) + 1;
	TDigest *t = palloc0(sizeof(TDigest) + size * sizeof(Centroid));

	t->compression = 1.0 * compression;
	t->threshold = estimate_compression_threshold(compression);
	t->size = size;
	t->min = INFINITY;

	SET_VARSIZE(t, TDigestSize(t));

	return t;
}

/*
 * TDigestDestroy
 */
void
TDigestDestroy(TDigest *t)
{
	if (t->unmerged_centroids != NIL)
		list_free_deep(t->unmerged_centroids);
	pfree(t);
}

/*
 * TDigestAdd
 */
TDigest *
TDigestAdd(TDigest *t, float8 x, int64 w)
{
	Centroid *c;

	c = palloc0(sizeof(Centroid));
	c->weight = w;
	c->mean = x;

	t->unmerged_centroids = lappend(t->unmerged_centroids, c);

	if (list_length(t->unmerged_centroids) > t->threshold)
		t = TDigestCompress(t);

	return t;
}

/*
 * centroid_cmp
 */
static int
centroid_cmp(const void *a, const void *b)
{
	Centroid *c1 = (Centroid *) a;
	Centroid *c2 = (Centroid *) b;
	if (c1->mean < c2->mean)
		return -1;
	if (c1->mean > c2->mean)
		return 1;
	return 0;
}

/*
 * merge_centroid
 */
static void
merge_centroid(mergeArgs *args, Centroid *merge)
{
	float8 k2;
	Centroid *c = &args->centroids[args->idx];

	args->weight_so_far += merge->weight;
	k2 = integrated_location(args->t->compression,
			args->weight_so_far / args->t->total_weight);

	if (k2 - args->k1 > 1 && c->weight > 0)
	{
		args->idx++;
		args->k1 = integrated_location(args->t->compression,
				(args->weight_so_far - merge->weight) / args->t->total_weight);
	}

	c = &args->centroids[args->idx];
	c->weight += merge->weight;
	c->mean += (merge->mean - c->mean) * merge->weight / c->weight;

	if (merge->weight > 0)
	{
		args->min = Min(merge->mean, args->min);
		args->max = Max(merge->mean, args->max);
	}
}

/*
 * We use our own reallocation function instead of just using repalloc because repalloc frees the old pointer.
 * This is problematic in the context of using TDigests in aggregates (which is their primary use case) because nodeAgg
 * automatically frees the old transition value when the pointer value changes within the transition function, which
 * would lead to a double free error if we were to free the old pointer ourselves via repalloc.
 */
static TDigest *
realloc_tdigest(TDigest *t, Size size)
{
	TDigest *result = palloc0(size);

	memcpy(result, t, TDigestSize(t));

	return result;
}

/*
 * TDigestCompress
 */
TDigest *
TDigestCompress(TDigest *t)
{
	int num_unmerged = list_length(t->unmerged_centroids);
	Centroid *unmerged_centroids;
	uint64_t unmerged_weight = 0;
	ListCell *lc;
	int i, j;
	mergeArgs *args;
	uint32_t num_centroids = t->num_centroids;

	if (!num_unmerged)
		return t;

	unmerged_centroids = palloc(sizeof(Centroid) * num_unmerged);

	i = 0;
	foreach(lc, t->unmerged_centroids)
	{
		Centroid *c = (Centroid *) lfirst(lc);
		memcpy(&unmerged_centroids[i], c, sizeof(Centroid));
		unmerged_weight += c->weight;
		i++;
	}

	list_free_deep(t->unmerged_centroids);
	t->unmerged_centroids = NIL;

	if (unmerged_weight == 0)
		return t;

	t->total_weight += unmerged_weight;

	qsort(unmerged_centroids, num_unmerged, sizeof(Centroid), centroid_cmp);

	args = palloc0(sizeof(mergeArgs));
	args->centroids = palloc0(sizeof(Centroid) * t->size);
	args->t = t;
	args->min = INFINITY;

	i = 0;
	j = 0;
	while (i < num_unmerged && j < t->num_centroids)
	{
		Centroid *a = &unmerged_centroids[i];
		Centroid *b = &t->centroids[j];

		if (a->mean <= b->mean)
		{
			merge_centroid(args, a);
			i++;
		}
		else
		{
			merge_centroid(args, b);
			j++;
		}
	}

	while (i < num_unmerged)
		merge_centroid(args, &unmerged_centroids[i++]);

	pfree(unmerged_centroids);

	while (j < t->num_centroids)
		merge_centroid(args, &t->centroids[j++]);

	if (t->total_weight > 0)
	{
		t->min = Min(t->min, args->min);

		if (args->centroids[args->idx].weight <= 0)
			args->idx--;

		t->num_centroids = args->idx + 1;
		t->max = Max(t->max, args->max);
	}

	if (t->num_centroids > num_centroids)
	{
		if (MemoryContextContains(CurrentMemoryContext, t))
			t = realloc_tdigest(t, TDigestSize(t));
		else
		{
			TDigest *new = (TDigest *) palloc(TDigestSize(t));
			memcpy(new, t, sizeof(TDigest));
			t = new;
		}
	}

	Assert(t->num_centroids <= t->size);

	memcpy(t->centroids, args->centroids, sizeof(Centroid) * t->num_centroids);
	pfree(args->centroids);
	pfree(args);

	SET_VARSIZE(t, TDigestSize(t));

	return t;
}

/*
 * TDigestMerge
 */
TDigest *
TDigestMerge(TDigest *t1, TDigest *t2)
{
	int i;

	t2 = TDigestCompress(t2);

	for (i = 0; i < t2->num_centroids; i++)
	{
		Centroid *c = &t2->centroids[i];
		t1 = TDigestAdd(t1, c->mean, c->weight);
	}

	return t1;
}

/*
 * TDigestCDF
 */
float8
TDigestCDF(TDigest *t, float8 x)
{
	int i;
	float8 left, right;
	uint64 weight_so_far;
	Centroid *a, *b, tmp;

	t = TDigestCompress(t);

	if (t->num_centroids == 0)
		return NAN;

	if (x < t->min)
		return 0;
	if (x > t->max)
		return 1;

	if (t->num_centroids == 1)
	{
		if (float_eq(t->max, t->min))
			return 0.5;

		return interpolate(x, t->min, t->max);
	}

	weight_so_far = 0;
	a = b = &tmp;
	b->mean = t->min;
	b->weight = 0;
	right = 0;

	for (i = 0; i < t->num_centroids; i++)
	{
		Centroid *c = &t->centroids[i];

		left = b->mean - (a->mean + right);
		a = b;
		b = c;
		right = (b->mean - a->mean) * a->weight / (a->weight + b->weight);
		if (x < a->mean + right)
			return Max((weight_so_far + a->weight * interpolate(x, a->mean - left, a->mean + right)) / t->total_weight, 0.0);

		weight_so_far += a->weight;
	}

	left = b->mean - (a->mean + right);
	a = b;
	right = t->max - a->mean;

	if (x < a->mean + right)
		return (weight_so_far + a->weight * interpolate(x, a->mean - left, a->mean + right)) / t->total_weight;

	return 1;
}

/*
 * TDigestQuantile
 */
float8
TDigestQuantile(TDigest *t, float8 q)
{
	int i;
	float8 left, right, idx;
	uint64 weight_so_far;
	Centroid *a, *b, tmp;

	t = TDigestCompress(t);

	if (q < 0 || q > 1)
		elog(ERROR, "q should be in [0, 1], got %f", q);

	if (t->num_centroids == 0)
		return NAN;

	if (t->num_centroids == 1)
		return t->centroids[0].mean;

	if (float_eq(q, 0.0))
		return t->min;

	if (float_eq(q, 1.0))
		return t->max;

	idx = q * t->total_weight;

	weight_so_far = 0;
	b = &tmp;
	b->mean = t->min;
	b->weight = 0;
	right = t->min;

	for (i = 0; i < t->num_centroids; i++)
	{
		Centroid *c = &t->centroids[i];
		a = b;
		left = right;

		b = c;
		right = (b->weight * a->mean + a->weight * b->mean) / (a->weight + b->weight);

		if (idx < weight_so_far + a->weight)
		{
			float8 p = (idx - weight_so_far) / a->weight;
			return left * (1 - p) + right * p;
		}

		weight_so_far += a->weight;
	}

	left = right;
	a = b;
	right = t->max;

	if (idx < weight_so_far + a->weight)
	{
		float8 p = (idx - weight_so_far) / a->weight;
		return left * (1 - p) + right * p;
	}

	return t->max;
}

/*
 * TDigestCopy
 */
TDigest *
TDigestCopy(TDigest *t)
{
	Size size = TDigestSize(t);
	char *new = palloc(size);
	memcpy(new, (char *) t, size);
	return (TDigest *) new;
}

/*
 * TDigestSize
 */
Size
TDigestSize(TDigest *t)
{
	return sizeof(TDigest) + (sizeof(Centroid) * t->num_centroids);
}
