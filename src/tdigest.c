/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * Interface for t-digest support
 *
 * Originally from PipelineDB 1.0.0
 * Original copyright (c) 2018, PipelineDB, Inc. and released under Apache License 2.0
 * Modifications copyright by Timescale, Inc. per NOTICE
 *
 * Original Paper by tdunning:
 * https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf
 */

#include <float.h>
#include <math.h>
#include <stdlib.h>

#include "postgres.h"
#include "tdigest.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/palloc.h"

/* scale function */
#define INTEGRATED_LOCATION(compression, q) ((compression) * (asin(2 * (q) -1) + M_PI / 2) / M_PI)

#define INTERPOLATE(x, x0, x1) (((x) - (x0)) / ((x1) - (x0)))
#define FLOAT_EQ(f1, f2) (fabs((f1) - (f2)) <= FLT_EPSILON)

typedef struct TDigestMergeArgs
{
	TDigest *t;
	Centroid *centroids;
	int index;
	float8 weight_so_far;
	float8 k1;
	float8 min;
	float8 max;
} TDigestMergeArgs;

/*
 * Based on compression level, estimates ideal number of centroids to store in input buffer
 * before merging into main centroid storage
 */
static uint32
estimate_compression_threshold(int compression)
{
	compression = Min(TDIGEST_MAX_COMPRESSION, Max(TDIGEST_MIN_COMPRESSION, compression));
	return (uint32)(7.5 + 0.37 * compression - 2e-4 * compression * compression);
}

TDigest *
ts_tdigest_create(void)
{
	return ts_tdigest_create_with_compression(TDIGEST_DEFAULT_COMPRESSION);
}

TDigest *
ts_tdigest_create_with_compression(int compression)
{
	uint32 max_centroids;
	TDigest *t;

	if (compression < TDIGEST_MIN_COMPRESSION || compression > TDIGEST_MAX_COMPRESSION)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("compression parameter should be in [%d, %d], got %d",
						TDIGEST_MIN_COMPRESSION,
						TDIGEST_MAX_COMPRESSION,
						compression)));

	max_centroids = ceil(compression * M_PI / 2) + 1;

	/* palloc0 means no manual nil'ing/setting to 0 of values necessary */
	t = palloc0(sizeof(TDigest) + max_centroids * sizeof(Centroid));

	t->compression = 1.0 * compression;
	t->threshold = estimate_compression_threshold(compression);
	t->max_centroids = max_centroids;
	t->min = INFINITY;

	SET_VARSIZE(t, ts_tdigest_size(t));

	return t;
}

void
ts_tdigest_destroy(TDigest *t)
{
	pfree(t);
}

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
 * Merges a centroid into the existing list using a scaling function
 */
static void
merge_centroid(TDigestMergeArgs *args, const Centroid *merge)
{
	float8 k2;
	Centroid *c = &args->centroids[args->index];

	/*
	 * INTEGRATED_LOCATION is used as the scaling function; based on the k_1 scaling function in the
	 * paper k2 corresponds to k(q_right) in the paper
	 */
	args->weight_so_far += merge->weight;
	k2 = INTEGRATED_LOCATION(args->t->compression, args->weight_so_far / args->t->total_weight);

	/*
	 * k1 corresponds to k(q_left) in the paper
	 * if their difference is greater than 1, the resulting merge is not doable
	 * so we move on to the next centroid
	 */
	if (k2 - args->k1 > 1 && c->weight > 0)
	{
		/* move to the next centroid in the list */
		args->index++;
		/*
		 * update the k1 value (qleft) for the next comparison by removing the effect of the
		 * unmerged 'merge' centroid weight
		 */
		args->k1 =
			INTEGRATED_LOCATION(args->t->compression,
								(args->weight_so_far - merge->weight) / args->t->total_weight);
	}

	/* we now merge the centroid at the (possibly updated) index with the incoming 'merge' centroid
	 */
	c = &args->centroids[args->index];
	c->weight += merge->weight;
	c->mean += (merge->mean - c->mean) * merge->weight / c->weight;

	/* update the properties of the being-built tdigest */
	if (merge->weight > 0)
	{
		args->min = Min(merge->mean, args->min);
		args->max = Max(merge->mean, args->max);
	}
}

/*
 * Merges the input buffer of centroids into the t-digest's stored centroids
 * Implements the merge algorithm, which is algorithm 1 of tdunning's paper
 */
TDigest *
ts_tdigest_compress(TDigest *t, List *unmerged_centroids_list)
{
	int i, j;
	int num_unmerged = list_length(unmerged_centroids_list);
	uint32 unmerged_weight = 0;
	ListCell *lc;
	Centroid *unmerged_centroids;
	TDigestMergeArgs *args;

	/* return unchanged if no unmerged centroids to add */
	if (!num_unmerged)
		return t;

	unmerged_centroids = palloc(sizeof(Centroid) * num_unmerged);

	/* Move unmerged centroids from linked list to array */
	i = 0;
	foreach (lc, unmerged_centroids_list)
	{
		Centroid *c = (Centroid *) lfirst(lc);
		memcpy(&unmerged_centroids[i], c, sizeof(Centroid));
		unmerged_weight += c->weight;
		i++;
	}

	/* only support up to 2^32 - 1 data points in a TDigest */
	if ((t->total_weight + unmerged_weight) < t->total_weight)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("too many rows added to TDigest"),
				 errhint("TDigest data structures only support up to 2^32-1 data points.")));

	list_free_deep(unmerged_centroids_list);

	/* return unchanged if centroids are 0-weight */
	if (unmerged_weight == 0)
		return t;

	t->total_weight += unmerged_weight;

	qsort(unmerged_centroids, num_unmerged, sizeof(Centroid), centroid_cmp);

	/* args will store the result of our merge until ready to copy to main t-digest */
	args = palloc0(sizeof(TDigestMergeArgs));
	args->centroids = palloc0(sizeof(Centroid) * t->max_centroids);
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

	/* Finish running merge on any remaining unmerged centroids from the list */
	while (i < num_unmerged)
		merge_centroid(args, &unmerged_centroids[i++]);

	pfree(unmerged_centroids);

	/* Finish running merge on any remaining centroids from the original t-digest */
	while (j < t->num_centroids)
		merge_centroid(args, &t->centroids[j++]);

	/* update properties of the t-digest */
	if (t->total_weight > 0)
	{
		t->min = Min(t->min, args->min);
		t->max = Max(t->max, args->max);

		if (args->centroids[args->index].weight <= 0)
			args->index--;

		t->num_centroids = args->index + 1;
	}

	Assert(t->num_centroids <= t->max_centroids);

	/* Update t-digest centroid list to reflect result of input buffer merge */
	memcpy(t->centroids, args->centroids, sizeof(Centroid) * t->max_centroids);
	pfree(args->centroids);
	pfree(args);

	SET_VARSIZE(t, ts_tdigest_size(t));

	return t;
}

/*
 * Returns approximate value of t-digest's CDF evaluated at x
 */
float8
ts_tdigest_cdf(const TDigest *t, float8 x)
{
	int i;
	float8 left, right;
	uint64 weight_so_far;
	Centroid *a, *b, tmp;

	if (t->num_centroids == 0)
		return NAN;

	if (x < t->min)
		return 0;
	if (x > t->max)
		return 1.0;

	if (t->num_centroids == 1)
	{
		if (FLOAT_EQ(t->max, t->min))
			return 0.5;

		return INTERPOLATE(x, t->min, t->max);
	}

	weight_so_far = 0;
	a = b = &tmp;
	b->mean = t->min;
	b->weight = 0;
	right = 0;

	for (i = 0; i < t->num_centroids; i++)
	{
		const Centroid *c = &t->centroids[i];

		left = b->mean - (a->mean + right);
		a = b;
		b = (Centroid *) c;
		right = (b->mean - a->mean) * a->weight / (a->weight + b->weight);

		if (x < a->mean + right)
			return Max((weight_so_far +
						a->weight * INTERPOLATE(x, a->mean - left, a->mean + right)) /
						   t->total_weight,
					   0.0);

		weight_so_far += a->weight;
	}

	left = b->mean - (a->mean + right);
	a = b;
	right = t->max - a->mean;

	if (x < a->mean + right)
		return (weight_so_far + a->weight * INTERPOLATE(x, a->mean - left, a->mean + right)) /
			   t->total_weight;

	return 1.0;
}

/*
 * Returns approximate value at the qth quantile
 * q must be in the interval [0, 1.0]
 */
float8
ts_tdigest_percentile(const TDigest *t, float8 q)
{
	int i;
	float8 left, right, index;
	uint64 weight_so_far;
	Centroid *a, *b, tmp;

	if (q < 0 || q > 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("percentile parameter should be in [0, 1.0], got %f", q)));

	if (t->num_centroids == 0)
		return NAN;

	if (t->num_centroids == 1)
		return t->centroids[0].mean;

	if (FLOAT_EQ(q, 0.0))
		return t->min;

	if (FLOAT_EQ(q, 1.0))
		return t->max;

	index = q * t->total_weight;

	weight_so_far = 0;
	b = &tmp;
	b->mean = t->min;
	b->weight = 0;
	right = t->min;

	for (i = 0; i < t->num_centroids; i++)
	{
		const Centroid *c = &t->centroids[i];
		a = b;
		left = right;

		b = (Centroid *) c;
		right = (b->weight * a->mean + a->weight * b->mean) / (a->weight + b->weight);

		if (index < weight_so_far + a->weight)
		{
			float8 p = (index - weight_so_far) / a->weight;
			return left * (1 - p) + right * p;
		}

		weight_so_far += a->weight;
	}

	left = right;
	a = b;
	right = t->max;

	if (index < weight_so_far + a->weight)
	{
		float8 p = (index - weight_so_far) / a->weight;
		return left * (1 - p) + right * p;
	}

	return t->max;
}

/*
 * Returns total amount of data points added to the tdigest
 */
int32
ts_tdigest_count(const TDigest *t)
{
	return t->total_weight;
}

TDigest *
ts_tdigest_copy(const TDigest *t)
{
	Size size = ts_tdigest_size(t);
	char *new = palloc0(size);
	memcpy(new, (char *) t, size);
	return (TDigest *) new;
}

Size
ts_tdigest_size(const TDigest *t)
{
	return (sizeof(TDigest) + sizeof(Centroid) * t->max_centroids);
}

/********************************
 * TDigest Comparison Functions *
 ********************************/

/*
 * Uses the in-memory representation to define a total order for TDigest.
 * This supports DISTINCT and ORDER BY operations when a TDigest is in a row with other data types.
 */

/* defines the total order for TDigest, based on in-memory representation */
int
ts_tdigest_cmp(const TDigest *t1, const TDigest *t2)
{
	Size t1s, t2s;
	t1s = ts_tdigest_size(t1);
	t2s = ts_tdigest_size(t2);

	/* if t1 is smaller than t2, t1 < t2 in total order */
	if (t1s != t2s)
		return t1s < t2s ? -1 : 1;

	/* if the same size, do a memcmp
	 * this works because we use palloc0, so padding is definitely 0
	 */
	return memcmp(t1, t2, t1s);
}

bool
ts_tdigest_equal(const TDigest *t1, const TDigest *t2)
{
	return ts_tdigest_cmp(t1, t2) == 0;
}

bool
ts_tdigest_lt(const TDigest *t1, const TDigest *t2)
{
	return ts_tdigest_cmp(t1, t2) < 0;
}

bool
ts_tdigest_ge(const TDigest *t1, const TDigest *t2)
{
	return ts_tdigest_cmp(t1, t2) >= 0;
}

bool
ts_tdigest_gt(const TDigest *t1, const TDigest *t2)
{
	return ts_tdigest_cmp(t1, t2) > 0;
}

bool
ts_tdigest_le(const TDigest *t1, const TDigest *t2)
{
	return ts_tdigest_cmp(t1, t2) <= 0;
}
