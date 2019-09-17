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
 */

#ifndef TDIGEST_H
#define TDIGEST_H

#include "c.h"
#include "nodes/pg_list.h"

#define TDIGEST_DEFAULT_COMPRESSION 200
#define TDIGEST_MIN_COMPRESSION 20
#define TDIGEST_MAX_COMPRESSION 1000

#ifndef M_PI
#define M_PI 3.14159265358979323846264338327950288
#endif

/*
 * A centroid is the fundamental unit of t-digest, and it represents an
 * aggregate data values by storing the mean of the values and how many
 * values contribute to that mean.
 */
typedef struct Centroid
{
	uint32 weight; /* number of samples contributing to this centroid's mean */
	float8 mean;   /* the average value of the samples represented by this centroid */
} Centroid;

/*
 * A t-digest structure holds all the centroids that represent a given
 * dataset, as well as a few extra values for use in maintaining the data
 * structure effciently.
 */
typedef struct TDigest
{
	uint32 vl_len_; /* for postgres variable length data type */

	float8 compression; /* increasing compressing lowers memory usage and decreases accuracy */
	uint32 threshold;   /* maximum number of centroids to store before automatically compressing */
	uint32 max_centroids; /* max number of centroids that can be stored in the t-digest */

	uint32 total_weight; /* total num of merged samples represented by the t-digest */
	float8 min;			 /* lowest centroid mean in the t-digest */
	float8 max;			 /* highest centroid mean in the t-digest */

	uint32 num_centroids; /* total number of currently stored merged centroids (does not include
						   input buffer) */
	Centroid centroids[FLEXIBLE_ARRAY_MEMBER]; /* storage for merged centroids */

} TDigest;

/*
 * Functions to allocate, destroy, and copy a t-digest
 */
extern TDigest *ts_tdigest_create(void);
extern TDigest *ts_tdigest_create_with_compression(int compression);
extern void ts_tdigest_destroy(TDigest *t);
extern TDigest *ts_tdigest_copy(const TDigest *t);

/*
 * Functions to modify a t-digest
 */
extern TDigest *ts_tdigest_compress(TDigest *t, List *unmerged_centroids);
extern TDigest *ts_tdigest_merge(TDigest *t1, TDigest *t2);

/*
 * Functions to compute values from a t-digest
 */
extern float8 ts_tdigest_cdf(const TDigest *t, float8 x);
extern float8 ts_tdigest_percentile(const TDigest *t, float8 q);
extern int32 ts_tdigest_count(const TDigest *t);

/*
 * Returns total total memory usage of a t-digest and its centroids, in bytes
 */
extern Size ts_tdigest_size(const TDigest *t);

/*
 * Totally orders TDigest by total number of entered data points
 */
extern int ts_tdigest_cmp(const TDigest *t1, const TDigest *t2);
extern bool ts_tdigest_equal(const TDigest *t1, const TDigest *t2);
extern bool ts_tdigest_lt(const TDigest *t1, const TDigest *t2);
extern bool ts_tdigest_gt(const TDigest *t1, const TDigest *t2);
extern bool ts_tdigest_le(const TDigest *t1, const TDigest *t2);
extern bool ts_tdigest_ge(const TDigest *t1, const TDigest *t2);

#endif
