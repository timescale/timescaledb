/*-------------------------------------------------------------------------
 *
 * tdigest.h
 *	  Interface for t-digest support
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef TDIGEST_H
#define TDIGEST_H

#include "c.h"
#include "nodes/pg_list.h"

typedef struct Centroid
{
	uint64 weight;
	float8 mean;
} Centroid;

typedef struct TDigest {
	uint32	vl_len_;

	float8 compression;
	uint32 threshold;
	uint32 size;

	uint64 total_weight;
	float8 min;
	float8 max;

	List *unmerged_centroids;
	uint32 num_centroids;
	Centroid centroids[1];
} TDigest;

extern TDigest *TDigestCreate(void);
extern TDigest *TDigestCreateWithCompression(int compression);
extern void TDigestDestroy(TDigest *t);
extern TDigest *TDigestCopy(TDigest *t);

extern TDigest *TDigestAdd(TDigest *t, float8 x, int64 w);
extern TDigest *TDigestCompress(TDigest *t);
extern TDigest *TDigestMerge(TDigest *t1, TDigest *t2);

extern float8 TDigestCDF(TDigest *t, float8 x);
extern float8 TDigestQuantile(TDigest *t, float8 q);

extern Size TDigestSize(TDigest *t);

#endif
