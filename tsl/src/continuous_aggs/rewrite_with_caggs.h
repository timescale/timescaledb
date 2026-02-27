/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include "common.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/continuous_agg.h"

#if PG16_GE
typedef struct CaggRewriteContext
{
	bool eligible;						/* whether query is eligible for Caggs match */
	RangeTblEntry *ht_rte;				/* Parent RTE for candidate Caggs */
	Hypertable *ht;						/* Parent hypertable for candidate Caggs */
	ContinuousAgg *cagg_parent;			/* parent Cagg for candidate Caggs */
	ContinuousAgg *cagg;				/* matching Cagg, if match is found */
	ContinuousAggTimeBucketInfo tbinfo; /* query bucket info to be matched with Caggs buckets*/
	StringInfoData msg;					/* collects debug info */
} CaggRewriteContext;

Query *continuous_agg_apply_rewrites(Query *parse);

#endif
