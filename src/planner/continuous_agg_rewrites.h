/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include <access/reloptions.h>
#include <access/xact.h>
#include <catalog/pg_aggregate.h>
#include <catalog/pg_type.h>
#include <catalog/toasting.h>
#include <commands/tablecmds.h>
#include <commands/tablespace.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/parsenodes.h>
#include <nodes/pg_list.h>
#include <optimizer/optimizer.h>
#include <parser/parse_func.h>
#include <parser/parse_oper.h>
#include <parser/parsetree.h>
#include <rewrite/rewriteDefine.h>
#include <rewrite/rewriteHandler.h>
#include <rewrite/rewriteManip.h>
#include <utils/builtins.h>
#include <utils/syscache.h>
#include <utils/typcache.h>

#include "continuous_aggs_common.h"
#include "errors.h"
#include "func_cache.h"
#include "hypertable_cache.h"
#include "timezones.h"
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

extern void check_query_for_cagg_rewrites(CaggRewriteContext *cagg_rewrite_ctx, Query *query);
extern void check_query_rtes_for_cagg_rewrites(CaggRewriteContext *cagg_rewrite_ctx,
											   RangeTblEntry *rte);
extern const Dimension *
check_hypertable_dim_for_cagg_rewrites(CaggRewriteContext *cagg_rewrite_ctx);
extern void check_timebucket_for_cagg_rewrites(CaggRewriteContext *cagg_rewrite_ctx, Query *query);

extern void match_query_to_cagg(CaggRewriteContext *cagg_rewrite_ctx, Query *query,
								bool do_rewrite);
#endif
