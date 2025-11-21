/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
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
#include <rewrite/rewriteHandler.h>
#include <rewrite/rewriteManip.h>
#include <utils/builtins.h>
#include <utils/syscache.h>
#include <utils/typcache.h>

#include "errors.h"
#include "func_cache.h"
#include "hypertable_cache.h"
#include "timezones.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/continuous_agg.h"

#define IS_TIME_BUCKET_INFO_TIME_BASED(bucket_function)                                            \
	(bucket_function->bucket_width_type == INTERVALOID)

#define CAGG_MAKEQUERY(selquery, srcquery)                                                         \
	do                                                                                             \
	{                                                                                              \
		(selquery) = makeNode(Query);                                                              \
		(selquery)->commandType = CMD_SELECT;                                                      \
		(selquery)->querySource = (srcquery)->querySource;                                         \
		(selquery)->queryId = (srcquery)->queryId;                                                 \
		(selquery)->canSetTag = (srcquery)->canSetTag;                                             \
		(selquery)->utilityStmt = copyObject((srcquery)->utilityStmt);                             \
		(selquery)->resultRelation = 0;                                                            \
		(selquery)->hasAggs = true;                                                                \
		(selquery)->hasRowSecurity = false;                                                        \
		(selquery)->rtable = NULL;                                                                 \
	} while (0);

typedef struct ContinuousAggTimeBucketInfo
{
	int32 htid;						/* hypertable id */
	int32 parent_mat_hypertable_id; /* parent materialization hypertable id */
	Oid htoid;						/* hypertable oid */
	Oid htoidparent;				/* parent hypertable oid in case of hierarchical */
	AttrNumber htpartcolno;			/* primary partitioning column of raw hypertable */
									/* This should also be the column used by time_bucket */
	Oid htpartcoltype;				/* The collation type */
	int64 htpartcol_interval_len;	/* interval length setting for primary partitioning column */

	/* General bucket information */
	ContinuousAggBucketFunction *bf;
} ContinuousAggTimeBucketInfo;

/* Methods checking validity of Caggs and Cagg rewrites */
extern TSDLLEXPORT bool ts_cagg_query_supported(const Query *query, StringInfo hint,
												StringInfo detail, const bool finalized,
												const bool for_rewrites);
extern TSDLLEXPORT bool ts_cagg_query_rtes_supported(RangeTblEntry *rte, RangeTblEntry **ht_rte,
													 StringInfo detail, const bool for_rewrites);
extern TSDLLEXPORT const Dimension *
ts_cagg_hypertable_dim_supported(RangeTblEntry *ht_rte, Hypertable *ht, StringInfo msg,
								 StringInfo detail, StringInfo hint, const bool for_rewrites);
extern TSDLLEXPORT bool ts_function_allowed_in_cagg_definition(Oid funcid);
extern TSDLLEXPORT void ts_caggtimebucketinfo_init(ContinuousAggTimeBucketInfo *src,
												   int32 hypertable_id, Oid hypertable_oid,
												   AttrNumber hypertable_partition_colno,
												   Oid hypertable_partition_coltype,
												   int64 hypertable_partition_col_interval,
												   int32 parent_mat_hypertable_id);
extern TSDLLEXPORT bool ts_caggtimebucket_validate(ContinuousAggBucketFunction *bf,
												   List *groupClause, List *targetList,
												   List *rtable, int ht_partcolno, StringInfo msg,
												   bool is_cagg_create, bool for_rewrite);
extern TSDLLEXPORT ContinuousAggBucketFunction *ts_cagg_get_bucket_function_info(Oid view_oid);
extern TSDLLEXPORT bool ts_time_bucket_info_has_fixed_width(const ContinuousAggBucketFunction *bf);

extern bool caggtimebucket_equal(ContinuousAggBucketFunction *bf1,
								 ContinuousAggBucketFunction *bf2);
