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

#define DEFAULT_MATPARTCOLUMN_NAME "time_partition_col"
#define CAGG_INVALIDATION_THRESHOLD_NAME "invalidation threshold watermark"
#define CAGG_INVALIDATION_WRONG_GREATEST_VALUE ((int64) -210866803200000001)

typedef struct FinalizeQueryInfo
{
	List *final_seltlist;	/* select target list for finalize query */
	Node *final_havingqual; /* having qual for finalize query */
	Query *final_userquery; /* user query used to compute the finalize_query */
} FinalizeQueryInfo;

typedef struct MaterializationHypertableColumnInfo
{
	List *matcollist;		 /* column defns for materialization tbl*/
	List *partial_seltlist;	 /* tlist entries for populating the materialization table columns */
	List *partial_grouplist; /* group clauses used for populating the materialization table */
	List *mat_groupcolname_list; /* names of columns that are populated by the group-by clause
									correspond to the partial_grouplist.
									time_bucket column is not included here: it is the
									matpartcolname */
	int matpartcolno;			 /*index of partitioning column in matcollist */
	char *matpartcolname;		 /*name of the partition column */
} MaterializationHypertableColumnInfo;

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

typedef enum ContinuousAggRefreshCallContext
{
	CAGG_REFRESH_CREATION,
	CAGG_REFRESH_WINDOW,
	CAGG_REFRESH_POLICY,
	CAGG_REFRESH_POLICY_BATCHED
} ContinuousAggRefreshCallContext;

typedef struct ContinuousAggRefreshContext
{
	ContinuousAggRefreshCallContext callctx;
	int32 processing_batch;
	int32 number_of_batches;
} ContinuousAggRefreshContext;

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

extern ContinuousAggTimeBucketInfo cagg_validate_query(const Query *query, const char *cagg_schema,
													   const char *cagg_name,
													   const bool is_cagg_create);
extern Query *destroy_union_query(Query *q);
extern void RemoveRangeTableEntries(Query *query);
extern Query *build_union_query(ContinuousAggTimeBucketInfo *tbinfo, int matpartcolno, Query *q1,
								Query *q2, int materialize_htid);
extern void mattablecolumninfo_init(MaterializationHypertableColumnInfo *matcolinfo,
									List *grouplist);
extern bool function_allowed_in_cagg_definition(Oid funcid);
extern Oid get_watermark_function_oid(void);
extern Oid cagg_get_boundary_converter_funcoid(Oid typoid);

extern ContinuousAgg *cagg_get_by_relid_or_fail(const Oid cagg_relid);
extern List *cagg_find_groupingcols(ContinuousAgg *agg, Hypertable *mat_ht);

static inline int64
cagg_get_time_min(const ContinuousAgg *cagg)
{
	if (cagg->bucket_function->bucket_fixed_interval == false)
	{
		/*
		 * To determine inscribed/circumscribed refresh window for variable-sized
		 * buckets we should be able to calculate time_bucket(window.begin) and
		 * time_bucket(window.end). This, however, is not possible in general case.
		 * As an example, the minimum date is 4714-11-24 BC, which is before any
		 * reasonable default `origin` value. Thus for variable-sized buckets
		 * instead of minimum date we use -infinity since time_bucket(-infinity)
		 * is well-defined as -infinity.
		 *
		 * For more details see:
		 * - ts_compute_inscribed_bucketed_refresh_window_variable()
		 * - ts_compute_circumscribed_bucketed_refresh_window_variable()
		 */
		return ts_time_get_nobegin_or_min(cagg->partition_type);
	}

	/* For fixed-sized buckets return min (start of time) */
	return ts_time_get_min(cagg->partition_type);
}

ContinuousAggBucketFunction *ts_cagg_get_bucket_function_info(Oid view_oid);
