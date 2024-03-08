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

#define CONTINUOUS_AGG_MAX_JOIN_RELATIONS 2
#define DEFAULT_MATPARTCOLUMN_NAME "time_partition_col"
#define CAGG_INVALIDATION_THRESHOLD_NAME "invalidation threshold watermark"

typedef struct FinalizeQueryInfo
{
	List *final_seltlist;	/* select target list for finalize query */
	Node *final_havingqual; /* having qual for finalize query */
	Query *final_userquery; /* user query used to compute the finalize_query */
	bool finalized;			/* finalized form? */
} FinalizeQueryInfo;

typedef struct MatTableColumnInfo
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
} MatTableColumnInfo;

typedef struct CAggTimebucketInfo
{
	int32 htid;						/* hypertable id */
	int32 parent_mat_hypertable_id; /* parent materialization hypertable id */
	Oid htoid;						/* hypertable oid */
	AttrNumber htpartcolno;			/* primary partitioning column of raw hypertable */
									/* This should also be the column used by time_bucket */
	Oid htpartcoltype;
	int64 htpartcol_interval_len; /* interval length setting for primary partitioning column */
	int64 bucket_width;			  /* bucket_width of time_bucket, stores BUCKET_WIDTH_VARIABLE for
									 variable-sized buckets */
	Oid bucket_width_type;		  /* type of bucket_width */
	Interval *interval;			  /* stores the interval, NULL if not specified */
	const char *timezone;		  /* the name of the timezone, NULL if not specified */

	FuncExpr *bucket_func; /* function call expr of the bucketing function */
	/*
	 * Custom origin value stored as UTC timestamp.
	 * If not specified, stores infinity.
	 */
	TimestampTz origin;
} CAggTimebucketInfo;

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

extern CAggTimebucketInfo cagg_validate_query(const Query *query, const bool finalized,
											  const char *cagg_schema, const char *cagg_name,
											  const bool is_cagg_create);
extern Query *destroy_union_query(Query *q);
extern Oid relation_oid(Name schema, Name name);
extern void RemoveRangeTableEntries(Query *query);
extern Query *build_union_query(CAggTimebucketInfo *tbinfo, int matpartcolno, Query *q1, Query *q2,
								int materialize_htid);
extern void mattablecolumninfo_init(MatTableColumnInfo *matcolinfo, List *grouplist);
extern bool function_allowed_in_cagg_definition(Oid funcid);
extern Oid get_watermark_function_oid(void);
extern Oid cagg_get_boundary_converter_funcoid(Oid typoid);

static inline int64
cagg_get_time_min(const ContinuousAgg *cagg)
{
	if (ts_continuous_agg_bucket_width_variable(cagg))
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

	/* For fixed-sized buckets return min (start of time) */
	return ts_time_get_min(cagg->partition_type);
}
