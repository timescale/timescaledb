/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CONTINUOUS_AGGS_COMMON_H
#define TIMESCALEDB_TSL_CONTINUOUS_AGGS_COMMON_H

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

#define TS_PARTIALFN "partialize_agg"
#define CONTINUOUS_AGG_MAX_JOIN_RELATIONS 2
#define DEFAULT_MATPARTCOLUMN_NAME "time_partition_col"

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
	Timestamp origin;
} CAggTimebucketInfo;

typedef struct AggPartCxt
{
	struct MatTableColumnInfo *mattblinfo;
	bool added_aggref_col;
	/*
	 * Set to true when you come across a Var
	 * that is not inside an Aggref node.
	 */
	bool var_outside_of_aggref;
	Oid ignore_aggoid;
	int original_query_resno;
	/*
	 * "Original variables" are the Var nodes of the target list of the original
	 * CREATE MATERIALIZED VIEW query. "Mapped variables" are the Var nodes of the materialization
	 * table columns. The partialization query is the one that populates those columns. The
	 * finalization query should use the "mapped variables" to populate the user view.
	 */
	List *orig_vars; /* List of Var nodes that have been mapped to materialization table columns */
	List *mapped_vars; /* List of Var nodes of the corresponding materialization table columns */
					   /* orig_vars and mapped_vars lists are mapped 1 to 1 */
} AggPartCxt;

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
											  const char *cagg_schema, const char *cagg_name);
extern Query *destroy_union_query(Query *q);
extern Oid relation_oid(Name schema, Name name);
extern void RemoveRangeTableEntries(Query *query);
extern Query *build_union_query(CAggTimebucketInfo *tbinfo, int matpartcolno, Query *q1, Query *q2,
								int materialize_htid);
extern void mattablecolumninfo_init(MatTableColumnInfo *matcolinfo, List *grouplist);
extern void mattablecolumninfo_addinternal(MatTableColumnInfo *matcolinfo);
extern bool function_allowed_in_cagg_definition(Oid funcid);
#endif
