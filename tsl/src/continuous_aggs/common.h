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

#define PARTIALFN "partialize_agg"

typedef struct FinalizeQueryInfo
{
	List *final_seltlist;   /* select target list for finalize query */
	Node *final_havingqual; /* having qual for finalize query */
	Query *final_userquery; /* user query used to compute the finalize_query */
	bool finalized;			/* finalized form? */
} FinalizeQueryInfo;

typedef struct MatTableColumnInfo
{
	List *matcollist;		 /* column defns for materialization tbl*/
	List *partial_seltlist;  /* tlist entries for populating the materialization table columns */
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
	int32 htid;				/* hypertable id */
	Oid htoid;				/* hypertable oid */
	AttrNumber htpartcolno; /* primary partitioning column of raw hypertable */
							/* This should also be the column used by time_bucket */
	Oid htpartcoltype;
	int64 htpartcol_interval_len; /* interval length setting for primary partitioning column */
	int64 bucket_width;			  /* bucket_width of time_bucket, stores BUCKET_WIDHT_VARIABLE for
									 variable-sized buckets */
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
	bool var_outside_of_aggref; /* Set to true when you come across a Var that is not inside an
								   Aggref node */
	Oid ignore_aggoid;
	int original_query_resno;

	/*
	 * "Original variables" are the Var nodes of the target list of the original
	 * CREATE MATERIALIZED VIEW query. "Mapped variables" are the Var nodes of the materialization
	 * table columns. The partialization query is the one that populates those columns. The
	 * finalization query should use the "mapped variables" to populate the user view.
	 */
	List *orig_vars; /* List of Var nodes that have been mapped to materialization table columns */
	List *mapped_vars; /* List of Var nodes of the corresponding materialization table columns
						  orig_vars and mapped_vars lists are mapped 1 to 1 */
} AggPartCxt;

/* Note that we set rowsecurity to false here */
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
	} while (0);

/*switch to ts user for _timescaledb_internal access */
#define SWITCH_TO_TS_USER(schemaname, newuid, saved_uid, saved_secctx)                             \
	do                                                                                             \
	{                                                                                              \
		if ((schemaname) &&                                                                        \
			strncmp(schemaname, INTERNAL_SCHEMA_NAME, strlen(INTERNAL_SCHEMA_NAME)) == 0)          \
			(newuid) = ts_catalog_database_info_get()->owner_uid;                                  \
		else                                                                                       \
			(newuid) = InvalidOid;                                                                 \
		if ((newuid) != InvalidOid)                                                                \
		{                                                                                          \
			GetUserIdAndSecContext(&(saved_uid), &(saved_secctx));                                 \
			SetUserIdAndSecContext(uid, (saved_secctx) | SECURITY_LOCAL_USERID_CHANGE);            \
		}                                                                                          \
	} while (0)

#define RESTORE_USER(newuid, saved_uid, saved_secctx)                                              \
	do                                                                                             \
	{                                                                                              \
		if ((newuid) != InvalidOid)                                                                \
			SetUserIdAndSecContext(saved_uid, saved_secctx);                                       \
	} while (0);

#define CHUNKIDFROMRELID "chunk_id_from_relid"
#define CONTINUOUS_AGG_CHUNK_ID_COL_NAME "chunk_id"

#define MATPARTCOL_INTERVAL_FACTOR 10
#define HT_DEFAULT_CHUNKFN "calculate_chunk_interval"
#define CAGG_INVALIDATION_TRIGGER "continuous_agg_invalidation_trigger"
#define BOUNDARY_FUNCTION "cagg_watermark"
#define INTERNAL_TO_DATE_FUNCTION "to_date"
#define INTERNAL_TO_TSTZ_FUNCTION "to_timestamp"
#define INTERNAL_TO_TS_FUNCTION "to_timestamp_without_timezone"

extern CAggTimebucketInfo cagg_validate_query(const Query *query, const bool finalized);
extern Query *destroy_union_query(Query *q);
extern bool function_allowed_in_cagg_definition(Oid funcid);
extern Var *mattablecolumninfo_addentry(MatTableColumnInfo *out, Node *input,
										int original_query_resno, bool finalized,
										bool *skip_adding);
extern Oid relation_oid(NameData schema, NameData name);
extern void remove_old_and_new_rte_from_query(Query *query);
extern Query *build_union_query(CAggTimebucketInfo *tbinfo, int matpartcolno, Query *q1, Query *q2,
								int materialize_htid);

#endif
