/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_FDW_TIMESCALEDB_FDW_H
#define TIMESCALEDB_TSL_FDW_TIMESCALEDB_FDW_H

#include <postgres.h>
#include <fmgr.h>
#include <foreign/foreign.h>
#include <lib/stringinfo.h>
#include <nodes/relation.h>
#include <utils/relcache.h>
#include <commands/explain.h>
#include <funcapi.h>

#include "server_chunk_assignment.h"

#define TIMESCALEDB_FDW_NAME "timescaledb_fdw"

/*
 * FDW-specific planner information kept in RelOptInfo.fdw_private. For a
 * baserel, this struct is created by GetForeignRelSize, although some fields
 * are not filled till later. GetForeignJoinPaths creates it for a joinrel,
 * and GetForeignUpperPaths creates it for an upperrel.
 */

typedef enum
{
	TS_FDW_RELATION_INFO_HYPERTABLE_SERVER = 0,
	TS_FDW_RELATION_INFO_HYPERTABLE,
	TS_FDW_RELATION_INFO_FOREIGN_TABLE,
} TsFdwRelationInfoType;

typedef struct TsFdwRelationInfo
{
	TsFdwRelationInfoType type;
	/*
	 * True means that the relation can be pushed down. Always true for simple
	 * foreign scan.
	 */
	bool pushdown_safe;

	/*
	 * Restriction clauses, divided into safe and unsafe to pushdown subsets.
	 * All entries in these lists should have RestrictInfo wrappers; that
	 * improves efficiency of selectivity and cost estimation.
	 */
	List *remote_conds;
	List *local_conds;

	/* Actual remote restriction clauses for scan (sans RestrictInfos) */
	List *final_remote_exprs;

	/* Bitmap of attr numbers we need to fetch from the remote server. */
	Bitmapset *attrs_used;

	/* Cost and selectivity of local_conds. */
	QualCost local_conds_cost;
	Selectivity local_conds_sel;

	/* Selectivity of join conditions */
	Selectivity joinclause_sel;

	/* Estimated size and cost for a scan or join. */
	double rows;
	int width;
	Cost startup_cost;
	Cost total_cost;
	/* Costs excluding costs for transferring data from the foreign server */
	Cost rel_startup_cost;
	Cost rel_total_cost;

	/* Options extracted from catalogs. */
	bool use_remote_estimate;
	Cost fdw_startup_cost;
	Cost fdw_tuple_cost;
	List *shippable_extensions; /* OIDs of whitelisted extensions */

	/* Cached catalog information. */
	ForeignTable *table;
	ForeignServer *server;
	UserMapping *user; /* only set in use_remote_estimate mode */

	int fetch_size; /* fetch size for this remote table */

	/*
	 * Name of the relation while EXPLAINing ForeignScan. It is used for join
	 * relations but is set for all relations. For join relation, the name
	 * indicates which foreign tables are being joined and the join type used.
	 */
	StringInfo relation_name;

	/* Join information */
	RelOptInfo *outerrel;
	RelOptInfo *innerrel;
	JoinType jointype;
	/* joinclauses contains only JOIN/ON conditions for an outer join */
	List *joinclauses; /* List of RestrictInfo */

	/* Grouping information */
	List *grouped_tlist;

	/* Subquery information */
	bool make_outerrel_subquery; /* do we deparse outerrel as a
								  * subquery? */
	bool make_innerrel_subquery; /* do we deparse innerrel as a
								  * subquery? */
	Relids lower_subquery_rels;  /* all relids appearing in lower
								  * subqueries */

	/*
	 * Index of the relation.  It is used to create an alias to a subquery
	 * representing the relation.
	 */
	int relation_index;
	ServerChunkAssignment *sca;
} TsFdwRelationInfo;

typedef struct ScanInfo
{
	Oid serverid;
	Index scan_relid;
	List *local_exprs;
	List *fdw_private;
	List *fdw_scan_tlist;
	List *fdw_recheck_quals;
	List *params_list;
	bool systemcol;
} ScanInfo;

/*
 * Execution state of a foreign scan using timescaledb_fdw.
 */
typedef struct TsFdwScanState
{
	Relation rel;								 /* relcache entry for the foreign table. NULL
												  * for a foreign join scan. */
	TupleDesc tupdesc;							 /* tuple descriptor of scan */
	struct AttConvInMetadata *att_conv_metadata; /* attribute datatype conversion metadata */

	/* extracted fdw_private data */
	char *query;		   /* text of SELECT command */
	List *retrieved_attrs; /* list of retrieved attribute numbers */

	/* for remote query execution */
	struct TSConnection *conn;  /* connection for the scan */
	unsigned int cursor_number; /* quasi-unique ID for my cursor */
	bool cursor_exists;			/* have we created the cursor? */
	int num_params;				/* number of parameters passed to query */
	FmgrInfo *param_flinfo;		/* output conversion functions for them */
	List *param_exprs;			/* executable expressions for param values */
	const char **param_values;  /* textual values of query parameters */

	/* for storing result tuples */
	HeapTuple *tuples; /* array of currently-retrieved tuples */
	int num_tuples;	/* # of tuples in array */
	int next_tuple;	/* index of next one to return */

	/* batch-level state, for optimizing rewinds and avoiding useless fetch */
	int fetch_ct_2;   /* Min(# of fetches done, 2) */
	bool eof_reached; /* true if last fetch reached EOF */

	/* working memory contexts */
	MemoryContext batch_cxt; /* context holding current batch of tuples */
	MemoryContext temp_cxt;  /* context for per-tuple temporary data */

	int fetch_size; /* number of tuples per fetch */
	int row_counter;
} TsFdwScanState;

typedef Path *(*CreatePathFunc)(PlannerInfo *root, RelOptInfo *rel, PathTarget *target, double rows,
								Cost startup_cost, Cost total_cost, List *pathkeys,
								Relids required_outer, Path *fdw_outerpath, List *fdw_private);

extern Datum timescaledb_fdw_handler(PG_FUNCTION_ARGS);
extern Datum timescaledb_fdw_validator(PG_FUNCTION_ARGS);
extern TsFdwRelationInfo *fdw_relation_info_get(RelOptInfo *baserel);
extern TsFdwRelationInfo *fdw_relation_info_create(PlannerInfo *root, RelOptInfo *baserel,
												   Oid server_oid, Oid local_table_id,
												   TsFdwRelationInfoType type);
extern void fdw_scan_info_init(ScanInfo *scaninfo, PlannerInfo *root, RelOptInfo *rel,
							   Path *best_path, List *scan_clauses);

extern void fdw_add_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel, Path *epq_path,
												CreatePathFunc);

extern void fdw_create_upper_paths(TsFdwRelationInfo *input_fpinfo, PlannerInfo *root,
								   UpperRelationKind stage, RelOptInfo *input_rel,
								   RelOptInfo *output_rel, void *extra,
								   CreatePathFunc create_paths);

extern void fdw_scan_init(ScanState *ss, TsFdwScanState *fsstate, Bitmapset *scanrelids,
						  List *fdw_private, List *fdw_exprs, int eflags);
extern TupleTableSlot *fdw_scan_iterate(ScanState *ss, TsFdwScanState *fsstate);
extern void fdw_scan_rescan(ScanState *ss, TsFdwScanState *fsstate);
extern void fdw_scan_end(TsFdwScanState *fsstate);
extern void fdw_scan_explain(ScanState *ss, List *fdw_private, ExplainState *es);

#endif /* TIMESCALEDB_TSL_FDW_TIMESCALEDB_FDW_H */
