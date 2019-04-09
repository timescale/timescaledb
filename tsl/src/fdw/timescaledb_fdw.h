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

extern Datum timescaledb_fdw_handler(PG_FUNCTION_ARGS);
extern Datum timescaledb_fdw_validator(PG_FUNCTION_ARGS);
TsFdwRelationInfo *fdw_relation_info_get(RelOptInfo *baserel);

#endif /* TIMESCALEDB_TSL_FDW_TIMESCALEDB_FDW_H */
