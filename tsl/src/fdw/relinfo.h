/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_FDW_RELINFO_H
#define TIMESCALEDB_TSL_FDW_RELINFO_H

#include <postgres.h>
#include <foreign/foreign.h>
#include <lib/stringinfo.h>
#include <nodes/nodes.h>
#include <nodes/pathnodes.h>
#include <optimizer/optimizer.h>

#include "remote/connection.h"
#include "data_node_chunk_assignment.h"

/*
 * FDW-specific planner information kept in RelOptInfo.fdw_private. For a
 * baserel, this struct is created by GetForeignRelSize, although some fields
 * are not filled till later. GetForeignJoinPaths creates it for a joinrel,
 * and GetForeignUpperPaths creates it for an upperrel.
 */

typedef enum
{
	TS_FDW_RELINFO_UNINITIALIZED = 0,
	TS_FDW_RELINFO_HYPERTABLE_DATA_NODE,
	TS_FDW_RELINFO_HYPERTABLE,
	TS_FDW_RELINFO_FOREIGN_TABLE,
} TsFdwRelInfoType;

#ifdef TS_DEBUG
/* A path considered during planning but which may have been pruned. Used for
 * debugging purposes. */
typedef struct ConsideredPath
{
	Path *path;
	uintptr_t origin; /* The pointer value of the original path */
} ConsideredPath;
#endif /* TS_DEBUG */

typedef struct TsFdwRelInfo
{
	TsFdwRelInfoType type;
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

	/* Bitmap of attr numbers we need to fetch from the remote data node. */
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

	/* Costs excluding costs for transferring data from the data node */
	Cost rel_startup_cost;
	Cost rel_total_cost;
	double rel_retrieved_rows;

	/* Costs for transferring data across the network */
	Cost fdw_startup_cost;
	Cost fdw_tuple_cost;
	List *shippable_extensions; /* OIDs of whitelisted extensions */

	/* Cached catalog information. */
	ForeignTable *table;
	ForeignServer *server;

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
	DataNodeChunkAssignment *sca;
#ifdef TS_DEBUG
	List *considered_paths; /* List of ConsideredPath objects of all the paths
							   that planner has considered. This is intended
							   to be only used for printing cost debug
							   output */
#endif

	/*
	 * Moving averages of chunk size, valid for the hypertable relinfo.
	 * We use them to compute the size for remote chunks that don't have local
	 * statistics, e.g. because ANALYZE haven't been run. Note that these values
	 * are adjusted for fill factor, i.e. they correspond to a fill factor of
	 * 1.0. The fill factor for a particular chunk is estimated separately.
	 */
	double average_chunk_pages;
	double average_chunk_tuples;

	/* Cached chunk data for the chunk relinfo. */
	struct Chunk *chunk;
} TsFdwRelInfo;

extern TsFdwRelInfo *fdw_relinfo_create(PlannerInfo *root, RelOptInfo *rel, Oid server_oid,
										Oid local_table_id, TsFdwRelInfoType type);
extern TsFdwRelInfo *fdw_relinfo_alloc_or_get(RelOptInfo *rel);
extern TsFdwRelInfo *fdw_relinfo_get(RelOptInfo *rel);

#endif /* TIMESCALEDB_TSL_FDW_RELINFO_H */
