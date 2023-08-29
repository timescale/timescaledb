/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_PLANNER_H
#define TIMESCALEDB_PLANNER_H

#include <postgres.h>
#include <nodes/pg_list.h>
#include <nodes/parsenodes.h>
#include <nodes/pathnodes.h>

#include "export.h"
#include "hypertable.h"
#include "guc.h"

#define CHUNK_EXCL_FUNC_NAME "chunks_in"
/*
 * Constraints created during planning to improve chunk exclusion
 * will be marked with this value as location so they can be easily
 * identified and removed when they are no longer needed.
 * Removal happens in timescaledb_set_rel_pathlist hook.
 */
#define PLANNER_LOCATION_MAGIC -29811

typedef struct Chunk Chunk;
typedef struct Hypertable Hypertable;
typedef struct TsFdwRelInfo TsFdwRelInfo;
typedef struct TimescaleDBPrivate
{
	bool appends_ordered;
	/* attno of the time dimension in the parent table if appends are ordered */
	int order_attno;
	List *nested_oids;
	List *chunk_oids;
	List *serverids;
	Relids server_relids;
	TsFdwRelInfo *fdw_relation_info;

	/* Cached chunk data for the chunk relinfo. */
	Chunk *cached_chunk_struct;

	/* Cached equivalence members for compressed chunks. List of (EC, EM) Lists. */
	List *compressed_ec_em_pairs;
} TimescaleDBPrivate;

extern TSDLLEXPORT bool ts_rte_is_hypertable(const RangeTblEntry *rte, bool *isdistributed);
extern TSDLLEXPORT bool ts_rte_is_marked_for_expansion(const RangeTblEntry *rte);
extern TSDLLEXPORT bool ts_contain_param(Node *node);

extern TSDLLEXPORT DataFetcherType ts_data_node_fetcher_scan_type;

static inline TimescaleDBPrivate *
ts_create_private_reloptinfo(RelOptInfo *rel)
{
	Assert(rel->fdw_private == NULL);
	rel->fdw_private = palloc0(sizeof(TimescaleDBPrivate));
	return rel->fdw_private;
}

static inline TimescaleDBPrivate *
ts_get_private_reloptinfo(RelOptInfo *rel)
{
	/* If rel->fdw_private is not set up here it means the rel got missclassified
	 * and did not get expanded by our code but by postgres native code.
	 * This is not a problem by itself, but probably an oversight on our part.
	 */
	Assert(rel->fdw_private);
	return rel->fdw_private ? rel->fdw_private : ts_create_private_reloptinfo(rel);
}

/*
 * TsRelType provides consistent classification of planned relations across
 * planner hooks.
 */
typedef enum TsRelType
{
	TS_REL_HYPERTABLE,		 /* A hypertable with no parent */
	TS_REL_CHUNK_STANDALONE, /* Chunk with no parent (i.e., it's part of the
							  * plan as a standalone table. For example,
							  * querying the chunk directly and not via the
							  * parent hypertable). */
	TS_REL_HYPERTABLE_CHILD, /* Self child. With PostgreSQL's table expansion,
							  * the root table is expanded as a child of
							  * itself. This happens when our expansion code
							  * is turned off. */
	TS_REL_CHUNK_CHILD,		 /* Chunk with parent and the result of table
							  * expansion. */
	TS_REL_OTHER,			 /* Anything which is none of the above */
} TsRelType;

typedef enum PartializeAggFixAggref
{
	TS_DO_NOT_FIX_AGGSPLIT = 0,
	TS_FIX_AGGSPLIT_SIMPLE = 1,
	TS_FIX_AGGSPLIT_FINAL = 2
} PartializeAggFixAggref;

extern TSDLLEXPORT Hypertable *ts_planner_get_hypertable(const Oid relid, const unsigned int flags);
bool has_partialize_function(Node *node, PartializeAggFixAggref fix_aggref);
bool ts_plan_process_partialize_agg(PlannerInfo *root, RelOptInfo *output_rel);

extern void ts_plan_add_hashagg(PlannerInfo *root, RelOptInfo *input_rel, RelOptInfo *output_rel);
extern void ts_preprocess_first_last_aggregates(PlannerInfo *root, List *tlist);
extern void ts_plan_expand_hypertable_chunks(Hypertable *ht, PlannerInfo *root, RelOptInfo *rel);
extern void ts_plan_expand_timebucket_annotate(PlannerInfo *root, RelOptInfo *rel);
extern Node *ts_constify_now(PlannerInfo *root, List *rtable, Node *node);
extern void ts_planner_constraint_cleanup(PlannerInfo *root, RelOptInfo *rel);
extern Node *ts_add_space_constraints(PlannerInfo *root, List *rtable, Node *node);

extern TSDLLEXPORT void ts_add_baserel_cache_entry_for_chunk(Oid chunk_reloid,
															 Hypertable *hypertable);

#endif /* TIMESCALEDB_PLANNER_H */
