/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <nodes/parsenodes.h>
#include <nodes/pathnodes.h>
#include <nodes/pg_list.h>
#include <parser/parsetree.h>

#include "chunk.h"
#include "export.h"
#include "guc.h"
#include "hypertable.h"

/*
 * Constraints created during planning to improve chunk exclusion
 * will be marked with this value as location so they can be easily
 * identified and removed when they are no longer needed.
 * Removal happens in timescaledb_set_rel_pathlist hook.
 */
#define PLANNER_LOCATION_MAGIC -29811

typedef struct Chunk Chunk;
typedef struct Hypertable Hypertable;
typedef struct TimescaleDBPrivate
{
	bool appends_ordered;
	/* attno of the time dimension in the parent table if appends are ordered */
	int order_attno;
	List *nested_oids;
	List *chunk_oids;

	/* Cached chunk data for the chunk relinfo. */
	Chunk *cached_chunk_struct;

	/* Cached equivalence members for compressed chunks. List of (EC, EM) Lists. */
	List *compressed_ec_em_pairs;
} TimescaleDBPrivate;

extern TSDLLEXPORT bool ts_rte_is_hypertable(const RangeTblEntry *rte);
extern TSDLLEXPORT bool ts_rte_is_marked_for_expansion(const RangeTblEntry *rte);
extern TSDLLEXPORT bool ts_contains_external_param(Node *node);
extern TSDLLEXPORT bool ts_contains_join_param(Node *node);

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
extern void ts_plan_expand_hypertable_chunks(Hypertable *ht, PlannerInfo *root, RelOptInfo *rel,
											 bool include_osm);
extern void ts_plan_expand_timebucket_annotate(PlannerInfo *root, RelOptInfo *rel);
extern Expr *ts_transform_time_bucket_comparison(Expr *);
extern Node *ts_constify_now(PlannerInfo *root, List *rtable, Node *node);
extern void ts_planner_constraint_cleanup(PlannerInfo *root, RelOptInfo *rel);
extern Node *ts_add_space_constraints(PlannerInfo *root, List *rtable, Node *node);

extern TSDLLEXPORT void ts_add_baserel_cache_entry_for_chunk(Oid chunk_reloid,
															 Hypertable *hypertable);
TsRelType TSDLLEXPORT ts_classify_relation(const PlannerInfo *root, const RelOptInfo *rel,
										   Hypertable **ht);

/*
 * Chunk-equivalent of planner_rt_fetch(), but returns the corresponding chunk
 * instead of range table entry.
 *
 * Returns NULL if this rel is not a chunk.
 *
 * This cache should be pre-warmed by hypertable expansion, but it
 * doesn't run in the following cases:
 *
 * 1. if it was a direct query on the chunk;
 *
 * 2. if it is not a SELECT QUERY.
 */
static inline const Chunk *
ts_planner_chunk_fetch(const PlannerInfo *root, RelOptInfo *rel)
{
	TimescaleDBPrivate *rel_private;

	/* The rel can only be a chunk if it is part of a hypertable expansion
	 * (RELOPT_OTHER_MEMBER_REL) or a directly query on the chunk
	 * (RELOPT_BASEREL) */
	if (rel->reloptkind != RELOPT_OTHER_MEMBER_REL && rel->reloptkind != RELOPT_BASEREL)
		return NULL;

	/* The rel_private entry should have been created as part of classifying
	 * the relation in timescaledb_get_relation_info_hook(). Therefore,
	 * ts_get_private_reloptinfo() asserts that it is already set but falls
	 * back to creating rel_private in release builds for safety. */
	rel_private = ts_get_private_reloptinfo(rel);

	if (NULL == rel_private->cached_chunk_struct)
	{
		RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);
		rel_private->cached_chunk_struct =
			ts_chunk_get_by_relid(rte->relid, /* fail_if_not_found = */ true);
	}

	return rel_private->cached_chunk_struct;
}
