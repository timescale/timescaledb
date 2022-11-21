/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <nodes/extensible.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>

#include "compat/compat.h"
#include "chunk.h"
#include "hypertable.h"
#include "frozen_chunk_dml.h"
#include "utils.h"

/*
 * Path, Plan and State node for blocking DML on frozen chunks.
 */

static Path *frozen_chunk_dml_path_create(Path *subpath, Oid chunk_relid);
static Plan *frozen_chunk_dml_plan_create(PlannerInfo *root, RelOptInfo *relopt,
										  CustomPath *best_path, List *tlist, List *clauses,
										  List *custom_plans);
static Node *frozen_chunk_dml_state_create(CustomScan *scan);

static void frozen_chunk_dml_begin(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *frozen_chunk_dml_exec(CustomScanState *node);
static void frozen_chunk_dml_end(CustomScanState *node);
static void frozen_chunk_dml_rescan(CustomScanState *node);

static CustomPathMethods frozen_chunk_dml_path_methods = {
	.CustomName = "FrozenChunkDml",
	.PlanCustomPath = frozen_chunk_dml_plan_create,
};

static CustomScanMethods frozen_chunk_dml_plan_methods = {
	.CustomName = "FrozenChunkDml",
	.CreateCustomScanState = frozen_chunk_dml_state_create,
};

static CustomExecMethods frozen_chunk_dml_state_methods = {
	.CustomName = FROZEN_CHUNK_DML_STATE_NAME,
	.BeginCustomScan = frozen_chunk_dml_begin,
	.EndCustomScan = frozen_chunk_dml_end,
	.ExecCustomScan = frozen_chunk_dml_exec,
	.ReScanCustomScan = frozen_chunk_dml_rescan,
};

static void
frozen_chunk_dml_begin(CustomScanState *node, EState *estate, int eflags)
{
	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
	Plan *subplan = linitial(cscan->custom_plans);
	node->custom_ps = list_make1(ExecInitNode(subplan, estate, eflags));
}

/*
 * nothing to reset for rescan in dml blocker
 */
static void
frozen_chunk_dml_rescan(CustomScanState *node)
{
}

/* we cannot update/delete rows if we have a frozen chunk. so
 * throw an error. Note this subplan will return 0 tuples as the chunk is empty
 * and all rows are saved in the compressed chunk.
 */
static TupleTableSlot *
frozen_chunk_dml_exec(CustomScanState *node)
{
	FrozenChunkDmlState *state = (FrozenChunkDmlState *) node;
	Oid chunk_relid = state->chunk_relid;
	elog(ERROR,
		 "cannot update/delete rows from chunk \"%s\" as it is frozen",
		 get_rel_name(chunk_relid));
	return NULL;
}

static void
frozen_chunk_dml_end(CustomScanState *node)
{
	PlanState *substate = linitial(node->custom_ps);
	ExecEndNode(substate);
}

static Path *
frozen_chunk_dml_path_create(Path *subpath, Oid chunk_relid)
{
	FrozenChunkDmlPath *path = (FrozenChunkDmlPath *) palloc0(sizeof(FrozenChunkDmlPath));

	memcpy(&path->cpath.path, subpath, sizeof(Path));
	path->cpath.path.type = T_CustomPath;
	path->cpath.path.pathtype = T_CustomScan;
	path->cpath.path.parent = subpath->parent;
	path->cpath.path.pathtarget = subpath->pathtarget;
	path->cpath.methods = &frozen_chunk_dml_path_methods;
	path->cpath.custom_paths = list_make1(subpath);
	path->chunk_relid = chunk_relid;

	return &path->cpath.path;
}

static Plan *
frozen_chunk_dml_plan_create(PlannerInfo *root, RelOptInfo *relopt, CustomPath *best_path,
							 List *tlist, List *clauses, List *custom_plans)
{
	FrozenChunkDmlPath *cdpath = (FrozenChunkDmlPath *) best_path;
	CustomScan *cscan = makeNode(CustomScan);

	Assert(list_length(custom_plans) == 1);

	cscan->methods = &frozen_chunk_dml_plan_methods;
	cscan->custom_plans = custom_plans;
	cscan->scan.scanrelid = relopt->relid;
	cscan->scan.plan.targetlist = tlist;
	cscan->custom_scan_tlist = NIL;
	cscan->custom_private = list_make1_oid(cdpath->chunk_relid);
	return &cscan->scan.plan;
}

static Node *
frozen_chunk_dml_state_create(CustomScan *scan)
{
	FrozenChunkDmlState *state;

	state = (FrozenChunkDmlState *) newNode(sizeof(FrozenChunkDmlState), T_CustomScanState);
	state->chunk_relid = linitial_oid(scan->custom_private);
	state->cscan_state.methods = &frozen_chunk_dml_state_methods;
	return (Node *) state;
}

Path *
frozen_chunk_dml_generate_path(Path *subpath, Chunk *chunk)
{
	return frozen_chunk_dml_path_create(subpath, chunk->table_id);
}
