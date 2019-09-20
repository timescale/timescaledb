/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <nodes/extensible.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>

#include "compat.h"
#include "chunk.h"
#include "hypertable.h"
#include "hypertable_compression.h"
#include "compress_dml.h"
#include "utils.h"

/*Path, Plan and State node for processing dml on compressed chunks
 * For now, this just blocks updates/deletes on compressed chunks
 * since trigger based approach does not work
 */

static Path *compress_chunk_dml_path_create(Path *subpath, Oid chunk_relid);
static Plan *compress_chunk_dml_plan_create(PlannerInfo *root, RelOptInfo *relopt,
											CustomPath *best_path, List *tlist, List *clauses,
											List *custom_plans);
static Node *compress_chunk_dml_state_create(CustomScan *scan);

static void compress_chunk_dml_begin(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *compress_chunk_dml_exec(CustomScanState *node);
static void compress_chunk_dml_end(CustomScanState *node);

static CustomPathMethods compress_chunk_dml_path_methods = {
	.CustomName = "CompressChunkDml",
	.PlanCustomPath = compress_chunk_dml_plan_create,
};

static CustomScanMethods compress_chunk_dml_plan_methods = {
	.CustomName = "CompressChunkDml",
	.CreateCustomScanState = compress_chunk_dml_state_create,
};

static CustomExecMethods compress_chunk_dml_state_methods = {
	.CustomName = COMPRESS_CHUNK_DML_STATE_NAME,
	.BeginCustomScan = compress_chunk_dml_begin,
	.EndCustomScan = compress_chunk_dml_end,
	.ExecCustomScan = compress_chunk_dml_exec,
};

static void
compress_chunk_dml_begin(CustomScanState *node, EState *estate, int eflags)
{
	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
	Plan *subplan = linitial(cscan->custom_plans);
	node->custom_ps = list_make1(ExecInitNode(subplan, estate, eflags));
}

/* we cannot update/delete rows if we have a compressed chunk. so
 * throw an error. Note this subplan will return 0 tuples as the chunk is empty
 * and all rows are saved in the compressed chunk.
 */
static TupleTableSlot *
compress_chunk_dml_exec(CustomScanState *node)
{
	CompressChunkDmlState *state = (CompressChunkDmlState *) node;
	Oid chunk_relid = state->chunk_relid;
	elog(ERROR,
		 "cannot update/delete rows from chunk \"%s\" as it is compressed",
		 get_rel_name(chunk_relid));
	return NULL;
}

static void
compress_chunk_dml_end(CustomScanState *node)
{
	// CompressChunkDmlState *state = (CompressChunkDmlState *) node;
	PlanState *substate = linitial(node->custom_ps);
	ExecEndNode(substate);
}

static Path *
compress_chunk_dml_path_create(Path *subpath, Oid chunk_relid)
{
	CompressChunkDmlPath *path = (CompressChunkDmlPath *) palloc0(sizeof(CompressChunkDmlPath));

	memcpy(&path->cpath.path, subpath, sizeof(Path));
	path->cpath.path.type = T_CustomPath;
	path->cpath.path.pathtype = T_CustomScan;
	path->cpath.path.parent = subpath->parent;
	path->cpath.path.pathtarget = subpath->pathtarget;
	// path->cpath.path.param_info = subpath->param_info;
	path->cpath.methods = &compress_chunk_dml_path_methods;
	path->cpath.custom_paths = list_make1(subpath);
	path->chunk_relid = chunk_relid;

	return &path->cpath.path;
}

static Plan *
compress_chunk_dml_plan_create(PlannerInfo *root, RelOptInfo *relopt, CustomPath *best_path,
							   List *tlist, List *clauses, List *custom_plans)
{
	CompressChunkDmlPath *cdpath = (CompressChunkDmlPath *) best_path;
	CustomScan *cscan = makeNode(CustomScan);

	Assert(list_length(custom_plans) == 1);

	cscan->methods = &compress_chunk_dml_plan_methods;
	cscan->custom_plans = custom_plans;
	cscan->scan.scanrelid = relopt->relid;
	cscan->scan.plan.targetlist = tlist;
	cscan->custom_scan_tlist = NIL;
	cscan->custom_private = list_make1_oid(cdpath->chunk_relid);
	return &cscan->scan.plan;
}

static Node *
compress_chunk_dml_state_create(CustomScan *scan)
{
	CompressChunkDmlState *state;

	state = (CompressChunkDmlState *) newNode(sizeof(CompressChunkDmlState), T_CustomScanState);
	state->chunk_relid = linitial_oid(scan->custom_private);
	state->cscan_state.methods = &compress_chunk_dml_state_methods;
	return (Node *) state;
}

Path *
compress_chunk_dml_generate_paths(Path *subpath, Chunk *chunk)
{
	Assert(chunk->fd.compressed_chunk_id > 0);
	return compress_chunk_dml_path_create(subpath, chunk->table_id);
}
