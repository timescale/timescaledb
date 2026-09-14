/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>

#include <access/sysattr.h>
#include <executor/executor.h>
#include <executor/tuptable.h>
#include <nodes/extensible.h>
#include <nodes/plannodes.h>
#include <utils/rel.h>

#include "columnar_index_scan.h"

typedef struct ColumnarIndexScanState
{
	CustomScanState custom;
	int num_outputs;
	AttrNumber *result_attnos;
	AttrNumber *child_resnos;
} ColumnarIndexScanState;

static void
columnar_index_scan_begin(CustomScanState *node, EState *estate, int eflags)
{
	ColumnarIndexScanState *state = (ColumnarIndexScanState *) node;
	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
	List *output_map = cscan->custom_private;

	Assert(list_length(output_map) % 2 == 0);
	int num_outputs = list_length(output_map) / 2;
	state->num_outputs = num_outputs;
	state->result_attnos = palloc(sizeof(AttrNumber) * num_outputs);
	state->child_resnos = palloc(sizeof(AttrNumber) * num_outputs);

	for (int i = 0; i < num_outputs; i++)
	{
		state->result_attnos[i] = list_nth_int(output_map, i * 2);
		state->child_resnos[i] = list_nth_int(output_map, i * 2 + 1);
	}

	Assert(list_length(cscan->custom_plans) == 1);
	node->custom_ps = list_make1(ExecInitNode(linitial(cscan->custom_plans), estate, eflags));
}

static TupleTableSlot *
columnar_index_scan_next(ScanState *node)
{
	ColumnarIndexScanState *state = (ColumnarIndexScanState *) node;
	PlanState *child_ps = linitial(state->custom.custom_ps);
	TupleTableSlot *child_slot = ExecProcNode(child_ps);
	if (TupIsNull(child_slot))
	{
		return NULL;
	}

	TupleTableSlot *scan_slot = node->ss_ScanTupleSlot;
	ExecStoreAllNullTuple(scan_slot);
	for (int i = 0; i < state->num_outputs; i++)
	{
		bool isnull;
		AttrNumber result_attno = state->result_attnos[i];
		AttrNumber child_resno = state->child_resnos[i];
		int result_index = AttrNumberGetAttrOffset(result_attno);

		if (child_resno == TableOidAttributeNumber)
		{
			scan_slot->tts_values[result_index] = ObjectIdGetDatum(node->ss_currentRelation->rd_id);
			scan_slot->tts_isnull[result_index] = false;
		}
		else
		{
			scan_slot->tts_values[result_index] = slot_getattr(child_slot, child_resno, &isnull);
			scan_slot->tts_isnull[result_index] = isnull;
		}
	}

	return scan_slot;
}

static bool
columnar_index_scan_recheck(ScanState *node, TupleTableSlot *slot)
{
	return true;
}

static TupleTableSlot *
columnar_index_scan_exec(CustomScanState *node)
{
	return ExecScan(&node->ss,
					(ExecScanAccessMtd) columnar_index_scan_next,
					(ExecScanRecheckMtd) columnar_index_scan_recheck);
}

static void
columnar_index_scan_end(CustomScanState *node)
{
	ExecEndNode(linitial(node->custom_ps));
}

static void
columnar_index_scan_rescan(CustomScanState *node)
{
	ExecReScan(linitial(node->custom_ps));
	ExecScanReScan(&node->ss);
}

static struct CustomExecMethods exec_methods = {
	.CustomName = COLUMNAR_INDEX_SCAN_NAME,
	.BeginCustomScan = columnar_index_scan_begin,
	.ExecCustomScan = columnar_index_scan_exec,
	.EndCustomScan = columnar_index_scan_end,
	.ReScanCustomScan = columnar_index_scan_rescan,
	.ExplainCustomScan = NULL,
};

Node *
columnar_index_scan_state_create(CustomScan *cscan)
{
	ColumnarIndexScanState *state = palloc0(sizeof(ColumnarIndexScanState));
	NodeSetTag(state, T_CustomScanState);

	state->custom.methods = &exec_methods;
	return (Node *) state;
}
