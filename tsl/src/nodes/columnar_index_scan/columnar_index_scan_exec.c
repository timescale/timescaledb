/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>

#include <access/sysattr.h>
#include <executor/executor.h>
#include <nodes/extensible.h>
#include <nodes/plannodes.h>
#include <utils/rel.h>

#include "columnar_index_scan.h"

typedef struct ColumnarIndexScanState
{
	CustomScanState custom;
	int num_outputs;
	AttrNumber *child_resnos; /* one per output column */
} ColumnarIndexScanState;

static void
columnar_index_scan_begin(CustomScanState *node, EState *estate, int eflags)
{
	ColumnarIndexScanState *state = (ColumnarIndexScanState *) node;
	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);

	/*
	 * Parse output_map from custom_private: a flat list of Integer values,
	 * one child_resno per output column.
	 */
	List *output_map = linitial(cscan->custom_private);
	int num_outputs = list_length(output_map);
	state->num_outputs = num_outputs;
	state->child_resnos = palloc(sizeof(AttrNumber) * num_outputs);

	ListCell *lc;
	int i = 0;
	foreach (lc, output_map)
	{
		state->child_resnos[i++] = intVal(lfirst(lc));
	}

	Assert(list_length(cscan->custom_plans) == 1);
	node->custom_ps = list_make1(ExecInitNode(linitial(cscan->custom_plans), estate, eflags));
}

static TupleTableSlot *
columnar_index_scan_exec(CustomScanState *node)
{
	ColumnarIndexScanState *state = (ColumnarIndexScanState *) node;
	PlanState *child_ps = linitial(node->custom_ps);
	TupleTableSlot *result_slot = node->ss.ps.ps_ResultTupleSlot;
	ExecClearTuple(result_slot);

	TupleTableSlot *child_slot = ExecProcNode(child_ps);
	if (TupIsNull(child_slot))
		return NULL;

	/*
	 * Copy values from the child slot to the output slot using the output_map.
	 * Each output column i gets its value from child_resnos[i].
	 */
	Datum *values = result_slot->tts_values;
	bool *nulls = result_slot->tts_isnull;

	for (int i = 0; i < state->num_outputs; i++)
	{
		if (state->child_resnos[i] == TableOidAttributeNumber)
		{
			values[i] = ObjectIdGetDatum(node->ss.ss_currentRelation->rd_id);
			nulls[i] = false;
		}
		else
		{
			values[i] = slot_getattr(child_slot, state->child_resnos[i], &nulls[i]);
		}
	}

	return ExecStoreVirtualTuple(result_slot);
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
	ColumnarIndexScanState *state =
		(ColumnarIndexScanState *) newNode(sizeof(ColumnarIndexScanState), T_CustomScanState);
	state->custom.methods = &exec_methods;
	return (Node *) state;
}
