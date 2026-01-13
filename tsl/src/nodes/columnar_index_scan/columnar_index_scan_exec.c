/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <executor/executor.h>
#include <nodes/extensible.h>

typedef struct ColumnarIndexScanState
{
	CustomScanState csstate;

	List *output_map;

	/* Custom scan tuple slot */
	TupleTableSlot *custom_scan_slot;
} ColumnarIndexScanState;

extern Node *columnar_index_scan_state_create(CustomScan *cscan);
static void columnar_index_scan_begin(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *columnar_index_scan_exec(CustomScanState *node);
static void columnar_index_scan_end(CustomScanState *node);
static void columnar_index_scan_rescan(CustomScanState *node);

static CustomExecMethods columnar_index_scan_state_methods = {
	.CustomName = "ColumnarIndexScanState",
	.BeginCustomScan = columnar_index_scan_begin,
	.ExecCustomScan = columnar_index_scan_exec,
	.EndCustomScan = columnar_index_scan_end,
	.ReScanCustomScan = columnar_index_scan_rescan,
};

Node *
columnar_index_scan_state_create(CustomScan *cscan)
{
	ColumnarIndexScanState *state =
		(ColumnarIndexScanState *) newNode(sizeof(ColumnarIndexScanState), T_CustomScanState);

	state->csstate.methods = &columnar_index_scan_state_methods;
	state->output_map = linitial(cscan->custom_private);

	return (Node *) state;
}

static void
columnar_index_scan_begin(CustomScanState *node, EState *estate, int eflags)
{
	ColumnarIndexScanState *state = (ColumnarIndexScanState *) node;
	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
	Plan *compressed_scan = linitial(cscan->custom_plans);

	/* Initialize child plan */
	PlanState *child_state = ExecInitNode(compressed_scan, estate, eflags);
	node->custom_ps = list_make1(child_state);

	/* Use the scan tuple slot set up by PostgreSQL */
	state->custom_scan_slot = node->ss.ss_ScanTupleSlot;
}

static TupleTableSlot *
columnar_index_scan_exec(CustomScanState *node)
{
	ColumnarIndexScanState *state = (ColumnarIndexScanState *) node;

	for (;;)
	{
		TupleTableSlot *compressed_slot = ExecProcNode(linitial(node->custom_ps));

		if (TupIsNull(compressed_slot))
			return NULL;

		/* Build output tuple */
		TupleTableSlot *result_slot = state->custom_scan_slot;
		ExecClearTuple(result_slot);

		ListCell *lc;
		int i = 0;
		foreach (lc, state->output_map)
		{
			bool isnull;
			AttrNumber attno = lfirst_int(lc);
			Datum value = slot_getattr(compressed_slot, attno, &isnull);
			result_slot->tts_values[i] = isnull ? (Datum) 0 : value;
			result_slot->tts_isnull[i] = isnull;
			i++;
		}

		ExecStoreVirtualTuple(result_slot);

		return result_slot;
	}
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
