/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * SkipScan is an optimized form of SELECT DISTINCT ON (column)
 * Conceptually, a SkipScan is a regular IndexScan with an additional skip-qual like
 *     WHERE column > [previous value of column]
 *
 * Implementing this qual is complicated by two factors:
 *   1. The first time through the SkipScan there is no previous value for the
 *      DISTINCT column.
 *   2. NULL values don't behave nicely with ordering operators.
 *
 * To get around these issues, we have to special case those two cases. All in
 * all, the SkipScan's state machine evolves according to the following flowchart
 *
 *                        start
 *                          |
 *          +================================+
 *          | search for NULL if NULLS FIRST |
 *          +================================+
 *                  |
 *                  v
 *  +=====================+          +==============================+
 *  | search for non-NULL |--found-->| search for values after prev |
 *  +=====================+  value   +==============================+
 *                  |                   |
 *                  v                   v
 *           +================================+
 *           | search for NULL if NULLS LAST  |
 *           +================================+
 *                          |
 *                          v
 *                    /===========\
 *                    |   DONE    |
 *                    \===========/
 *
 */

#include <postgres.h>
#include <access/genam.h>
#include <nodes/extensible.h>
#include <nodes/pg_list.h>
#include <utils/datum.h>

#include "guc.h"
#include "nodes/skip_scan/skip_scan.h"

typedef enum SkipScanStage
{
	SS_BEGIN = 0,
	SS_NULLS_FIRST,
	SS_NOT_NULL,
	SS_VALUES,
	SS_NULLS_LAST,
	SS_END,
} SkipScanStage;

typedef struct SkipScanState
{
	CustomScanState cscan_state;
	IndexScanDesc *scan_desc;
	MemoryContext ctx;

	/* Interior Index(Only)Scan the SkipScan runs over */
	ScanState *idx;

	/* Pointers into the Index(Only)Scan */
	int *num_scan_keys;
	ScanKey *scan_keys;
	ScanKey skip_key;

	Datum prev_datum;
	bool prev_is_null;

	/* Info about the type we are performing DISTINCT on */
	bool distinct_by_val;
	int distinct_col_attnum;
	int distinct_typ_len;
	int sk_attno;

	SkipScanStage stage;

	bool nulls_first;
	/* rescan required before getting next tuple */
	bool needs_rescan;

	void *idx_scan;
} SkipScanState;

static bool has_nulls_first(SkipScanState *state);
static bool has_nulls_last(SkipScanState *state);
static void skip_scan_rescan_index(SkipScanState *state);
static void skip_scan_switch_stage(SkipScanState *state, SkipScanStage new_stage);

static void
skip_scan_begin(CustomScanState *node, EState *estate, int eflags)
{
	SkipScanState *state = (SkipScanState *) node;
	state->ctx = AllocSetContextCreate(estate->es_query_cxt, "skipscan", ALLOCSET_DEFAULT_SIZES);

	state->idx = (ScanState *) ExecInitNode(state->idx_scan, estate, eflags);
	node->custom_ps = list_make1(state->idx);

	if (IsA(state->idx_scan, IndexScan))
	{
		IndexScanState *idx = castNode(IndexScanState, state->idx);
		state->scan_keys = &idx->iss_ScanKeys;
		state->num_scan_keys = &idx->iss_NumScanKeys;
		state->scan_desc = &idx->iss_ScanDesc;
	}
	else if (IsA(state->idx_scan, IndexOnlyScan))
	{
		IndexOnlyScanState *idx = castNode(IndexOnlyScanState, state->idx);
		state->scan_keys = &idx->ioss_ScanKeys;
		state->num_scan_keys = &idx->ioss_NumScanKeys;
		state->scan_desc = &idx->ioss_ScanDesc;
	}
	else
		elog(ERROR, "unknown subscan type in SkipScan");

	/* scankeys are not setup for explain only */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* find position of our skip key
	 * skip key is put as first key for the respective column in sort_indexquals
	 */
	ScanKey data = *state->scan_keys;
	for (int i = 0; i < *state->num_scan_keys; i++)
	{
		if (data[i].sk_flags == SK_ISNULL && data[i].sk_attno == state->sk_attno)
		{
			state->skip_key = &data[i];
			break;
		}
	}
	if (!state->skip_key)
		elog(ERROR, "ScanKey for skip qual not found");
}

static bool
has_nulls_first(SkipScanState *state)
{
	return state->nulls_first;
}

static bool
has_nulls_last(SkipScanState *state)
{
	return !has_nulls_first(state);
}

static void
skip_scan_rescan_index(SkipScanState *state)
{
	/* if the scan in the child scan has not been
	 * setup yet which is true before the first tuple
	 * has been retrieved from child scan we cannot
	 * trigger rescan but since the child scan
	 * has not been initialized it will pick up
	 * any ScanKey changes we did */
	if (*state->scan_desc)
		index_rescan(*state->scan_desc,
					 *state->scan_keys,
					 *state->num_scan_keys,
					 NULL /*orderbys*/,
					 0 /*norderbys*/);
	state->needs_rescan = false;
}

/*
 * Update skip scankey flags according to stage
 */
static void
skip_scan_switch_stage(SkipScanState *state, SkipScanStage new_stage)
{
	Assert(new_stage > state->stage);

	switch (new_stage)
	{
		case SS_NOT_NULL:
			state->skip_key->sk_flags = SK_ISNULL | SK_SEARCHNOTNULL;
			state->skip_key->sk_argument = 0;
			state->needs_rescan = true;
			break;

		case SS_VALUES:
			state->skip_key->sk_flags = 0;
			state->needs_rescan = true;
			break;

		case SS_NULLS_LAST:
		case SS_NULLS_FIRST:
			state->skip_key->sk_flags = SK_ISNULL | SK_SEARCHNULL;
			state->skip_key->sk_argument = 0;
			state->needs_rescan = true;
			break;

		case SS_BEGIN:
		case SS_END:
			break;
	}

	state->stage = new_stage;
}

static void
skip_scan_update_key(SkipScanState *state, TupleTableSlot *slot)
{
	if (!state->prev_is_null && !state->distinct_by_val)
	{
		Assert(state->stage == SS_VALUES);
		pfree(DatumGetPointer(state->prev_datum));
	}

	MemoryContext old_ctx = MemoryContextSwitchTo(state->ctx);
	state->prev_datum = slot_getattr(slot, state->distinct_col_attnum, &state->prev_is_null);
	if (state->prev_is_null)
	{
		state->skip_key->sk_flags = SK_ISNULL;
		state->skip_key->sk_argument = 0;
	}
	else
	{
		state->prev_datum =
			datumCopy(state->prev_datum, state->distinct_by_val, state->distinct_typ_len);
		state->skip_key->sk_argument = state->prev_datum;
	}

	MemoryContextSwitchTo(old_ctx);

	/* we need to do a rescan whenever we modify the ScanKey */
	state->needs_rescan = true;
}

static TupleTableSlot *
skip_scan_exec(CustomScanState *node)
{
	SkipScanState *state = (SkipScanState *) node;
	TupleTableSlot *result;

	/*
	 * We are not supporting projection here since no plan
	 * we generate will need it as our SkipScan node will
	 * always be below Unique need so our targetlist
	 * will not get modified by postgres.
	 */
	Assert(!node->ss.ps.ps_ProjInfo);

	while (true)
	{
		if (state->needs_rescan)
			skip_scan_rescan_index(state);

		switch (state->stage)
		{
			case SS_BEGIN:
				if (has_nulls_first(state))
					skip_scan_switch_stage(state, SS_NULLS_FIRST);
				else
					skip_scan_switch_stage(state, SS_NOT_NULL);

				break;

			case SS_NULLS_FIRST:
				result = state->idx->ps.ExecProcNode(&state->idx->ps);

				/*
				 * if we found a NULL value we return it, otherwise
				 * we restart the scan looking for non-NULL
				 */
				skip_scan_switch_stage(state, SS_NOT_NULL);
				if (!TupIsNull(result))
					return result;

				break;

			case SS_NOT_NULL:
			case SS_VALUES:
				result = state->idx->ps.ExecProcNode(&state->idx->ps);

				if (!TupIsNull(result))
				{
					/*
					 * if we found a tuple we update the skip scan key
					 * and look for values greater than the value we just
					 * found. If this is the first non-NULL value we
					 * also switch stage to look for values greater than
					 * that in subsequent calls.
					 */
					if (state->stage == SS_NOT_NULL)
						skip_scan_switch_stage(state, SS_VALUES);

					skip_scan_update_key(state, result);
					return result;
				}
				else
				{
					/*
					 * if there are no more values that satisfy
					 * the skip constraint we are either done
					 * for NULLS FIRST ordering or need to check
					 * for NULLs if we have NULLS LAST ordering
					 */
					if (has_nulls_last(state))
						skip_scan_switch_stage(state, SS_NULLS_LAST);
					else
						skip_scan_switch_stage(state, SS_END);
				}
				break;

			case SS_NULLS_LAST:
				result = state->idx->ps.ExecProcNode(&state->idx->ps);
				skip_scan_switch_stage(state, SS_END);
				return result;
				break;

			case SS_END:
				return NULL;
				break;
		}
	}
}

static void
skip_scan_end(CustomScanState *node)
{
	SkipScanState *state = (SkipScanState *) node;
	ExecEndNode(&state->idx->ps);
}

static void
skip_scan_rescan(CustomScanState *node)
{
	SkipScanState *state = (SkipScanState *) node;
	/* reset stage so we can assert in skip_scan_switch_stage that stage always moves forward */
	state->stage = SS_BEGIN;

	/* Switching state here instead of in the main loop
	 * means we dont have to call skip_scan_rescan_index
	 * as ExecReScan on the child scan takes care of that. */
	if (has_nulls_first(state))
		skip_scan_switch_stage(state, SS_NULLS_FIRST);
	else
		skip_scan_switch_stage(state, SS_NOT_NULL);

	state->prev_is_null = true;
	state->prev_datum = 0;

	state->needs_rescan = false;
	ExecReScan(&state->idx->ps);
	MemoryContextReset(state->ctx);
}

static CustomExecMethods skip_scan_state_methods = {
	.CustomName = "SkipScanState",
	.BeginCustomScan = skip_scan_begin,
	.EndCustomScan = skip_scan_end,
	.ExecCustomScan = skip_scan_exec,
	.ReScanCustomScan = skip_scan_rescan,
};

Node *
tsl_skip_scan_state_create(CustomScan *cscan)
{
	SkipScanState *state = (SkipScanState *) newNode(sizeof(SkipScanState), T_CustomScanState);

	state->idx_scan = linitial(cscan->custom_plans);
	state->stage = SS_BEGIN;

	state->distinct_col_attnum = linitial_int(cscan->custom_private);
	state->distinct_by_val = lsecond_int(cscan->custom_private);
	state->distinct_typ_len = lthird_int(cscan->custom_private);
	state->nulls_first = lfourth_int(cscan->custom_private);
	state->sk_attno = list_nth_int(cscan->custom_private, 4);

	state->prev_is_null = true;
	state->cscan_state.methods = &skip_scan_state_methods;
	return (Node *) state;
}
