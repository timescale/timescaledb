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
 *
 *
 * N-key SkipScan needs to do 2^N null check stages when using the above scheme,
 * made even more complicated with having to change searches for previous keys.
 *
 * So we made a decision to support multikey SkipScan in NOT NULL mode only.
 *
 * For N-key SkipScan we search with these predicates when current key = K:
 * (key_1 = prev_1),...,(key_K > prev_K),(key_K+1 IS NOT NULL)...(key_N IS NOT NULL)
 *
 * As all skip keys are NOT NULL, "IS NOT NULL" fetches the tuple with no previous value.
 *
 * We start the search with K=1 i.e. with these predicates:
 * (key_1 IS NOT NULL),...,(key_N IS NOT NULL).
 *
 * When a tuple is fetched we set  K=N as we can fill all previous values, search is now:
 * (key_1 = prev_1),...,(key_N > prev_N)
 *
 * When no tuple is fetched and K>1 we can relax the search and move to previous key (K-1):
 * (key_1 = prev_1),...,(key_K-1 > prev_K-1),(key_K IS NOT NULL)...(key_N IS NOT NULL)
 *
 * When no tuple is fetched and K=1, we are done.
 *
 * Multikey SkipScan flowchart:
 *                   start (K=1)
 *                      |         +---------+
 *                      |         |         |
 *                      v         v         |
 *      +=================================+ |
 *      |   search for NOT NULL after K   | |
 *      +=================================+ |
 *            |                          |  |
 *            | found value              |  |
 *            v                          |  |
 *  +==============================+     |  |
 *  | search for values after prev |     |  |
 *  +==============================+     |  |
 *                       |               |  |
 *                       |   no value    |  |
 *                       v               v  |
 *                 +======================+ |
 *                 | K=1         | K>1      |
 *                 v             v          |
 *            /===========\   +=========+   |
 *            |   DONE    |   | K = K-1 |---+
 *            \===========/   +=========+
 *
 */

#include <postgres.h>
#include <access/genam.h>
#include <access/nbtree.h>
#include <nodes/extensible.h>
#include <nodes/pg_list.h>
#include <utils/datum.h>

#include "guc.h"
#include "nodes/decompress_chunk/decompress_chunk.h"
#include "nodes/decompress_chunk/exec.h"
#include "nodes/skip_scan/skip_scan.h"

typedef enum SkipScanStage
{
	SS_BEGIN = 0,
	SS_NULLS_FIRST,
	SS_NOT_NULL,
	SS_VALUES,
	SS_NULLS_LAST,
	SS_PREV_KEY,
	SS_END,
} SkipScanStage;

typedef struct SkipKeyData
{
	ScanKey skip_key;

	/* Comparison value filled in at runtime */
	Datum prev_datum;
	bool prev_is_null;

	/* Info about the type we are performing DISTINCT on */
	bool distinct_by_val;
	int distinct_col_attnum;
	int distinct_typ_len;
	int sk_attno;
	SkipKeyNullStatus nulls;
} SkipKeyData;

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

	int num_skip_keys;
	SkipKeyData *skip_keys;

	/* Skip key with ">" qual, coming after "=" skip quals for multikey SkipScan */
	int current_key;

	/* For Multikey SkipScan we keep copies of "sk_func" for "=" and ">" for keys 1..N-1
	 * to be swapped during execution.
	 */
	FmgrInfo *eq_funcs;
	/* Will be filled after IndexScan scankeys have been initialized */
	FmgrInfo *comp_funcs;
	StrategyNumber *comp_strategies;

	SkipScanStage stage;

	/* rescan required before getting next tuple */
	bool needs_rescan;

	/* child_plan node is the input for skip scan plan node.
	 * if skip scan is directly over index scan, child_plan = idx_scan
	 * if skip scan is over compressed chunk,
	 * idx_scan = compressed index scan,
	 * child_plan = decompressed input into skip scan
	 */
	Plan *child_plan;
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

	node->custom_ps = list_make1((ScanState *) ExecInitNode(state->child_plan, estate, eflags));
	ScanState *child_state = linitial(node->custom_ps);
	if (state->child_plan == state->idx_scan)
	{
		state->idx = child_state;
	}
	else if (IsA(child_state, CustomScanState))
	{
		Assert(ts_is_columnar_scan_plan(state->child_plan));
		state->idx = linitial(castNode(CustomScanState, child_state)->custom_ps);
	}
	else
		elog(ERROR, "unknown subscan type in SkipScan");

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
	ScanKey scankeydata = *state->scan_keys;
	int j = 0;
	for (int i = 0; i < *state->num_scan_keys; i++)
	{
		if (scankeydata[i].sk_flags == SK_ISNULL &&
			scankeydata[i].sk_attno == state->skip_keys[j].sk_attno)
		{
			SkipKeyData *skipkeydata = &state->skip_keys[j++];
			skipkeydata->skip_key = &scankeydata[i];
			/* Set up ">" sk_func swaps for skip keys 1..N-1 */
			if (j < state->num_skip_keys)
			{
				state->comp_strategies[j - 1] = scankeydata[i].sk_strategy;
				fmgr_info_copy(&state->comp_funcs[j - 1],
							   &scankeydata[i].sk_func,
							   CurrentMemoryContext);
			}
			if (j == state->num_skip_keys)
				break;
		}
	}
	if (j < state->num_skip_keys)
		elog(ERROR, "ScanKey for skip qual not found");

	/* when we fetch the 1st tuple we update all skip keys from 0 to N */
	state->current_key = 0;
}

static bool
has_nulls_first(SkipScanState *state)
{
	return state->skip_keys[0].nulls == SK_NULLS_FIRST;
}

static bool
has_nulls_last(SkipScanState *state)
{
	return state->skip_keys[0].nulls == SK_NULLS_LAST;
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
	{
		index_rescan(*state->scan_desc,
					 *state->scan_keys,
					 *state->num_scan_keys,
					 NULL /*orderbys*/,
					 0 /*norderbys*/);

		/* Discard current compressed index tuple as we are ready to move to the next compressed
		 * tuple via SkipScan */
		ScanState *child = linitial(state->cscan_state.custom_ps);
		if (ts_is_columnar_scan_plan(state->child_plan))
		{
			ColumnarScanState *ds = (ColumnarScanState *) child;
			TupleTableSlot *slot = ds->batch_queue->funcs->top_tuple(ds->batch_queue);
			if (slot)
			{
				compressed_batch_discard_tuples((DecompressBatchState *) slot);
			}
		}
	}
	state->needs_rescan = false;
}

/*
 * Update skip scankey flags according to stage
 */
static void
skip_scan_switch_stage(SkipScanState *state, SkipScanStage new_stage)
{
	Assert(new_stage > state->stage || state->num_skip_keys > 1);

	switch (new_stage)
	{
		case SS_NOT_NULL:
			for (int i = 0; i < state->num_skip_keys; i++)
			{
				state->skip_keys[i].skip_key->sk_flags = SK_ISNULL | SK_SEARCHNOTNULL;
				state->skip_keys[i].skip_key->sk_argument = 0;
			}
			state->current_key = 0;
			state->needs_rescan = true;
			break;

		case SS_PREV_KEY:
			/* Done searching with ">" for this key: set this key to NOT NULL i.e. any value,
			 * set previous "=" key to search with ">".
			 */
			state->skip_keys[state->current_key].skip_key->sk_flags = SK_ISNULL | SK_SEARCHNOTNULL;
			state->current_key--;
			state->skip_keys[state->current_key].skip_key->sk_flags = 0;
			fmgr_info_copy(&state->skip_keys[state->current_key].skip_key->sk_func,
						   &state->comp_funcs[state->current_key],
						   CurrentMemoryContext);
			state->skip_keys[state->current_key].skip_key->sk_strategy =
				state->comp_strategies[state->current_key];
			state->needs_rescan = true;
			break;

		case SS_VALUES:
			for (int i = 0; i < state->num_skip_keys; i++)
			{
				state->skip_keys[i].skip_key->sk_flags = 0;
				/* reset all ">" back to "=" from the current key to N-1 */
				if (i >= state->current_key && i < state->num_skip_keys - 1)
				{
					fmgr_info_copy(&state->skip_keys[i].skip_key->sk_func,
								   &state->eq_funcs[i],
								   CurrentMemoryContext);
					state->skip_keys[i].skip_key->sk_strategy = BTEqualStrategyNumber;
				}
			}
#if PG18_GE
			/* PG18+ skip arrays are not used for "=",..,"=",">" multikey index quals,
			 * but so->skipScan is never reset to false in PG18
			 * when we change from ">",...,">" quals to "=",..,"=",">",
			 * so we reset it here.
			 * https://github.com/postgres/postgres/commit/8a51027
			 */
			if (*state->scan_desc && state->num_skip_keys > 1)
			{
				BTScanOpaque so = (BTScanOpaque) (*state->scan_desc)->opaque;
				so->skipScan = false;
			}
#endif
			state->current_key = state->num_skip_keys - 1;
			state->needs_rescan = true;
			break;

		case SS_NULLS_LAST:
		case SS_NULLS_FIRST:
			state->skip_keys[0].skip_key->sk_flags = SK_ISNULL | SK_SEARCHNULL;
			state->skip_keys[0].skip_key->sk_argument = 0;
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
	for (int i = state->current_key; i < state->num_skip_keys; i++)
	{
		if (!state->skip_keys[i].prev_is_null && !state->skip_keys[i].distinct_by_val)
		{
			Assert(state->stage == SS_VALUES || state->num_skip_keys > 1);
			pfree(DatumGetPointer(state->skip_keys[i].prev_datum));
		}

		MemoryContext old_ctx = MemoryContextSwitchTo(state->ctx);
		state->skip_keys[i].prev_datum = slot_getattr(slot,
													  state->skip_keys[i].distinct_col_attnum,
													  &state->skip_keys[i].prev_is_null);
		if (state->skip_keys[i].prev_is_null)
		{
			state->skip_keys[i].skip_key->sk_flags = SK_ISNULL;
			state->skip_keys[i].skip_key->sk_argument = 0;
		}
		else
		{
			state->skip_keys[i].prev_datum = datumCopy(state->skip_keys[i].prev_datum,
													   state->skip_keys[i].distinct_by_val,
													   state->skip_keys[i].distinct_typ_len);
			state->skip_keys[i].skip_key->sk_argument = state->skip_keys[i].prev_datum;
		}
		MemoryContextSwitchTo(old_ctx);
	}
	/* we need to do a rescan whenever we modify the ScanKey */
	state->needs_rescan = true;
}

static TupleTableSlot *
skip_scan_exec(CustomScanState *node)
{
	SkipScanState *state = (SkipScanState *) node;
	TupleTableSlot *result;
	ScanState *child_state;

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
				child_state = linitial(state->cscan_state.custom_ps);
				result = child_state->ps.ExecProcNode(&child_state->ps);

				/*
				 * if we found a NULL value we return it, otherwise
				 * we restart the scan looking for non-NULL
				 */
				skip_scan_switch_stage(state, SS_NOT_NULL);
				if (!TupIsNull(result))
					return result;

				break;

			case SS_NOT_NULL:
			case SS_PREV_KEY:
			case SS_VALUES:
				child_state = linitial(state->cscan_state.custom_ps);
				result = child_state->ps.ExecProcNode(&child_state->ps);

				if (!TupIsNull(result))
				{
					/*
					 * if we found a tuple we update the skip scan key
					 * and look for values greater than the value we just
					 * found. If this is the first non-NULL value we
					 * also switch stage to look for values greater than
					 * that in subsequent calls.
					 */
					skip_scan_update_key(state, result);
					if (state->stage == SS_NOT_NULL || state->stage == SS_PREV_KEY)
						skip_scan_switch_stage(state, SS_VALUES);

					return result;
				}
				else
				{
					/*
					 * if there are no more values that satisfy
					 * the skip constraint we are either done
					 * for NULLS FIRST ordering or need to check
					 * for NULLs if we have NULLS LAST ordering
					 *
					 * Or we can move back one key for multikey SkipScan to relax the search,
					 * i.e. make current key NOT NULL (any value) and change previous search from
					 * "=" to ">"
					 */
					if (has_nulls_last(state))
						skip_scan_switch_stage(state, SS_NULLS_LAST);
					else if (state->current_key > 0)
						skip_scan_switch_stage(state, SS_PREV_KEY);
					else
						skip_scan_switch_stage(state, SS_END);
				}
				break;

			case SS_NULLS_LAST:
				child_state = linitial(state->cscan_state.custom_ps);
				result = child_state->ps.ExecProcNode(&child_state->ps);
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
	ScanState *child_state = linitial(state->cscan_state.custom_ps);
	ExecEndNode(&child_state->ps);
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

	for (int i = 0; i < state->num_skip_keys; i++)
	{
		state->skip_keys[i].prev_is_null = true;
		state->skip_keys[i].prev_datum = 0;
	}

	state->needs_rescan = false;
	ScanState *child_state = linitial(state->cscan_state.custom_ps);
	ExecReScan(&child_state->ps);
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

	state->child_plan = linitial(cscan->custom_plans);
	if (ts_is_columnar_scan_plan(state->child_plan))
	{
		CustomScan *csplan = castNode(CustomScan, state->child_plan);
		state->idx_scan = linitial(csplan->custom_plans);
	}
	else
	{
		state->idx_scan = state->child_plan;
	}
	state->stage = SS_BEGIN;

	/* set up N skipkeyinfos for N skip keys */
	List *skinfos = (List *) linitial(cscan->custom_private);
	state->num_skip_keys = list_length(skinfos);
	state->skip_keys = palloc(sizeof(SkipKeyData) * state->num_skip_keys);

	ListCell *lc;
	int i = 0;
	foreach (lc, skinfos)
	{
		List *skipkeyinfo = (List *) lfirst(lc);

		state->skip_keys[i].distinct_col_attnum = list_nth_int(skipkeyinfo, SK_DistinctColAttno);
		state->skip_keys[i].distinct_by_val = list_nth_int(skipkeyinfo, SK_DistinctByVal);
		state->skip_keys[i].distinct_typ_len = list_nth_int(skipkeyinfo, SK_DistinctTypeLen);
		state->skip_keys[i].nulls = list_nth_int(skipkeyinfo, SK_NullStatus);
		Assert(state->num_skip_keys == 1 || state->skip_keys[i].nulls == SK_NOT_NULL);
		state->skip_keys[i].sk_attno = list_nth_int(skipkeyinfo, SK_IndexKeyAttno);

		state->skip_keys[i].prev_is_null = true;
		i++;
	}

	state->eq_funcs = NULL;
	state->comp_funcs = NULL;
	state->comp_strategies = NULL;

	/* set up N-1 equality ops for N skip keys if N>1 */
	if (state->num_skip_keys > 1)
	{
		/* Should have a list of N-1 equality op Oids for N skip keys if N>1 */
		Assert(list_length(cscan->custom_private) == 2);
		List *eqoids = (List *) lsecond(cscan->custom_private);

		state->eq_funcs = palloc(sizeof(FmgrInfo) * (state->num_skip_keys - 1));
		state->comp_funcs = palloc(sizeof(FmgrInfo) * (state->num_skip_keys - 1));
		state->comp_strategies = palloc(sizeof(StrategyNumber) * (state->num_skip_keys - 1));

		int i = 0;
		/* Set up "=" sk_funcs for keys 1..N-1 */
		foreach (lc, eqoids)
		{
			Oid eqoid = lfirst_oid(lc);
			Assert(OidIsValid(eqoid));
			fmgr_info(eqoid, &state->eq_funcs[i++]);
		}
		Assert(i == state->num_skip_keys - 1);
	}

	state->cscan_state.methods = &skip_scan_state_methods;
	return (Node *) state;
}
