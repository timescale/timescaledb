/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include "nodes/decompress_chunk/vector_quals.h"
#include <utils/memutils.h>

#include "arrow_tts.h"
#include "vector_quals.h"

/*
 * Support functions to execute vectorized quals over arrow tuple table slots.
 */

/*
 * Initialize the vector qual state.
 */
void
vector_qual_state_init(VectorQualState *vqstate, List *quals, TupleTableSlot *slot)
{
	MemSet(vqstate, 0, sizeof(VectorQualState));
	vqstate->vectorized_quals_constified = quals;
	vqstate->per_vector_mcxt = arrow_slot_per_segment_memory_context(slot);
	vqstate->get_arrow_array = vector_qual_state_get_arrow_array;
	vqstate->num_results = TupIsNull(slot) ? 0 : arrow_slot_total_row_count(slot);
	vqstate->slot = slot;
}

/*
 * Reset the vector qual state.
 *
 * The function should be called when all values in the arrow array have been
 * processed.
 */
void
vector_qual_state_reset(VectorQualState *vqstate)
{
	MemoryContextReset(vqstate->per_vector_mcxt);
	vqstate->vector_qual_result = NULL;
	vqstate->num_results = arrow_slot_total_row_count(vqstate->slot);
	arrow_slot_set_qual_result(vqstate->slot, NULL);
}

/*
 * Implementation of VectorQualState->get_arrow_array() for arrow tuple table
 * slots.
 *
 * Given a VectorQualState return the ArrowArray in the contained slot.
 */
const ArrowArray *
vector_qual_state_get_arrow_array(VectorQualState *vqstate, Expr *expr, bool *is_default_value)
{
	TupleTableSlot *slot = vqstate->slot;
	const Var *var = castNode(Var, expr);
	const int attoff = AttrNumberGetAttrOffset(var->varattno);
	const ArrowArray *array = arrow_slot_get_array(slot, var->varattno);

	if (array == NULL)
	{
		Form_pg_attribute attr = &slot->tts_tupleDescriptor->attrs[attoff];
		/*
		 * If getting here, this is a non-compressed value or a compressed
		 * column with a default value. We can treat non-compressed values the
		 * same as default ones. It is not possible to fall back to the
		 * non-vectorized quals now, so build a single-value ArrowArray with
		 * this (default) value, check if it passes the predicate, and apply
		 * it to the entire batch.
		 */
		array = make_single_value_arrow(attr->atttypid,
										slot->tts_values[attoff],
										slot->tts_isnull[attoff]);
		*is_default_value = true;
	}
	else
		*is_default_value = false;

	return array;
}

/*
 * Execute vectorized filter over a vector/array of values.
 *
 * Returns the number of values filtered until the first valid value.
 */
uint16
ExecVectorQual(VectorQualState *vqstate, ExprContext *econtext)
{
	TupleTableSlot *slot = econtext->ecxt_scantuple;
	const uint16 rowindex = arrow_slot_row_index(slot);

	/* Compute the vector quals over both compressed and non-compressed
	 * tuples. In case a non-compressed tuple is filtered, return SomeRowsPass
	 * although only one row will pass. */
	if (rowindex <= 1)
	{
		vector_qual_state_reset(vqstate);
		VectorQualSummary vector_qual_summary = vqstate->vectorized_quals_constified != NIL ?
													vector_qual_compute(vqstate) :
													AllRowsPass;

		switch (vector_qual_summary)
		{
			case NoRowsPass:
				return arrow_slot_total_row_count(slot);
			case AllRowsPass:
				/*
				 * If all rows pass, no need to test the vector qual for each row. This
				 * is a common case for time range conditions.
				 */
				vector_qual_state_reset(vqstate);
				return 0;
			case SomeRowsPass:
				break;
		}
	}

	/* Fast path when all rows have passed (i.e., no rows filtered). No need
	 * to check qual result and it should be NULL. */
	if (vqstate->vector_qual_result == NULL)
		return 0;

	const uint16 nrows = arrow_slot_total_row_count(slot);
	const uint16 off = arrow_slot_arrow_offset(slot);
	uint16 nfiltered = 0;

	for (uint16 i = off; i < nrows; i++)
	{
		if (arrow_row_is_valid(vqstate->vector_qual_result, i))
			break;
		nfiltered++;
	}

	arrow_slot_set_qual_result(slot, vqstate->vector_qual_result);

	return nfiltered;
}
