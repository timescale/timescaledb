/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * This grouping policy aggregates entire compressed batches. It can be used to
 * aggregate with no grouping, or to produce partial aggregates per each batch
 * to group by segmentby columns.
 */

#include <postgres.h>

#include <executor/tuptable.h>
#include <nodes/pg_list.h>

#include "grouping_policy.h"

#include "nodes/decompress_chunk/compressed_batch.h"
#include "nodes/vector_agg/exec.h"

typedef struct
{
	Datum key;
	uint32 status;
	uint32 agg_state_index;
} HashEntry;

static uint64_t
hash64(uint64_t x)
{
	x ^= x >> 30;
	x *= 0xbf58476d1ce4e5b9U;
	x ^= x >> 27;
	x *= 0x94d049bb133111ebU;
	x ^= x >> 31;
	return x;
}

#define SH_PREFIX h
#define SH_ELEMENT_TYPE HashEntry
#define SH_KEY_TYPE Datum
#define SH_KEY key
#define SH_HASH_KEY(tb, key) hash64(key)
#define SH_EQUAL(tb, a, b) a == b
#define SH_SCOPE static inline
#define SH_DECLARE
#define SH_DEFINE
#include "lib/simplehash.h"

struct h_hash;

typedef struct
{
	GroupingPolicy funcs;
	List *agg_defs;
	List *output_grouping_columns;
	bool partial_per_batch;
	struct h_hash *table;
	bool have_null_key;
	struct h_iterator iter;
	bool returning_results;

	/*
	 * A memory context for aggregate functions to allocate additional data,
	 * i.e. if they store strings or float8 datum on 32-bit systems. Valid until
	 * the grouping policy is reset.
	 */
	MemoryContext agg_extra_mctx;

	uint64 aggstate_bytes_per_key;
	uint64 allocated_aggstate_rows;
	List *per_agg_states;

	uint64 stat_input_valid_rows;
} GroupingPolicyHash;

static const GroupingPolicy grouping_policy_hash_functions;

GroupingPolicy *
create_grouping_policy_hash(List *agg_defs, List *output_grouping_columns)
{
	GroupingPolicyHash *policy = palloc0(sizeof(GroupingPolicyHash));
	policy->funcs = grouping_policy_hash_functions;
	policy->output_grouping_columns = output_grouping_columns;
	policy->agg_defs = agg_defs;
	policy->agg_extra_mctx =
		AllocSetContextCreate(CurrentMemoryContext, "agg extra", ALLOCSET_DEFAULT_SIZES);
	policy->allocated_aggstate_rows = 1000;
	ListCell *lc;
	foreach (lc, agg_defs)
	{
		VectorAggDef *def = lfirst(lc);
		policy->aggstate_bytes_per_key += def->func->state_bytes;

		policy->per_agg_states =
			lappend(policy->per_agg_states,
					palloc0(def->func->state_bytes * policy->allocated_aggstate_rows));
	}

	policy->table = h_create(CurrentMemoryContext, 1000, NULL);
	policy->have_null_key = false;

	policy->returning_results = false;

	return &policy->funcs;
}

static void
gp_hash_reset(GroupingPolicy *obj)
{
	GroupingPolicyHash *policy = (GroupingPolicyHash *) obj;

	MemoryContextReset(policy->agg_extra_mctx);

	policy->returning_results = false;

	h_reset(policy->table);
	policy->have_null_key = false;

	policy->stat_input_valid_rows = 0;
}

static void
compute_single_aggregate(DecompressBatchState *batch_state, VectorAggDef *agg_def, void *agg_states,
						 int32 *offsets, MemoryContext agg_extra_mctx)
{
	ArrowArray *arg_arrow = NULL;
	Datum arg_datum = 0;
	bool arg_isnull = true;

	/*
	 * We have functions with one argument, and one function with no arguments
	 * (count(*)). Collect the arguments.
	 */
	if (agg_def->input_offset >= 0)
	{
		CompressedColumnValues *values = &batch_state->compressed_columns[agg_def->input_offset];
		Assert(values->decompression_type != DT_Invalid);
		Assert(values->decompression_type != DT_Iterator);

		if (values->arrow != NULL)
		{
			arg_arrow = values->arrow;
		}
		else
		{
			Assert(values->decompression_type == DT_Scalar);
			arg_datum = *values->output_value;
			arg_isnull = *values->output_isnull;
		}
	}

	/*
	 * Now call the function.
	 */
	if (arg_arrow != NULL)
	{
		/* Arrow argument. */
		agg_def->func->agg_many(agg_states, offsets, arg_arrow, agg_extra_mctx);
	}
	else
	{
		/*
		 * Scalar argument, or count(*).
		 */
		for (int i = 0; i < batch_state->total_batch_rows; i++)
		{
			if (!arrow_row_is_valid(batch_state->vector_qual_result, i))
			{
				continue;
			}

			if (offsets[i] == 0)
			{
				continue;
			}

			void *state = (offsets[i] * agg_def->func->state_bytes + (char *) agg_states);
			agg_def->func->agg_const(state, arg_datum, arg_isnull, 1, agg_extra_mctx);
		}
	}
}

static void
gp_hash_add_batch(GroupingPolicy *gp, DecompressBatchState *batch_state)
{
	GroupingPolicyHash *policy = (GroupingPolicyHash *) gp;

	Assert(!policy->returning_results);

	/*
	 * State index zero is invalid, and state index one is for null key. We have
	 * to initialize it at the first run.
	 */
	const uint64 last_initialized_state_index =
		policy->table->members ? policy->table->members + 2 : 1;
	uint64 next_unused_state_index = policy->table->members + 2;

	int32 offsets[1000] = { 0 };
	Assert(batch_state->total_batch_rows <= 1000);

	/*
	 * For the partial aggregation node, the grouping columns are always in the
	 * output, so we don't have to separately look at the list of the grouping
	 * columns.
	 */
	Assert(list_length(policy->output_grouping_columns) == 1);
	GroupingColumn *g = linitial(policy->output_grouping_columns);
	CompressedColumnValues *gv = &batch_state->compressed_columns[g->input_offset];
	// Assert(gv->decompression_type == 8 /* lolwut */);
	Assert(gv->decompression_type > 0);
	const void *vv = gv->arrow->buffers[1];
	const uint64 *key_validity = gv->arrow->buffers[0];
	const uint64 *filter = batch_state->vector_qual_result;
	for (int row = 0; row < batch_state->total_batch_rows; row++)
	{
		Datum key = { 0 };
		memcpy(&key, gv->decompression_type * row + (char *) vv, gv->decompression_type);
		if (!arrow_row_is_valid(filter, row))
		{
			continue;
		}

		if (arrow_row_is_valid(key_validity, row))
		{
			bool found = false;
			HashEntry *entry = h_insert(policy->table, key, &found);
			if (!found)
			{
				entry->agg_state_index = next_unused_state_index++;
			}
			offsets[row] = entry->agg_state_index;
		}
		else
		{
			policy->have_null_key = true;
			offsets[row] = 1;
		}
	}

	policy->stat_input_valid_rows += arrow_num_valid(key_validity, batch_state->total_batch_rows);

	ListCell *aggdeflc;
	ListCell *aggstatelc;

	if (next_unused_state_index > last_initialized_state_index)
	{
		if (next_unused_state_index > policy->allocated_aggstate_rows)
		{
			policy->allocated_aggstate_rows = policy->allocated_aggstate_rows * 2 + 1;
			forboth (aggdeflc, policy->agg_defs, aggstatelc, policy->per_agg_states)
			{
				VectorAggDef *def = lfirst(aggdeflc);
				lfirst(aggstatelc) =
					repalloc(lfirst(aggstatelc),
							 policy->allocated_aggstate_rows * def->func->state_bytes);
			}
		}

		forboth (aggdeflc, policy->agg_defs, aggstatelc, policy->per_agg_states)
		{
			VectorAggDef *def = lfirst(aggdeflc);
			for (uint64 i = last_initialized_state_index; i < next_unused_state_index; i++)
			{
				void *aggstate = def->func->state_bytes * i + (char *) lfirst(aggstatelc);
				def->func->agg_init(aggstate);
			}
		}
	}

	forboth (aggdeflc, policy->agg_defs, aggstatelc, policy->per_agg_states)
	{
		compute_single_aggregate(batch_state,
								 lfirst(aggdeflc),
								 lfirst(aggstatelc),
								 offsets,
								 policy->agg_extra_mctx);
	}
}

static bool
gp_hash_should_emit(GroupingPolicy *gp)
{
	GroupingPolicyHash *policy = (GroupingPolicyHash *) gp;
	(void) policy;
//	if (policy->table->members + policy->have_null_key > 0)
//	{
//		return true;
//	}
	return false;
}

static bool
gp_hash_do_emit(GroupingPolicy *gp, TupleTableSlot *aggregated_slot)
{
	GroupingPolicyHash *policy = (GroupingPolicyHash *) gp;

	if (!policy->returning_results)
	{
		/* FIXME doesn't work on final result emission w/o should_emit. */
		policy->returning_results = true;
		h_start_iterate(policy->table, &policy->iter);
//		fprintf(stderr, "spill after %ld input rows, %d keys, %f ratio\n",
//			policy->stat_input_valid_rows, policy->table->members + policy->have_null_key,
//				policy->stat_input_valid_rows / (float) (policy->table->members + policy->have_null_key));
	}

	HashEntry null_key_entry = { .agg_state_index = 1 };
	HashEntry *entry = h_iterate(policy->table, &policy->iter);
	bool key_is_null = false;
	if (entry == NULL && policy->have_null_key)
	{
		policy->have_null_key = false;
		entry = &null_key_entry;
		key_is_null = true;
	}

	if (entry == NULL)
	{
		policy->returning_results = false;
		return false;
	}

	const int naggs = list_length(policy->agg_defs);
	for (int i = 0; i < naggs; i++)
	{
		VectorAggDef *agg_def = (VectorAggDef *) list_nth(policy->agg_defs, i);
		void *agg_states = list_nth(policy->per_agg_states, i);
		void *agg_state = entry->agg_state_index * agg_def->func->state_bytes + (char *) agg_states;
		agg_def->func->agg_emit(agg_state,
								&aggregated_slot->tts_values[agg_def->output_offset],
								&aggregated_slot->tts_isnull[agg_def->output_offset]);
	}

	Assert(list_length(policy->output_grouping_columns) == 1);
	GroupingColumn *col = linitial(policy->output_grouping_columns);
	aggregated_slot->tts_values[col->output_offset] = entry->key;
	aggregated_slot->tts_isnull[col->output_offset] = key_is_null;

	return true;
}

static const GroupingPolicy grouping_policy_hash_functions = {
	.gp_reset = gp_hash_reset,
	.gp_add_batch = gp_hash_add_batch,
	.gp_should_emit = gp_hash_should_emit,
	.gp_do_emit = gp_hash_do_emit,
};
