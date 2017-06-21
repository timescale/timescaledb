#include <postgres.h>
#include <utils/rel.h>
#include <commands/trigger.h>

#include "cache.h"
#include "hypertable_cache.h"
#include "dimension.h"
#include "chunk_cache.h"
#include "errors.h"
#include "utils.h"
#include "metadata_queries.h"
#include "partitioning.h"
#include "scanner.h"
#include "catalog.h"
#include "chunk.h"
#include "insert_chunk_state.h"
#include "insert_statement_state.h"
#include "executor.h"

static void
insert_main_table_cleanup(InsertStatementState **state_p)
{
	if (*state_p != NULL)
	{
		insert_statement_state_destroy(*state_p);
		*state_p = NULL;
	}
}

InsertStatementState *insert_statement_state = NULL;

Datum       insert_main_table_trigger(PG_FUNCTION_ARGS);
Datum       insert_main_table_trigger_after(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(insert_main_table_trigger);
PG_FUNCTION_INFO_V1(insert_main_table_trigger_after);

/*
 * This row-level trigger is called for every row INSERTed into a hypertable. We
 * use it to redirect inserted tuples to the correct hypertable chunk in space
 * and time.
 *
 */
Datum
insert_main_table_trigger(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	HeapTuple   tuple;
	Hypertable *ht;
	Point *point;
	InsertChunkState *cstate;
	Oid         relid = trigdata->tg_relation->rd_id;
	TupleDesc   tupdesc = trigdata->tg_relation->rd_att;
	MemoryContext oldctx;

	PG_TRY();
	{
		/* Check that this is called the way it should be */
		if (!CALLED_AS_TRIGGER(fcinfo))
			elog(ERROR, "Trigger not called by trigger manager");

		if (!TRIGGER_FIRED_BEFORE(trigdata->tg_event))
			elog(ERROR, "Trigger should only fire before insert");

		if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
			tuple = trigdata->tg_newtuple;
		else if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
			tuple = trigdata->tg_trigtuple;
		else
			elog(ERROR, "Unsupported event for trigger");

		if (NULL == insert_statement_state)
			insert_statement_state = insert_statement_state_new(relid);

		oldctx = MemoryContextSwitchTo(insert_statement_state->mctx);
		ht = insert_statement_state->hypertable;

		/* Calculate the tuple's point in the N-dimensional hyperspace */
		point = hyperspace_calculate_point(ht->space, tuple, tupdesc);

		elog(NOTICE, "Point is %s", point_to_string(point));

		/* Find or create the insert state matching the point */
		cstate = insert_statement_state_get_insert_chunk_state(insert_statement_state,
															   ht->space, point);
		insert_chunk_state_insert_tuple(cstate, tuple);

		MemoryContextSwitchTo(oldctx);
	}
	PG_CATCH();
	{
		insert_main_table_cleanup(&insert_statement_state);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/*
	 * add 1 to the number of processed tuples in the commandTag. Without this
	 * tuples that return NULL in before triggers are not counted.
	 */
	executor_add_number_tuples_processed(1);

	/* Return NULL since we do not want the tuple in the trigger's table */
	return PointerGetDatum(NULL);
}

/* Trigger called after all tuples of the insert statement are done */

Datum
insert_main_table_trigger_after(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;

	PG_TRY();
	{
		if (!CALLED_AS_TRIGGER(fcinfo))
			elog(ERROR, "not called by trigger manager");

		if (!TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event) &&
			!TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
			elog(ERROR, "Unsupported event for trigger");
	}
	PG_CATCH();
	{
		insert_main_table_cleanup(&insert_statement_state);
		PG_RE_THROW();
	}
	PG_END_TRY();

	insert_main_table_cleanup(&insert_statement_state);

	return PointerGetDatum(NULL);
}
