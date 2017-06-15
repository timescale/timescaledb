#include <postgres.h>
#include <funcapi.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <catalog/pg_opfamily.h>
#include <utils/rel.h>
#include <utils/tuplesort.h>
#include <utils/tqual.h>
#include <utils/rls.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/guc.h>
#include <commands/tablecmds.h>
#include <commands/trigger.h>

#include <access/xact.h>
#include <access/htup_details.h>
#include <access/heapam.h>

#include <miscadmin.h>
#include <fmgr.h>

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

Datum		insert_main_table_trigger(PG_FUNCTION_ARGS);
Datum		insert_main_table_trigger_after(PG_FUNCTION_ARGS);

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
	HeapTuple	tuple;
	Partition  *part;

	Datum		datum;
	bool		isnull;
	int64		timepoint;
	int64		spacepoint;
	PartitionEpoch *epoch;
	Dimension *time_dim, *space_dim;

	InsertChunkState *cstate;

	Oid			relid = trigdata->tg_relation->rd_id;
	TupleDesc	tupdesc = trigdata->tg_relation->rd_att;

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
		{
			insert_statement_state = insert_statement_state_new(relid);
		}

		oldctx = MemoryContextSwitchTo(insert_statement_state->mctx);


		/* Get the time dimension associated with the hypertable */
		time_dim = hypertable_time_dimension(insert_statement_state->hypertable);

		/*
		 * Get the timepoint from the tuple, converting to our internal time
		 * representation
		 */
		datum = heap_getattr(tuple, insert_statement_state->time_attno, tupdesc, &isnull);

		if (isnull)
		{
			elog(ERROR, "No time attribute in tuple");
		}

		timepoint = time_value_to_internal(datum, time_dim->fd.time_type);

		space_dim = hypertable_space_dimension(insert_statement_state->hypertable);
		
		/* Find correct partition */
		if (space_dim->num_slices > 1)
		{
			spacepoint = partitioning_func_apply_tuple(space_dim->partitioning, tuple, tupdesc);
		}
		else
		{
			spacepoint = KEYSPACE_PT_NO_PARTITIONING;
		}

		part = partition_epoch_get_partition(epoch, spacepoint);

		cstate = insert_statement_state_get_insert_chunk_state(insert_statement_state, part, epoch, timepoint);
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
