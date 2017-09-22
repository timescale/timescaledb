#include <postgres.h>
#include <commands/event_trigger.h>
#include <executor/executor.h>
#include <access/htup_details.h>

#include "event_trigger.h"

#define DDL_INFO_NATTS 9

/* Function manager info for the event "pg_event_trigger_ddl_commands", which is
 * used to retrieve information on executed DDL commands in an event
 * trigger. The function manager info is initialized on extension load. */
static FmgrInfo ddl_commands_fmgrinfo;

/*
 * Get a list of executed DDL commands in an event trigger.
 *
 * This function calls the function pg_event_trigger_ddl_commands(), which is
 * part of the event trigger API, and retrieves the DDL commands executed in
 * relation to the event trigger. It is only valid to call this function from
 * within an event trigger.
 */
List *
event_trigger_ddl_commands(void)
{
	ReturnSetInfo rsinfo;
	FunctionCallInfoData fcinfo;
	TupleTableSlot *slot;
	EState	   *estate = CreateExecutorState();
	List	   *objects = NIL;

	InitFunctionCallInfoData(fcinfo, &ddl_commands_fmgrinfo, 1, InvalidOid, NULL, NULL);
	MemSet(&rsinfo, 0, sizeof(rsinfo));
	rsinfo.type = T_ReturnSetInfo;
	rsinfo.allowedModes = SFRM_Materialize;
	rsinfo.econtext = CreateExprContext(estate);
	fcinfo.resultinfo = (fmNodePtr) &rsinfo;

	FunctionCallInvoke(&fcinfo);

	slot = MakeSingleTupleTableSlot(rsinfo.setDesc);

	while (tuplestore_gettupleslot(rsinfo.setResult, true, false, slot))
	{
		HeapTuple	tuple = ExecFetchSlotTuple(slot);
		CollectedCommand *cmd;
		Datum		values[DDL_INFO_NATTS];
		bool		nulls[DDL_INFO_NATTS];

		heap_deform_tuple(tuple, rsinfo.setDesc, values, nulls);

		if (rsinfo.setDesc->natts > 8 && !nulls[8])
		{
			cmd = (CollectedCommand *) DatumGetPointer(values[8]);
			objects = lappend(objects, cmd);
		}
	}

	FreeExprContext(rsinfo.econtext, false);
	FreeExecutorState(estate);
	ExecDropSingleTupleTableSlot(slot);

	return objects;
}

void
_event_trigger_init(void)
{
	fmgr_info(fmgr_internal_function("pg_event_trigger_ddl_commands"),
			  &ddl_commands_fmgrinfo);
}

void
_event_trigger_fini(void)
{
	/* Nothing to do */
}
