#include <postgres.h>
#include <commands/event_trigger.h>
#include <utils/builtins.h>
#include <executor/executor.h>
#include <access/htup_details.h>
#include <catalog/pg_type.h>
#include <catalog/pg_constraint.h>
#include <catalog/pg_class.h>

#include "event_trigger.h"

#define DDL_INFO_NATTS 9
#define DROPPED_OBJECTS_NATTS 12

/* Function manager info for the event "pg_event_trigger_ddl_commands", which is
 * used to retrieve information on executed DDL commands in an event
 * trigger. The function manager info is initialized on extension load. */
static FmgrInfo ddl_commands_fmgrinfo;
static FmgrInfo dropped_objects_fmgrinfo;

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

/* Given a TEXT[] of addrnames return a list of heap allocated char *
 *
 * similar to textarray_to_strvaluelist */
static List *
extract_addrnames(ArrayType *arr)
{
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;
	List	   *list = NIL;
	int			i;

	deconstruct_array(arr, TEXTOID, -1, false, 'i',
					  &elems, &nulls, &nelems);

	for (i = 0; i < nelems; i++)
	{
		if (nulls[i])
			elog(ERROR, "unexpected null in name list");

		/* TextDatumGetCString heap allocates the string */
		list = lappend(list, TextDatumGetCString(elems[i]));
	}

	return list;
}

static EventTriggerDropTableConstraint *
makeEventTriggerDropTableConstraint(char *constraint_name, char *schema, char *table)
{
	EventTriggerDropTableConstraint *obj = palloc(sizeof(EventTriggerDropTableConstraint));

	*obj = (EventTriggerDropTableConstraint)
	{
		.obj =
		{
			.type = EVENT_TRIGGER_DROP_TABLE_CONSTRAINT
		},
			.constraint_name = constraint_name,
			.schema = schema,
			.table = table
	};

	return obj;
}

static EventTriggerDropIndex *
makeEventTriggerDropIndex(char *index_name, char *schema)
{
	EventTriggerDropIndex *obj = palloc(sizeof(EventTriggerDropIndex));

	*obj = (EventTriggerDropIndex)
	{
		.obj =
		{
			.type = EVENT_TRIGGER_DROP_INDEX
		},
			.index_name = index_name,
			.schema = schema,
	};
	return obj;
}

List *
event_trigger_dropped_objects(void)
{
	ReturnSetInfo rsinfo;
	FunctionCallInfoData fcinfo;
	TupleTableSlot *slot;
	EState	   *estate = CreateExecutorState();
	List	   *objects = NIL;

	InitFunctionCallInfoData(fcinfo, &dropped_objects_fmgrinfo, 0, InvalidOid, NULL, NULL);
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
		Datum		values[DROPPED_OBJECTS_NATTS];
		bool		nulls[DROPPED_OBJECTS_NATTS];
		Oid			class_id;
		char	   *objtype;

		heap_deform_tuple(tuple, rsinfo.setDesc, values, nulls);

		class_id = DatumGetObjectId(values[0]);

		switch (class_id)
		{
			case ConstraintRelationId:
				objtype = TextDatumGetCString(values[6]);
				if (objtype != NULL && strcmp(objtype, "table constraint") == 0)
				{
					List	   *addrnames = extract_addrnames(DatumGetArrayTypeP(values[10]));

					objects = lappend(objects,
									  makeEventTriggerDropTableConstraint(lthird(addrnames),
																		  linitial(addrnames),
																		  lsecond(addrnames)));
				}
				break;
			case RelationRelationId:
				objtype = TextDatumGetCString(values[6]);
				if (objtype != NULL && strcmp(objtype, "index") == 0)
				{
					List	   *addrnames = extract_addrnames(DatumGetArrayTypeP(values[10]));

					objects = lappend(objects,
									  makeEventTriggerDropIndex(lsecond(addrnames),
																linitial(addrnames)));
				}
				break;
			default:
				break;
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
	fmgr_info(fmgr_internal_function("pg_event_trigger_dropped_objects"),
			  &dropped_objects_fmgrinfo);
}

void
_event_trigger_fini(void)
{
	/* Nothing to do */
}
