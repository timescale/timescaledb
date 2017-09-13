#include "trigger.h"

#include <access/htup_details.h>
#include <access/heapam.h>
#include <utils/relcache.h>
#include <utils/builtins.h>
#include <utils/fmgroids.h>
#include <tcop/tcopprot.h>
#include <catalog/objectaddress.h>
#include <catalog/indexing.h>
#include <commands/trigger.h>
#include <access/xact.h>
#include <fmgr.h>


Form_pg_trigger
trigger_by_oid(Oid trigger_oid, bool missing_ok)
{
	Relation	trigRel;
	HeapTuple	tup;
	Form_pg_trigger trig = NULL;

	trigRel = heap_open(TriggerRelationId, AccessShareLock);

	tup = get_catalog_object_by_oid(trigRel, trigger_oid);

	if (!HeapTupleIsValid(tup))
	{
		if (!missing_ok)
			elog(ERROR, "could not find tuple for trigger %u",
				 trigger_oid);
	}
	else
	{
		trig = (Form_pg_trigger) GETSTRUCT(tup);
	}

	heap_close(trigRel, AccessShareLock);
	return trig;
}


bool
trigger_is_chunk_trigger(const Form_pg_trigger trigger)
{
	return trigger != NULL && TRIGGER_FOR_ROW(trigger->tgtype) && !trigger->tgisinternal;
}

char *
trigger_name(const Form_pg_trigger trigger)
{
	return NameStr(trigger->tgname);
}

/* all creation of triggers on chunks should go through this. Strictly speaking,
 * this deparsing is not necessary in all cases, but this keeps things consistent. */
void
trigger_create_on_chunk(Oid trigger_oid, char *chunk_schema_name, char *chunk_table_name)
{
	Datum		datum_def = DirectFunctionCall1(pg_get_triggerdef, ObjectIdGetDatum(trigger_oid));
	const char *def = TextDatumGetCString(datum_def);
	List	   *deparsed_list;
	Node	   *deparsed_node;
	CreateTrigStmt *stmt;

	deparsed_list = pg_parse_query(def);
	Assert(list_length(deparsed_list) == 1);
	deparsed_node = linitial(deparsed_list);
	Assert(IsA(deparsed_node, CreateTrigStmt));
	stmt = (CreateTrigStmt *) deparsed_node;

	stmt->relation->relname = chunk_table_name;
	stmt->relation->schemaname = chunk_schema_name;

	CreateTrigger(stmt, def, InvalidOid, InvalidOid,
				  InvalidOid, InvalidOid, false);

	CommandCounterIncrement();	/* needed to prevent pg_class being updated
								 * twice */
}



void
trigger_create_on_all_chunks(Hypertable *ht, Chunk *chunk)
{
	ScanKeyData skey;
	Relation	tgrel;
	SysScanDesc tgscan;
	HeapTuple	htup;

	ScanKeyInit(&skey,
				Anum_pg_trigger_tgrelid,
				BTEqualStrategyNumber, F_OIDEQ, ht->main_table_relid);

	tgrel = heap_open(TriggerRelationId, AccessShareLock);
	tgscan = systable_beginscan(tgrel, TriggerRelidNameIndexId, true,
								NULL, 1, &skey);

	while (HeapTupleIsValid(htup = systable_getnext(tgscan)))
	{
		Form_pg_trigger pg_trigger = (Form_pg_trigger) GETSTRUCT(htup);
		Oid			trigger_oid = HeapTupleGetOid(htup);

		if (trigger_is_chunk_trigger(pg_trigger))
		{
			trigger_create_on_chunk(trigger_oid, NameStr(chunk->fd.schema_name), NameStr(chunk->fd.table_name));
		}
	}
	systable_endscan(tgscan);
	heap_close(tgrel, AccessShareLock);
}
