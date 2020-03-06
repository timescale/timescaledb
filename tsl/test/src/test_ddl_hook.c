/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <string.h>
#include <unistd.h>
#include <postgres.h>
#include <fmgr.h>

#include "export.h"
#include "process_utility.h"

#include "compat.h"
#include "event_trigger.h"
#include "cross_module_fn.h"

TS_FUNCTION_INFO_V1(ts_test_ddl_command_hook_reg);
TS_FUNCTION_INFO_V1(ts_test_ddl_command_hook_unreg);

static List *tsl_delayed_execution_list = NIL;

static void
test_ddl_command_start(ProcessUtilityArgs *args)
{
	Cache *hcache;
	ListCell *cell;
	Hypertable *ht;

	elog(NOTICE,
		 "test_ddl_command_start: %d hypertables, query: %s",
		 list_length(args->hypertable_list),
		 args->query_string);

	/*
	 * Hypertable oid from those commands is available in hypertable_list but
	 * cannot be resolved here until standard utility hook will synchronize new
	 * relation name and schema.
	 *
	 * Save hypertable list here for command_end execution to avoid statement
	 * parsing for second time.
	 */
	switch (nodeTag(args->parsetree))
	{
		case T_AlterObjectSchemaStmt:
		case T_RenameStmt:
			elog(NOTICE, "test_ddl_command_start: wait for ddl_command_end");
			tsl_delayed_execution_list = list_copy(args->hypertable_list);
			return;
		default:
			break;
	}

	hcache = ts_hypertable_cache_pin();

	foreach (cell, args->hypertable_list)
	{
		Oid relid = lfirst_oid(cell);

		ht = ts_hypertable_cache_get_entry(hcache, relid, false);

		elog(NOTICE,
			 "test_ddl_command_start: %s.%s",
			 NameStr(ht->fd.schema_name),
			 NameStr(ht->fd.table_name));
	}

	ts_cache_release(hcache);
}

static void
test_ddl_command_end(EventTriggerData *command)
{
	Cache *hcache;
	ListCell *cell;
	Hypertable *ht;

	elog(NOTICE, "test_ddl_command_end: %s", command->tag);

	if (tsl_delayed_execution_list == NIL)
		return;

	elog(NOTICE,
		 "test_ddl_command_end: %d hypertables scheduled",
		 list_length(tsl_delayed_execution_list));

	hcache = ts_hypertable_cache_pin();

	foreach (cell, tsl_delayed_execution_list)
	{
		Oid relid = lfirst_oid(cell);

		ht = ts_hypertable_cache_get_entry(hcache, relid, false);

		elog(NOTICE,
			 "test_ddl_command_end: %s.%s",
			 NameStr(ht->fd.schema_name),
			 NameStr(ht->fd.table_name));
	}

	ts_cache_release(hcache);

	pfree(tsl_delayed_execution_list);
	tsl_delayed_execution_list = NIL;
}

static int
event_trigger_event_cmp(const void *obj1, const void *obj2)
{
	const EventTriggerDropObject *obj[] = { *((const EventTriggerDropObject **) obj1),
											*((const EventTriggerDropObject **) obj2) };

	/* This only orders on object type for simplicity. Thus it is assumed that
	 * the order of objects with the same type is predictible across
	 * PostgreSQL versions */
	return obj[0]->type - obj[1]->type;
}

static void
test_sql_drop(List *dropped_objects)
{
	ListCell *lc;
	int num_objects = list_length(dropped_objects);
	EventTriggerDropObject **objects = palloc(num_objects * sizeof(EventTriggerDropObject *));
	int i = 0;

	/* Sort the list of dropped objects for predictible order in tests across
	 * PostgreSQL versions. Note that PG11 introduced a list_qsort function,
	 * but it is not available in earlier PostgreSQL versions so we're doing
	 * our own sorting. */
	foreach (lc, dropped_objects)
		objects[i++] = lfirst(lc);

	qsort(objects, num_objects, sizeof(EventTriggerDropObject *), event_trigger_event_cmp);

	for (i = 0; i < num_objects; i++)
	{
		EventTriggerDropObject *obj = objects[i];

		switch (obj->type)
		{
			case EVENT_TRIGGER_DROP_TABLE_CONSTRAINT:
			{
				EventTriggerDropTableConstraint *event = (EventTriggerDropTableConstraint *) obj;

				elog(NOTICE,
					 "test_sql_drop: constraint: %s.%s.%s",
					 event->schema,
					 event->table,
					 event->constraint_name);
				break;
			}
			case EVENT_TRIGGER_DROP_INDEX:
			{
				elog(NOTICE, "test_sql_drop: index");
				break;
			}
			case EVENT_TRIGGER_DROP_TABLE:
			{
				EventTriggerDropTable *event = (EventTriggerDropTable *) obj;

				elog(NOTICE, "test_sql_drop: table: %s.%s", event->schema, event->table_name);
				break;
			}
			case EVENT_TRIGGER_DROP_SCHEMA:
			{
				EventTriggerDropSchema *event = (EventTriggerDropSchema *) obj;

				elog(NOTICE, "test_sql_drop: schema: %s", event->schema);
				break;
			}
			case EVENT_TRIGGER_DROP_TRIGGER:
			{
				elog(NOTICE, "test_sql_drop: trigger");
				break;
			}
			case EVENT_TRIGGER_DROP_VIEW:
			{
				elog(NOTICE, "test_sql_drop: view");
				break;
			}
		}
	}
}

Datum
ts_test_ddl_command_hook_reg(PG_FUNCTION_ARGS)
{
	Assert(ts_cm_functions->ddl_command_start == NULL);
	ts_cm_functions->ddl_command_start = test_ddl_command_start;
	ts_cm_functions->ddl_command_end = test_ddl_command_end;
	ts_cm_functions->sql_drop = test_sql_drop;
	PG_RETURN_VOID();
}

Datum
ts_test_ddl_command_hook_unreg(PG_FUNCTION_ARGS)
{
	Assert(ts_cm_functions->ddl_command_start == test_ddl_command_start);
	ts_cm_functions->ddl_command_start = NULL;
	ts_cm_functions->ddl_command_end = NULL;
	ts_cm_functions->sql_drop = NULL;
	PG_RETURN_VOID();
}
