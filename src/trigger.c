#include <postgres.h>
#include <utils/rel.h>
#include <utils/lsyscache.h>
#include <utils/builtins.h>
#include <tcop/tcopprot.h>
#include <commands/trigger.h>
#include <access/xact.h>

#include "trigger.h"
#include "compat.h"

#if PG10
#include <parser/analyze.h>
#endif

static Trigger *
trigger_by_name_relation(Relation rel, const char *trigname, bool missing_ok)
{
	TriggerDesc *trigdesc = rel->trigdesc;
	Trigger    *trigger = NULL;

	if (trigdesc != NULL)
	{
		int			i;

		for (i = 0; i < trigdesc->numtriggers; i++)
		{
			trigger = &trigdesc->triggers[i];

			if (strncmp(trigger->tgname, trigname, NAMEDATALEN) == 0)
				break;

			trigger = NULL;
		}
	}

	if (!missing_ok && NULL == trigger)
		elog(ERROR, "No trigger %s for relation %s",
			 trigname, get_rel_name(rel->rd_id));

	return trigger;
}

Trigger *
trigger_by_name(Oid relid, const char *trigname, bool missing_ok)
{
	Relation	rel;
	Trigger    *trigger;

	rel = relation_open(relid, AccessShareLock);

	trigger = trigger_by_name_relation(rel, trigname, missing_ok);

	relation_close(rel, AccessShareLock);

	return trigger;
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

#if PG10
	do
	{
		RawStmt    *rawstmt = (RawStmt *) deparsed_node;
		ParseState *pstate = make_parsestate(NULL);
		Query	   *query;

		Assert(IsA(deparsed_node, RawStmt));
		pstate->p_sourcetext = def;
		query = transformTopLevelStmt(pstate, rawstmt);
		free_parsestate(pstate);
		stmt = (CreateTrigStmt *) query->utilityStmt;
	} while (0);
#elif PG96
	stmt = (CreateTrigStmt *) deparsed_node;
#endif

	Assert(IsA(stmt, CreateTrigStmt));
	stmt->relation->relname = chunk_table_name;
	stmt->relation->schemaname = chunk_schema_name;

	CreateTrigger(stmt, def, InvalidOid, InvalidOid,
				  InvalidOid, InvalidOid, false);

	CommandCounterIncrement();	/* needed to prevent pg_class being updated
								 * twice */
}

typedef bool (*trigger_handler) (Trigger *trigger, void *arg);

static inline void
for_each_trigger(Oid relid, trigger_handler on_trigger, void *arg)
{
	Relation	rel;

	rel = relation_open(relid, AccessShareLock);

	if (rel->trigdesc != NULL)
	{
		TriggerDesc *trigdesc = rel->trigdesc;
		int			i;

		for (i = 0; i < trigdesc->numtriggers; i++)
		{
			Trigger    *trigger = &trigdesc->triggers[i];

			if (!on_trigger(trigger, arg))
				return;
		}
	}

	relation_close(rel, AccessShareLock);
}

static bool
create_trigger_handler(Trigger *trigger, void *arg)
{
	Chunk	   *chunk = arg;

#if PG10
	if (TRIGGER_USES_TRANSITION_TABLE(trigger->tgnewtable) ||
		TRIGGER_USES_TRANSITION_TABLE(trigger->tgoldtable))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Hypertables do not support transition tables in triggers.")));
#endif
	if (trigger_is_chunk_trigger(trigger))
		trigger_create_on_chunk(trigger->tgoid,
								NameStr(chunk->fd.schema_name),
								NameStr(chunk->fd.table_name));

	return true;
}

void
trigger_create_all_on_chunk(Hypertable *ht, Chunk *chunk)
{
	for_each_trigger(ht->main_table_relid, create_trigger_handler, chunk);
}

#if PG10
static bool
check_for_transition_table(Trigger *trigger, void *arg)
{
	bool	   *found = arg;

	if (TRIGGER_USES_TRANSITION_TABLE(trigger->tgnewtable) ||
		TRIGGER_USES_TRANSITION_TABLE(trigger->tgoldtable))
	{
		*found = true;
		return false;
	}

	return true;
}
#endif

bool
relation_has_transition_table_trigger(Oid relid)
{
	bool		found = false;

#if PG10
	for_each_trigger(relid, check_for_transition_table, &found);
#endif

	return found;
}
