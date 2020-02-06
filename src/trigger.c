/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <utils/rel.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <utils/builtins.h>
#include <tcop/tcopprot.h>
#include <commands/trigger.h>
#include <access/xact.h>
#include <miscadmin.h>

#include "trigger.h"
#include "compat.h"

#if PG10_GE
#include <parser/analyze.h>
#endif

/*
 * Replicate a trigger on a chunk.
 *
 * Given a trigger OID (e.g., a Hypertable trigger), create the equivalent
 * trigger on a chunk.
 *
 * Note: it is assumed that this function is called under a user that has
 * permissions to modify the chunk since CreateTrigger() performs permissions
 * checks.
 */
void
ts_trigger_create_on_chunk(Oid trigger_oid, char *chunk_schema_name, char *chunk_table_name)
{
	Datum datum_def = DirectFunctionCall1(pg_get_triggerdef, ObjectIdGetDatum(trigger_oid));
	const char *def = TextDatumGetCString(datum_def);
	List *deparsed_list;
	Node *deparsed_node;
	CreateTrigStmt *stmt;

	deparsed_list = pg_parse_query(def);
	Assert(list_length(deparsed_list) == 1);
	deparsed_node = linitial(deparsed_list);

#if PG96
	stmt = (CreateTrigStmt *) deparsed_node;
#else
	do
	{
		RawStmt *rawstmt = (RawStmt *) deparsed_node;
		ParseState *pstate = make_parsestate(NULL);
		Query *query;

		Assert(IsA(deparsed_node, RawStmt));
		pstate->p_sourcetext = def;
		query = transformTopLevelStmt(pstate, rawstmt);
		free_parsestate(pstate);
		stmt = (CreateTrigStmt *) query->utilityStmt;
	} while (0);
#endif

	Assert(IsA(stmt, CreateTrigStmt));
	stmt->relation->relname = chunk_table_name;
	stmt->relation->schemaname = chunk_schema_name;

	CreateTriggerCompat(stmt, def, InvalidOid, InvalidOid, InvalidOid, InvalidOid, false);

	CommandCounterIncrement(); /* needed to prevent pg_class being updated
								* twice */
}

typedef bool (*trigger_handler)(Trigger *trigger, void *arg);

static inline void
for_each_trigger(Oid relid, trigger_handler on_trigger, void *arg)
{
	Relation rel;

	rel = table_open(relid, AccessShareLock);

	if (rel->trigdesc != NULL)
	{
		TriggerDesc *trigdesc = rel->trigdesc;
		int i;

		for (i = 0; i < trigdesc->numtriggers; i++)
		{
			Trigger *trigger = &trigdesc->triggers[i];

			if (!on_trigger(trigger, arg))
				break;
		}
	}

	table_close(rel, AccessShareLock);
}

static bool
create_trigger_handler(Trigger *trigger, void *arg)
{
	Chunk *chunk = arg;

#if PG10_GE
	if (TRIGGER_USES_TRANSITION_TABLE(trigger->tgnewtable) ||
		TRIGGER_USES_TRANSITION_TABLE(trigger->tgoldtable))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("hypertables do not support transition tables in triggers")));
#endif
	if (trigger_is_chunk_trigger(trigger))
		ts_trigger_create_on_chunk(trigger->tgoid,
								   NameStr(chunk->fd.schema_name),
								   NameStr(chunk->fd.table_name));

	return true;
}

/*
 * Create all hypertable triggers on a new chunk.
 *
 * Since chunk creation typically happens automatically on hypertable INSERT, we
 * need to execute the trigger creation under the role of the hypertable owner.
 * This is due to the use of CreateTrigger(), which does permissions checks. The
 * user role inserting might have INSERT permissions, but not TRIGGER
 * permissions (needed to create triggers on a table).
 *
 * We assume that the owner of the Hypertable is also the owner of the new
 * chunk.
 */
void
ts_trigger_create_all_on_chunk(Hypertable *ht, Chunk *chunk)
{
	int sec_ctx;
	Oid saved_uid;
	Oid owner = ts_rel_get_owner(ht->main_table_relid);

	GetUserIdAndSecContext(&saved_uid, &sec_ctx);

	if (saved_uid != owner)
		SetUserIdAndSecContext(owner, sec_ctx | SECURITY_LOCAL_USERID_CHANGE);

	for_each_trigger(ht->main_table_relid, create_trigger_handler, chunk);

	if (saved_uid != owner)
		SetUserIdAndSecContext(saved_uid, sec_ctx);
}

#if PG10_GE
static bool
check_for_transition_table(Trigger *trigger, void *arg)
{
	bool *found = arg;

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
ts_relation_has_transition_table_trigger(Oid relid)
{
	bool found = false;

#if PG10_GE
	for_each_trigger(relid, check_for_transition_table, &found);
#endif

	return found;
}
