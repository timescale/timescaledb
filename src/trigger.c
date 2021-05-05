/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/xact.h>
#include <commands/trigger.h>
#include <miscadmin.h>
#include <parser/analyze.h>
#include <tcop/tcopprot.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/syscache.h>

#include "trigger.h"

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
ts_trigger_create_on_chunk(Oid trigger_oid, const char *chunk_schema_name,
						   const char *chunk_table_name)
{
	Datum datum_def = DirectFunctionCall1(pg_get_triggerdef, ObjectIdGetDatum(trigger_oid));
	const char *def = TextDatumGetCString(datum_def);
	List *deparsed_list;
	Node *deparsed_node;
	CreateTrigStmt *stmt;

	deparsed_list = pg_parse_query(def);
	Assert(list_length(deparsed_list) == 1);
	deparsed_node = linitial(deparsed_list);

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

	Assert(IsA(stmt, CreateTrigStmt));
	stmt->relation->relname = (char *) chunk_table_name;
	stmt->relation->schemaname = (char *) chunk_schema_name;

	CreateTrigger(stmt,
				  def,
				  InvalidOid,
				  InvalidOid,
				  InvalidOid,
				  InvalidOid,
				  InvalidOid,
				  InvalidOid,
				  NULL,
				  false,
				  false);

	CommandCounterIncrement(); /* needed to prevent pg_class being updated
								* twice */
}

typedef bool (*trigger_handler)(const Trigger *trigger, void *arg);

static inline void
for_each_trigger(Oid relid, trigger_handler on_trigger, void *arg)
{
	Relation rel;

	rel = table_open(relid, AccessShareLock);

	if (rel->trigdesc != NULL)
	{
		int i;

		/*
		 * The TriggerDesc from rel->trigdesc seems to be modified during
		 * iterations of the loop and sometimes gets reallocated so we
		 * access trigdesc only through rel->trigdesc.
		 */
		for (i = 0; i < rel->trigdesc->numtriggers; i++)
		{
			Trigger *trigger = &rel->trigdesc->triggers[i];

			if (!on_trigger(trigger, arg))
				break;
		}
	}

	table_close(rel, AccessShareLock);
}

static bool
create_trigger_handler(const Trigger *trigger, void *arg)
{
	const Chunk *chunk = arg;

	if (TRIGGER_USES_TRANSITION_TABLE(trigger->tgnewtable) ||
		TRIGGER_USES_TRANSITION_TABLE(trigger->tgoldtable))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("hypertables do not support transition tables in triggers")));

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
ts_trigger_create_all_on_chunk(const Chunk *chunk)
{
	int sec_ctx;
	Oid saved_uid;
	Oid owner;

	/* We do not create triggers on foreign table chunks */
	if (chunk->relkind == RELKIND_FOREIGN_TABLE)
		return;

	Assert(chunk->relkind == RELKIND_RELATION);
	owner = ts_rel_get_owner(chunk->hypertable_relid);

	GetUserIdAndSecContext(&saved_uid, &sec_ctx);

	if (saved_uid != owner)
		SetUserIdAndSecContext(owner, sec_ctx | SECURITY_LOCAL_USERID_CHANGE);

	for_each_trigger(chunk->hypertable_relid, create_trigger_handler, (Chunk *) chunk);

	if (saved_uid != owner)
		SetUserIdAndSecContext(saved_uid, sec_ctx);
}

static bool
check_for_transition_table(const Trigger *trigger, void *arg)
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

bool
ts_relation_has_transition_table_trigger(Oid relid)
{
	bool found = false;

	for_each_trigger(relid, check_for_transition_table, &found);

	return found;
}
