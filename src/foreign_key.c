/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * The table referenced by a foreign constraint is supposed to have a
 * constraint that prevents removing the referenced rows. The constraint
 * is enforced by a pair of update and delete triggers. Normally this
 * is done by the postgres addFkRecurseReferenced(), but it doesn't work
 * for hypertables because they use inheritance, and that function only
 * recurses into declarative partitioning hierarchy.
 */

#include <postgres.h>
#include "access/attmap.h"
#include "catalog/pg_trigger.h"
#include "commands/trigger.h"
#include "parser/parser.h"

#include "compat/compat.h"
#include "chunk.h"
#include "foreign_key.h"
#include "hypertable.h"

static HeapTuple relation_get_fk_constraint(Oid conrelid, Oid confrelid);
static List *relation_get_referencing_fk(Oid reloid);
static Oid get_fk_index(Relation rel, int nkeys, AttrNumber *confkeys);
static void constraint_get_trigger(Oid conoid, Oid *updtrigoid, Oid *deltrigoid);
static char *ChooseForeignKeyConstraintNameAddition(int numkeys, AttrNumber *keys, Oid relid);
static void createForeignKeyActionTriggers(Form_pg_constraint fk, Oid relid, Oid refRelOid,
										   Oid constraintOid, Oid indexOid, Oid parentDelTrigger,
										   Oid parentUpdTrigger);
static void clone_constraint_on_chunk(const Chunk *chunk, Relation parentRel, Form_pg_constraint fk,
									  int numfks, AttrNumber *conkey, AttrNumber *confkey,
									  Oid *conpfeqop, Oid *conppeqop, Oid *conffeqop,
#if PG15_GE
									  int numfkdelsetcols, AttrNumber *confdelsetcols,
#endif
									  Oid parentDelTrigger, Oid parentUpdTrigger);

/*
 * Copy foreign key constraint fk_tuple to all chunks.
 */
static void
propagate_fk(Relation ht_rel, HeapTuple fk_tuple, List *chunks)
{
	Form_pg_constraint fk = (Form_pg_constraint) GETSTRUCT(fk_tuple);

	int numfks;
	AttrNumber conkey[INDEX_MAX_KEYS];
	AttrNumber confkey[INDEX_MAX_KEYS];
	Oid conpfeqop[INDEX_MAX_KEYS];
	Oid conppeqop[INDEX_MAX_KEYS];
	Oid conffeqop[INDEX_MAX_KEYS];
#if PG15_GE
	int numfkdelsetcols;
	AttrNumber confdelsetcols[INDEX_MAX_KEYS];
#endif

	DeconstructFkConstraintRow(fk_tuple,
							   &numfks,
							   conkey,
							   confkey,
							   conpfeqop,
							   conppeqop,
							   conffeqop
#if PG15_GE
							   ,
							   &numfkdelsetcols,
							   confdelsetcols
#endif
	);

	Oid parentDelTrigger, parentUpdTrigger;
	constraint_get_trigger(fk->oid, &parentUpdTrigger, &parentDelTrigger);

	ListCell *lc;
	foreach (lc, chunks)
	{
		Chunk *chunk = lfirst(lc);
		if (chunk->fd.osm_chunk)
			continue;

		clone_constraint_on_chunk(chunk,
								  ht_rel,
								  fk,
								  numfks,
								  conkey,
								  confkey,
								  conpfeqop,
								  conppeqop,
								  conffeqop,
#if PG15_GE
								  numfkdelsetcols,
								  confdelsetcols,
#endif
								  parentDelTrigger,
								  parentUpdTrigger);
	}
}

/*
 * Copy all foreign key constraints from the main table to a chunk.
 */
void
ts_chunk_copy_referencing_fk(const Hypertable *ht, const Chunk *chunk)
{
	ListCell *lc;
	List *chunks = list_make1((Chunk *) chunk);
	List *fks = relation_get_referencing_fk(ht->main_table_relid);

	Relation ht_rel = table_open(ht->main_table_relid, AccessShareLock);
	foreach (lc, fks)
	{
		HeapTuple fk_tuple = lfirst(lc);
		propagate_fk(ht_rel, fk_tuple, chunks);
	}
	table_close(ht_rel, NoLock);
}

/*
 * Copy one foreign key constraint from the main table to all chunks.
 */
void
ts_fk_propagate(Oid conrelid, Hypertable *ht)
{
	HeapTuple fk_tuple = relation_get_fk_constraint(conrelid, ht->main_table_relid);

	if (!fk_tuple)
		elog(ERROR, "foreign key constraint not found");

	Relation ht_rel = table_open(ht->main_table_relid, AccessShareLock);
	List *chunks = ts_chunk_get_by_hypertable_id(ht->fd.id);
	propagate_fk(ht_rel, fk_tuple, chunks);
	table_close(ht_rel, NoLock);
}

/*
 * Clone a single constraint to a single chunk.
 */
static void
clone_constraint_on_chunk(const Chunk *chunk, Relation parentRel, Form_pg_constraint fk, int numfks,
						  AttrNumber *conkey, AttrNumber *confkey, Oid *conpfeqop, Oid *conppeqop,
						  Oid *conffeqop,
#if PG15_GE
						  int numfkdelsetcols, AttrNumber *confdelsetcols,
#endif
						  Oid parentDelTrigger, Oid parentUpdTrigger)
{
	AttrNumber mapped_confkey[INDEX_MAX_KEYS];
	Relation pkrel = table_open(chunk->table_id, AccessShareLock);

	/* Map the foreign key columns on the hypertable side to the chunk columns */
#if PG16_GE
	AttrMap *attmap =
		build_attrmap_by_name(RelationGetDescr(pkrel), RelationGetDescr(parentRel), false);
#else
	AttrMap *attmap = build_attrmap_by_name(RelationGetDescr(pkrel), RelationGetDescr(parentRel));
#endif
	for (int i = 0; i < numfks; i++)
		mapped_confkey[i] = attmap->attnums[confkey[i] - 1];

	Oid indexoid = get_fk_index(pkrel, numfks, mapped_confkey);
	/* Since postgres accepted the constraint, there should be a supporting index. */
	Ensure(OidIsValid(indexoid), "index for constraint not found on chunk");

	table_close(pkrel, NoLock);

	char *conname_addition =
		ChooseForeignKeyConstraintNameAddition(numfks, confkey, parentRel->rd_id);
	char *conname = ChooseConstraintName(get_rel_name(fk->conrelid),
										 conname_addition,
										 "fkey",
										 fk->connamespace,
										 NIL);
	Oid conoid = CreateConstraintEntry(conname,
									   fk->connamespace,
									   CONSTRAINT_FOREIGN,
									   fk->condeferrable,
									   fk->condeferred,
									   fk->convalidated,
									   fk->oid,
									   fk->conrelid,
									   conkey,
									   numfks,
									   numfks,
									   InvalidOid,
									   indexoid,
									   chunk->table_id,
									   mapped_confkey,
									   conpfeqop,
									   conppeqop,
									   conffeqop,
									   numfks,
									   fk->confupdtype,
									   fk->confdeltype,
#if PG15_GE
									   confdelsetcols,
									   numfkdelsetcols,
#endif
									   fk->confmatchtype,
									   NULL,
									   NULL,
									   NULL,
									   false,
									   1,
									   false,
									   false);

	ObjectAddress address, referenced;
	ObjectAddressSet(address, ConstraintRelationId, conoid);
	ObjectAddressSet(referenced, ConstraintRelationId, fk->oid);
	recordDependencyOn(&address, &referenced, DEPENDENCY_INTERNAL);

	CommandCounterIncrement();

	createForeignKeyActionTriggers(fk,
								   fk->conrelid,
								   chunk->table_id,
								   conoid,
								   indexoid,
								   parentDelTrigger,
								   parentUpdTrigger);
}

/*
 * Generate the column-name portion of the constraint name for a new foreign
 * key given the list of column names that reference the referenced
 * table.  This will be passed to ChooseConstraintName along with the parent
 * table name and the "fkey" suffix.
 *
 * We know that less than NAMEDATALEN characters will actually be used, so we
 * can truncate the result once we've generated that many.
 *
 * This function is based on a static function by the same name in tablecmds.c in PostgreSQL.
 */
static char *
ChooseForeignKeyConstraintNameAddition(int numkeys, AttrNumber *keys, Oid relid)
{
	char buf[NAMEDATALEN * 2];
	int buflen = 0;

	buf[0] = '\0';
	for (int i = 0; i < numkeys; i++)
	{
		char *name = get_attname(relid, keys[i], false);
		if (buflen > 0)
			buf[buflen++] = '_'; /* insert _ between names */

		/*
		 * At this point we have buflen <= NAMEDATALEN.  name should be less
		 * than NAMEDATALEN already, but use strlcpy for paranoia.
		 */
		strlcpy(buf + buflen, name, NAMEDATALEN);
		buflen += strlen(buf + buflen);
		if (buflen >= NAMEDATALEN)
			break;
	}
	return pstrdup(buf);
}

/*
 * createForeignKeyActionTriggers
 *		Create the referenced-side "action" triggers that implement a foreign
 *		key.
 * This function is based on a static function by the same name in tablecmds.c in PostgreSQL.
 */
static void
createForeignKeyActionTriggers(Form_pg_constraint fk, Oid relid, Oid refRelOid, Oid constraintOid,
							   Oid indexOid, Oid parentDelTrigger, Oid parentUpdTrigger)
{
	CreateTrigStmt *fk_trigger;

	/*
	 * Build and execute a CREATE CONSTRAINT TRIGGER statement for the ON
	 * DELETE action on the referenced table.
	 */
	fk_trigger = makeNode(CreateTrigStmt);
	fk_trigger->replace = false;
	fk_trigger->isconstraint = true;
	fk_trigger->trigname = "RI_ConstraintTrigger_a";
	fk_trigger->relation = NULL;
	fk_trigger->args = NIL;
	fk_trigger->row = true;
	fk_trigger->timing = TRIGGER_TYPE_AFTER;
	fk_trigger->events = TRIGGER_TYPE_DELETE;
	fk_trigger->columns = NIL;
	fk_trigger->whenClause = NULL;
	fk_trigger->transitionRels = NIL;
	fk_trigger->constrrel = NULL;
	switch (fk->confdeltype)
	{
		case FKCONSTR_ACTION_NOACTION:
			fk_trigger->deferrable = fk->condeferrable;
			fk_trigger->initdeferred = fk->condeferred;
			fk_trigger->funcname = SystemFuncName("RI_FKey_noaction_del");
			break;
		case FKCONSTR_ACTION_RESTRICT:
			fk_trigger->deferrable = false;
			fk_trigger->initdeferred = false;
			fk_trigger->funcname = SystemFuncName("RI_FKey_restrict_del");
			break;
		case FKCONSTR_ACTION_CASCADE:
			fk_trigger->deferrable = false;
			fk_trigger->initdeferred = false;
			fk_trigger->funcname = SystemFuncName("RI_FKey_cascade_del");
			break;
		case FKCONSTR_ACTION_SETNULL:
			fk_trigger->deferrable = false;
			fk_trigger->initdeferred = false;
			fk_trigger->funcname = SystemFuncName("RI_FKey_setnull_del");
			break;
		case FKCONSTR_ACTION_SETDEFAULT:
			fk_trigger->deferrable = false;
			fk_trigger->initdeferred = false;
			fk_trigger->funcname = SystemFuncName("RI_FKey_setdefault_del");
			break;
		default:
			elog(ERROR, "unrecognized FK action type: %d", (int) fk->confdeltype);
			break;
	}

	/*
	 * clang will complain here about swapped arguments but this is intentional
	 * as this is the reverse trigger from the referenced table back to the
	 * referencing table. So we disable that specific warning for the next call.
	 *
	 * NOLINTBEGIN(readability-suspicious-call-argument)
	 */
	CreateTrigger(fk_trigger,
				  NULL,
				  refRelOid,
				  relid,
				  constraintOid,
				  indexOid,
				  InvalidOid,
				  parentDelTrigger,
				  NULL,
				  true,
				  false);
	/* NOLINTEND(readability-suspicious-call-argument) */

	/* Make changes-so-far visible */
	CommandCounterIncrement();

	/*
	 * Build and execute a CREATE CONSTRAINT TRIGGER statement for the ON
	 * UPDATE action on the referenced table.
	 */
	fk_trigger = makeNode(CreateTrigStmt);
	fk_trigger->replace = false;
	fk_trigger->isconstraint = true;
	fk_trigger->trigname = "RI_ConstraintTrigger_a";
	fk_trigger->relation = NULL;
	fk_trigger->args = NIL;
	fk_trigger->row = true;
	fk_trigger->timing = TRIGGER_TYPE_AFTER;
	fk_trigger->events = TRIGGER_TYPE_UPDATE;
	fk_trigger->columns = NIL;
	fk_trigger->whenClause = NULL;
	fk_trigger->transitionRels = NIL;
	fk_trigger->constrrel = NULL;
	switch (fk->confupdtype)
	{
		case FKCONSTR_ACTION_NOACTION:
			fk_trigger->deferrable = fk->condeferrable;
			fk_trigger->initdeferred = fk->condeferred;
			fk_trigger->funcname = SystemFuncName("RI_FKey_noaction_upd");
			break;
		case FKCONSTR_ACTION_RESTRICT:
			fk_trigger->deferrable = false;
			fk_trigger->initdeferred = false;
			fk_trigger->funcname = SystemFuncName("RI_FKey_restrict_upd");
			break;
		case FKCONSTR_ACTION_CASCADE:
			fk_trigger->deferrable = false;
			fk_trigger->initdeferred = false;
			fk_trigger->funcname = SystemFuncName("RI_FKey_cascade_upd");
			break;
		case FKCONSTR_ACTION_SETNULL:
			fk_trigger->deferrable = false;
			fk_trigger->initdeferred = false;
			fk_trigger->funcname = SystemFuncName("RI_FKey_setnull_upd");
			break;
		case FKCONSTR_ACTION_SETDEFAULT:
			fk_trigger->deferrable = false;
			fk_trigger->initdeferred = false;
			fk_trigger->funcname = SystemFuncName("RI_FKey_setdefault_upd");
			break;
		default:
			elog(ERROR, "unrecognized FK action type: %d", (int) fk->confupdtype);
			break;
	}

	/*
	 * clang will complain here about swapped arguments but this is intentional
	 * as this is the reverse trigger from the referenced table back to the
	 * referencing table.
	 *
	 * NOLINTBEGIN(readability-suspicious-call-argument)
	 */
	CreateTrigger(fk_trigger,
				  NULL,
				  refRelOid,
				  relid,
				  constraintOid,
				  indexOid,
				  InvalidOid,
				  parentUpdTrigger,
				  NULL,
				  true,
				  false);
	/* NOLINTEND(readability-suspicious-call-argument) */

	/* Make changes-so-far visible */
	CommandCounterIncrement();
}

/*
 * Return a list of foreign key pg_constraint heap tuples referencing reloid.
 */
static List *
relation_get_referencing_fk(Oid reloid)
{
	List *result = NIL;
	Relation conrel;
	SysScanDesc conscan;
	ScanKeyData skey[2];
	HeapTuple htup;

	/* Prepare to scan pg_constraint for entries having confrelid = this rel. */
	ScanKeyInit(&skey[0],
				Anum_pg_constraint_confrelid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(reloid));

	ScanKeyInit(&skey[1],
				Anum_pg_constraint_contype,
				BTEqualStrategyNumber,
				F_CHAREQ,
				CharGetDatum(CONSTRAINT_FOREIGN));

	conrel = table_open(ConstraintRelationId, AccessShareLock);
	conscan = systable_beginscan(conrel, InvalidOid, false, NULL, 2, skey);

	while (HeapTupleIsValid(htup = systable_getnext(conscan)))
	{
		result = lappend(result, heap_copytuple(htup));
	}

	systable_endscan(conscan);
	table_close(conrel, AccessShareLock);

	return result;
}

/*
 * Return a list of foreign key pg_constraint heap tuples referencing reloid.
 */
static HeapTuple
relation_get_fk_constraint(Oid conrelid, Oid confrelid)
{
	Relation conrel;
	SysScanDesc conscan;
	ScanKeyData skey[3];
	HeapTuple htup = NULL;

	/* Prepare to scan pg_constraint for entries having confrelid = this rel. */
	ScanKeyInit(&skey[0],
				Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(conrelid));

	ScanKeyInit(&skey[1],
				Anum_pg_constraint_confrelid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(confrelid));

	ScanKeyInit(&skey[2],
				Anum_pg_constraint_contype,
				BTEqualStrategyNumber,
				F_CHAREQ,
				CharGetDatum(CONSTRAINT_FOREIGN));

	conrel = table_open(ConstraintRelationId, AccessShareLock);
	conscan = systable_beginscan(conrel, InvalidOid, false, NULL, 3, skey);

	if (HeapTupleIsValid(htup = systable_getnext(conscan)))
	{
		htup = heap_copytuple(htup);
	}

	systable_endscan(conscan);
	table_close(conrel, AccessShareLock);

	return htup;
}

/* Get the UPDATE and DELETE trigger OIDs for the given constraint OID */
static void
constraint_get_trigger(Oid conoid, Oid *updtrigoid, Oid *deltrigoid)
{
	Relation rel;
	SysScanDesc scan;
	ScanKeyData skey[1];
	HeapTuple htup;

	*updtrigoid = InvalidOid;
	*deltrigoid = InvalidOid;

	ScanKeyInit(&skey[0],
				Anum_pg_trigger_tgconstraint,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(conoid));

	rel = table_open(TriggerRelationId, AccessShareLock);
	scan = systable_beginscan(rel, TriggerConstraintIndexId, true, NULL, 1, skey);

	while (HeapTupleIsValid(htup = systable_getnext(scan)))
	{
		Form_pg_trigger trigform = (Form_pg_trigger) GETSTRUCT(htup);

		if ((trigform->tgtype & TRIGGER_TYPE_UPDATE) == TRIGGER_TYPE_UPDATE)
			*updtrigoid = trigform->oid;
		if ((trigform->tgtype & TRIGGER_TYPE_DELETE) == TRIGGER_TYPE_DELETE)
			*deltrigoid = trigform->oid;
	}

	systable_endscan(scan);
	table_close(rel, AccessShareLock);
}

/*
 * Return the oid of the index supporting the foreign key constraint.
 */
static Oid
get_fk_index(Relation rel, int nkeys, AttrNumber *confkeys)
{
	Oid indexoid = InvalidOid;
	List *indexes = RelationGetIndexList(rel);
	ListCell *lc;

	foreach (lc, indexes)
	{
		Oid indexoid = lfirst_oid(lc);
		Relation indexrel = index_open(indexoid, AccessShareLock);

		if (!indexrel->rd_index->indisunique || indexrel->rd_index->indnkeyatts != nkeys)
		{
			index_close(indexrel, AccessShareLock);
			continue;
		}

		Bitmapset *con_keys = NULL;
		Bitmapset *ind_keys = NULL;

		for (int i = 0; i < nkeys; i++)
		{
			/*
			 * Since ordering of the constraint definition and index definition can differ,
			 * we need to check that all the columns in the constraint are present in the index
			 */
			con_keys = bms_add_member(con_keys, confkeys[i]);
			ind_keys = bms_add_member(ind_keys, indexrel->rd_index->indkey.values[i]);
		}

		bool match = bms_equal(con_keys, ind_keys);

		index_close(indexrel, AccessShareLock);
		bms_free(con_keys);
		bms_free(ind_keys);

		if (match)
		{
			return indexoid;
		}
	}

	return indexoid;
}
