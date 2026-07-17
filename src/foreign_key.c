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
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_trigger.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "parser/parser.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rls.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "compat/compat.h"
#include "chunk.h"
#include "export.h"
#include "extension_constants.h"
#include "foreign_key.h"
#include "hypertable.h"
#include "process_utility.h"
#include "ts_catalog/catalog.h"
#include "utils.h"

static HeapTuple relation_get_fk_constraint(Oid conrelid, Oid confrelid);
static List *relation_get_referencing_fk(Oid reloid);
static Oid get_fk_index(Relation rel, int nkeys, AttrNumber *confkeys);
static void constraint_get_trigger(Oid conoid, Oid *updtrigoid, Oid *deltrigoid);
static char *ChooseForeignKeyConstraintNameAddition(int numkeys, AttrNumber *keys, Oid relid);
static void drop_fk_constraint(HeapTuple constraint_tuple);
static void createForeignKeyActionTriggers(Form_pg_constraint fk, Oid relid, Oid refRelOid,
										   Oid constraintOid, Oid indexOid, Oid parentDelTrigger,
										   Oid parentUpdTrigger);
static void clone_constraint_on_chunk(const Chunk *chunk, Relation parentRel, Form_pg_constraint fk,
									  int numfks, AttrNumber *conkey, AttrNumber *confkey,
									  Oid *conpfeqop, Oid *conppeqop, Oid *conffeqop,
									  int numfkdelsetcols, AttrNumber *confdelsetcols,
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
	int numfkdelsetcols;
	AttrNumber confdelsetcols[INDEX_MAX_KEYS];

	DeconstructFkConstraintRow(fk_tuple,
							   &numfks,
							   conkey,
							   confkey,
							   conpfeqop,
							   conppeqop,
							   conffeqop,
							   &numfkdelsetcols,
							   confdelsetcols);

	Oid parentDelTrigger, parentUpdTrigger;
	constraint_get_trigger(fk->oid, &parentUpdTrigger, &parentDelTrigger);

	ListCell *lc;
	foreach (lc, chunks)
	{
		Chunk *chunk = lfirst(lc);
		if (chunk->fd.osm_chunk)
		{
			continue;
		}

		clone_constraint_on_chunk(chunk,
								  ht_rel,
								  fk,
								  numfks,
								  conkey,
								  confkey,
								  conpfeqop,
								  conppeqop,
								  conffeqop,
								  numfkdelsetcols,
								  confdelsetcols,
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
	{
		elog(ERROR, "foreign key constraint not found");
	}

	Relation ht_rel = table_open(ht->main_table_relid, AccessShareLock);
	List *chunks = ts_chunk_get_by_hypertable_id(ht->fd.id);
	propagate_fk(ht_rel, fk_tuple, chunks);
	table_close(ht_rel, NoLock);

#if PG19_GE
	/* PG19 probes the empty parent relation; route the check through our trigger instead. */
	ts_fk_swap_check_triggers(conrelid, ((Form_pg_constraint) GETSTRUCT(fk_tuple))->oid);
#endif
}

/*
 * Clone a single constraint to a single chunk.
 */
static void
clone_constraint_on_chunk(const Chunk *chunk, Relation parentRel, Form_pg_constraint fk, int numfks,
						  AttrNumber *conkey, AttrNumber *confkey, Oid *conpfeqop, Oid *conppeqop,
						  Oid *conffeqop, int numfkdelsetcols, AttrNumber *confdelsetcols,
						  Oid parentDelTrigger, Oid parentUpdTrigger)
{
	AttrNumber mapped_confkey[INDEX_MAX_KEYS];
	Relation pkrel = table_open(chunk->fd.relid, AccessShareLock);

	/* Map the foreign key columns on the hypertable side to the chunk columns */
	AttrMap *attmap =
		build_attrmap_by_name(RelationGetDescr(pkrel), RelationGetDescr(parentRel), false);
	for (int i = 0; i < numfks; i++)
	{
		mapped_confkey[i] = attmap->attnums[confkey[i] - 1];
	}

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
#if PG18_GE
									   true, /* isEnforced */
#endif
									   fk->convalidated,
									   fk->oid,
									   fk->conrelid,
									   conkey,
									   numfks,
									   numfks,
									   InvalidOid,
									   indexoid,
									   chunk->fd.relid,
									   mapped_confkey,
									   conpfeqop,
									   conppeqop,
									   conffeqop,
									   numfks,
									   fk->confupdtype,
									   fk->confdeltype,
									   confdelsetcols,
									   numfkdelsetcols,
									   fk->confmatchtype,
									   NULL,
									   NULL,
									   NULL,
									   false,
									   1,
									   false,
#if PG18_GE
									   false, /* conPeriod */
#endif
									   false);

	ObjectAddress address, referenced;
	ObjectAddressSet(address, ConstraintRelationId, conoid);
	ObjectAddressSet(referenced, ConstraintRelationId, fk->oid);
	recordDependencyOn(&address, &referenced, DEPENDENCY_INTERNAL);

	CommandCounterIncrement();

	createForeignKeyActionTriggers(fk,
								   fk->conrelid,
								   chunk->fd.relid,
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
		{
			buf[buflen++] = '_'; /* insert _ between names */
		}

		/*
		 * At this point we have buflen <= NAMEDATALEN.  name should be less
		 * than NAMEDATALEN already, but use strlcpy for paranoia.
		 */
		strlcpy(buf + buflen, name, NAMEDATALEN);
		buflen += strlen(buf + buflen);
		if (buflen >= NAMEDATALEN)
		{
			break;
		}
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

	HeapTuple htup = systable_getnext(conscan);
	if (HeapTupleIsValid(htup))
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
		{
			*updtrigoid = trigform->oid;
		}
		if ((trigform->tgtype & TRIGGER_TYPE_DELETE) == TRIGGER_TYPE_DELETE)
		{
			*deltrigoid = trigform->oid;
		}
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

/*
 * Drop a pg_constraint given its heap tuple.
 *
 * If the constraint inherits from a partitioned-parent constraint, break that
 * internal dependency first so performDeletion is allowed to remove it.
 */
static void
drop_fk_constraint(HeapTuple constraint_tuple)
{
	FormData_pg_constraint *constr = (FormData_pg_constraint *) GETSTRUCT(constraint_tuple);
	ObjectAddress constrobj = {
		.classId = ConstraintRelationId,
		.objectId = constr->oid,
	};

	if (OidIsValid(constr->conparentid))
	{
		deleteDependencyRecordsForClass(constrobj.classId,
										constrobj.objectId,
										ConstraintRelationId,
										DEPENDENCY_INTERNAL);
		CommandCounterIncrement();
	}

	if (OidIsValid(constrobj.objectId))
	{
		performDeletion(&constrobj, DROP_RESTRICT, 0);
	}
}

void
ts_chunk_drop_referencing_fk_by_chunk_id(Oid chunk_id)
{
	Chunk *chunk = ts_chunk_get_by_id(chunk_id, true);
	List *fks = relation_get_referencing_fk(chunk->fd.relid);
	ListCell *lc;

	foreach (lc, fks)
	{
		HeapTuple fk_tuple = lfirst(lc);
		drop_fk_constraint(fk_tuple);
	}
}

/*
 * Clone an outbound FK from the hypertable onto a chunk, keeping the parent's
 * name so the DROP/RENAME hooks can find it by name.
 */
void
ts_chunk_inherit_outbound_fk_by_oid(const Chunk *chunk, Oid parent_fk_oid)
{
	char *parent_name;
	Oid child_oid;

	if (chunk->relkind == RELKIND_FOREIGN_TABLE || IS_OSM_CHUNK(chunk))
	{
		return;
	}

	parent_name = get_constraint_name(parent_fk_oid);
	if (parent_name == NULL)
	{
		elog(ERROR, "cache lookup failed for constraint %u", parent_fk_oid);
	}

	child_oid = get_relation_constraint_oid(chunk->fd.relid, parent_name, true);
	if (OidIsValid(child_oid))
	{
		/* Only adopt the existing constraint if it's a FK to the same target. */
		HeapTuple parent_tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(parent_fk_oid));
		HeapTuple child_tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(child_oid));
		Oid parent_confrelid;
		Form_pg_constraint child_con;

		if (!HeapTupleIsValid(parent_tup) || !HeapTupleIsValid(child_tup))
		{
			elog(ERROR, "cache lookup failed for constraint");
		}

		parent_confrelid = ((Form_pg_constraint) GETSTRUCT(parent_tup))->confrelid;
		child_con = (Form_pg_constraint) GETSTRUCT(child_tup);

		if (child_con->contype != CONSTRAINT_FOREIGN || child_con->confrelid != parent_confrelid)
		{
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("cannot inherit foreign key \"%s\" onto chunk \"%s\"",
							parent_name,
							get_rel_name(chunk->fd.relid)),
					 errdetail("A constraint with that name already exists on the chunk "
							   "and does not match the hypertable's foreign key.")));
		}

		ReleaseSysCache(child_tup);
		ReleaseSysCache(parent_tup);
		return;
	}

	CatalogSecurityContext sec_ctx;

	/* Become DB owner so the ALTER TABLE in constraint_clone succeeds when
	 * the calling user doesn't own the chunk. */
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_process_utility_set_expect_chunk_modification(true);
	CatalogInternalCall2(DDL_CONSTRAINT_CLONE,
						 ObjectIdGetDatum(parent_fk_oid),
						 ObjectIdGetDatum(chunk->fd.relid));
	ts_process_utility_set_expect_chunk_modification(false);
	ts_catalog_restore_user(&sec_ctx);
	CommandCounterIncrement();
}

/*
 * Clone every outbound FK from the hypertable onto a chunk. Skips constraints
 * whose conparentid is set, since PG already propagates those.
 *
 * FK OIDs are snapshotted first: RelationGetFKeyList returns a relcache-owned
 * list that the clone path can invalidate.
 */
void
ts_chunk_inherit_outbound_fk(const Hypertable *ht, const Chunk *chunk)
{
	Relation ht_rel;
	List *fk_oids = NIL;
	ListCell *lc;

	if (chunk->relkind == RELKIND_FOREIGN_TABLE || IS_OSM_CHUNK(chunk))
	{
		return;
	}

	ht_rel = table_open(ht->main_table_relid, AccessShareLock);
	foreach (lc, RelationGetFKeyList(ht_rel))
	{
		Oid conoid = ((ForeignKeyCacheInfo *) lfirst(lc))->conoid;
		HeapTuple tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(conoid));
		bool has_parent;

		if (!HeapTupleIsValid(tup))
		{
			continue;
		}
		has_parent = OidIsValid(((Form_pg_constraint) GETSTRUCT(tup))->conparentid);
		ReleaseSysCache(tup);
		if (!has_parent)
		{
			fk_oids = lappend_oid(fk_oids, conoid);
		}
	}
	table_close(ht_rel, AccessShareLock);

	foreach (lc, fk_oids)
	{
		ts_chunk_inherit_outbound_fk_by_oid(chunk, lfirst_oid(lc));
	}
}

/*
 * Referenced-side check trigger for foreign keys pointing at a hypertable.
 *
 * PG19's fast path in RI_FKey_check probes the parent relation directly, which
 * for a hypertable is empty. We run the lookup as a query against the whole
 * hypertable instead, so it expands to the chunks.
 */
TS_FUNCTION_INFO_V1(ts_fk_referenced_check);

Datum
ts_fk_referenced_check(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;

	if (!CALLED_AS_TRIGGER(fcinfo))
	{
		elog(ERROR, "\"ts_fk_referenced_check\" was not called by trigger manager");
	}

	Relation fk_rel = trigdata->tg_relation;
	TupleTableSlot *newslot;

	if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
	{
		newslot = trigdata->tg_newslot;
	}
	else
	{
		newslot = trigdata->tg_trigslot;
	}

	/* Skip if the candidate row is no longer live. Mirrors RI_FKey_check. */
	if (!table_tuple_satisfies_snapshot(fk_rel, newslot, SnapshotSelf))
	{
		return PointerGetDatum(NULL);
	}

	Oid conoid = trigdata->tg_trigger->tgconstraint;
	HeapTuple contup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(conoid));
	if (!HeapTupleIsValid(contup))
	{
		elog(ERROR, "cache lookup failed for constraint %u", conoid);
	}

	Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(contup);
	Oid pk_relid = con->confrelid;
	char confmatchtype = con->confmatchtype;
	NameData conname = con->conname;

	int numfks;
	AttrNumber conkey[INDEX_MAX_KEYS];
	AttrNumber confkey[INDEX_MAX_KEYS];
	Oid conpfeqop[INDEX_MAX_KEYS];
	Oid conppeqop[INDEX_MAX_KEYS];
	Oid conffeqop[INDEX_MAX_KEYS];
	int numfkdelsetcols;
	AttrNumber confdelsetcols[INDEX_MAX_KEYS];
	DeconstructFkConstraintRow(contup,
							   &numfks,
							   conkey,
							   confkey,
							   conpfeqop,
							   conppeqop,
							   conffeqop,
							   &numfkdelsetcols,
							   confdelsetcols);
	ReleaseSysCache(contup);

	/* Extract the foreign key values from the new row. */
	Datum values[INDEX_MAX_KEYS];
	char nulls[INDEX_MAX_KEYS];
	Oid argtypes[INDEX_MAX_KEYS];
	TupleDesc fkdesc = RelationGetDescr(fk_rel);
	int nnull = 0;

	for (int i = 0; i < numfks; i++)
	{
		bool isnull;
		values[i] = slot_getattr(newslot, conkey[i], &isnull);
		nulls[i] = isnull ? 'n' : ' ';
		argtypes[i] = TupleDescAttr(fkdesc, conkey[i] - 1)->atttypid;
		if (isnull)
		{
			nnull++;
		}
	}

	/* An all-NULL key satisfies every match type. */
	if (nnull == numfks)
	{
		return PointerGetDatum(NULL);
	}

	if (nnull > 0)
	{
		if (confmatchtype == FKCONSTR_MATCH_FULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
					 errmsg("insert or update on table \"%s\" violates foreign key "
							"constraint \"%s\"",
							RelationGetRelationName(fk_rel),
							NameStr(conname)),
					 errdetail("MATCH FULL does not allow mixing of null and nonnull key "
							   "values."),
					 errtableconstraint(fk_rel, NameStr(conname))));
		}

		/* MATCH SIMPLE: a partially NULL key passes. */
		return PointerGetDatum(NULL);
	}

	/*
	 * Build the lookup query in the same shape RI_FKey_check emits, so our
	 * planner hook (preprocess_fk_checks) recognizes it and expands the
	 * hypertable to its chunks.
	 */
	StringInfoData querybuf;
	initStringInfo(&querybuf);
	appendStringInfo(&querybuf,
					 "SELECT 1 FROM ONLY %s x",
					 quote_qualified_identifier(get_namespace_name(get_rel_namespace(pk_relid)),
												get_rel_name(pk_relid)));

	const char *querysep = "WHERE";
	for (int i = 0; i < numfks; i++)
	{
		char *pkattname = get_attname(pk_relid, confkey[i], false);
		HeapTuple optup = SearchSysCache1(OPEROID, ObjectIdGetDatum(conpfeqop[i]));
		if (!HeapTupleIsValid(optup))
		{
			elog(ERROR, "cache lookup failed for operator %u", conpfeqop[i]);
		}
		Form_pg_operator opform = (Form_pg_operator) GETSTRUCT(optup);
		char *opnsp = get_namespace_name(opform->oprnamespace);

		appendStringInfo(&querybuf,
						 " %s %s OPERATOR(%s.%s) $%d",
						 querysep,
						 quote_identifier(pkattname),
						 quote_identifier(opnsp),
						 NameStr(opform->oprname),
						 i + 1);
		ReleaseSysCache(optup);
		querysep = "AND";
	}

	/*
	 * Lock the referenced rows with FOR KEY SHARE. preprocess_fk_checks drops
	 * this row mark when the referenced hypertable is compressed, since
	 * compressed tuples cannot be locked.
	 */
	appendStringInfoString(&querybuf, " FOR KEY SHARE OF x");

	/*
	 * Run the lookup as the referenced table's owner with RLS disabled, so it
	 * ignores the caller's permissions and row security. Mirrors ri_PerformCheck.
	 */
	Oid save_userid;
	int save_sec_context;
	Oid pkowner;
	HeapTuple reltup = SearchSysCache1(RELOID, ObjectIdGetDatum(pk_relid));
	if (!HeapTupleIsValid(reltup))
	{
		elog(ERROR, "cache lookup failed for relation %u", pk_relid);
	}
	pkowner = ((Form_pg_class) GETSTRUCT(reltup))->relowner;
	ReleaseSysCache(reltup);

	if (SPI_connect() != SPI_OK_CONNECT)
	{
		elog(ERROR, "SPI_connect failed");
	}

	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(pkowner,
						   save_sec_context | SECURITY_LOCAL_USERID_CHANGE | SECURITY_NOFORCE_RLS);

	int ret = SPI_execute_with_args(querybuf.data,
									numfks,
									argtypes,
									values,
									nulls,
									false /* read_only */,
									1 /* count */);

	SetUserIdAndSecContext(save_userid, save_sec_context);

	if (ret != SPI_OK_SELECT)
	{
		elog(ERROR, "foreign key check query failed: %s", querybuf.data);
	}

	bool found = (SPI_processed > 0);

	if (SPI_finish() != SPI_OK_FINISH)
	{
		elog(ERROR, "SPI_finish failed");
	}

	if (!found)
	{
		/*
		 * Report the violation like ri_ReportViolation does. Key details are
		 * only shown when the caller may read the key columns.
		 */
		bool has_perm = true;

		if (check_enable_rls(fk_rel->rd_id, InvalidOid, true) == RLS_ENABLED)
		{
			has_perm = false;
		}
		else if (pg_class_aclcheck(fk_rel->rd_id, GetUserId(), ACL_SELECT) != ACLCHECK_OK)
		{
			for (int i = 0; i < numfks; i++)
			{
				if (pg_attribute_aclcheck(fk_rel->rd_id, conkey[i], GetUserId(), ACL_SELECT) !=
					ACLCHECK_OK)
				{
					has_perm = false;
					break;
				}
			}
		}

		if (has_perm)
		{
			StringInfoData key_names;
			StringInfoData key_values;
			initStringInfo(&key_names);
			initStringInfo(&key_values);

			for (int i = 0; i < numfks; i++)
			{
				Form_pg_attribute att = TupleDescAttr(fkdesc, conkey[i] - 1);
				char *val;

				if (nulls[i] == 'n')
				{
					val = "null";
				}
				else
				{
					Oid foutoid;
					bool typisvarlena;
					getTypeOutputInfo(att->atttypid, &foutoid, &typisvarlena);
					val = OidOutputFunctionCall(foutoid, values[i]);
				}

				if (i > 0)
				{
					appendStringInfoString(&key_names, ", ");
					appendStringInfoString(&key_values, ", ");
				}
				appendStringInfoString(&key_names, NameStr(att->attname));
				appendStringInfoString(&key_values, val);
			}

			ereport(ERROR,
					(errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
					 errmsg("insert or update on table \"%s\" violates foreign key constraint "
							"\"%s\"",
							RelationGetRelationName(fk_rel),
							NameStr(conname)),
					 errdetail("Key (%s)=(%s) is not present in table \"%s\".",
							   key_names.data,
							   key_values.data,
							   get_rel_name(pk_relid)),
					 errtableconstraint(fk_rel, NameStr(conname))));
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
					 errmsg("insert or update on table \"%s\" violates foreign key constraint "
							"\"%s\"",
							RelationGetRelationName(fk_rel),
							NameStr(conname)),
					 errdetail("Key is not present in table \"%s\".", get_rel_name(pk_relid)),
					 errtableconstraint(fk_rel, NameStr(conname))));
		}
	}

	return PointerGetDatum(NULL);
}

/*
 * Point a foreign key's INSERT/UPDATE check triggers at ts_fk_referenced_check.
 * The referenced-side action triggers are left untouched.
 */
void
ts_fk_swap_check_triggers(Oid conrelid, Oid conoid)
{
	HeapTuple contup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(conoid));
	if (!HeapTupleIsValid(contup))
	{
		elog(ERROR, "cache lookup failed for constraint %u", conoid);
	}
	Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(contup);
	Oid confrelid = con->confrelid;
#if PG18_GE
	bool is_temporal = con->conperiod;
#else
	bool is_temporal = false;
#endif
	ReleaseSysCache(contup);

	/*
	 * Leave the builtin triggers in place for cases the fast path does not
	 * cover: temporal foreign keys (range containment, not implemented here)
	 * and partitioned referenced tables (PG keeps the check on its own path).
	 */
	if (is_temporal || get_rel_relkind(confrelid) == RELKIND_PARTITIONED_TABLE)
	{
		return;
	}

	Oid our_func = ts_get_function_oid("fk_referenced_check", FUNCTIONS_SCHEMA_NAME, 0, NULL);

	Relation tgrel = table_open(TriggerRelationId, RowExclusiveLock);
	ScanKeyData skey[1];
	ScanKeyInit(&skey[0],
				Anum_pg_trigger_tgconstraint,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(conoid));

	SysScanDesc scan = systable_beginscan(tgrel, TriggerConstraintIndexId, true, NULL, 1, skey);
	HeapTuple htup;
	bool changed = false;
	while (HeapTupleIsValid(htup = systable_getnext(scan)))
	{
		Form_pg_trigger trig = (Form_pg_trigger) GETSTRUCT(htup);

		/* Only the check triggers on the referencing table. */
		if (trig->tgrelid != conrelid)
		{
			continue;
		}
		if (trig->tgfoid != F_RI_FKEY_CHECK_INS && trig->tgfoid != F_RI_FKEY_CHECK_UPD)
		{
			continue;
		}

		HeapTuple newtup = heap_copytuple(htup);
		((Form_pg_trigger) GETSTRUCT(newtup))->tgfoid = our_func;
		CatalogTupleUpdate(tgrel, &newtup->t_self, newtup);
		heap_freetuple(newtup);
		changed = true;
	}

	systable_endscan(scan);
	table_close(tgrel, RowExclusiveLock);

	if (changed)
	{
		CacheInvalidateRelcacheByRelid(conrelid);
	}

	CommandCounterIncrement();
}

/*
 * Swap the check triggers of every foreign key referencing reloid. Used when a
 * plain table with inbound foreign keys becomes a hypertable.
 */
void
ts_fk_swap_referencing_check_triggers(Oid reloid)
{
	ListCell *lc;
	List *fks = relation_get_referencing_fk(reloid);

	foreach (lc, fks)
	{
		Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT((HeapTuple) lfirst(lc));
		ts_fk_swap_check_triggers(con->conrelid, con->oid);
	}
}

/*
 * Re-point the check triggers of every foreign key referencing a hypertable.
 * Restore recreates the constraints with the builtin triggers, so this runs
 * from timescaledb_post_restore() to reinstate the swap. No-op before PG19.
 */
TS_FUNCTION_INFO_V1(ts_fk_restore_check_triggers);

Datum
ts_fk_restore_check_triggers(PG_FUNCTION_ARGS)
{
#if PG19_GE
	List *conrelids = NIL;
	List *conoids = NIL;
	ListCell *lc1, *lc2;

	Relation conrel = table_open(ConstraintRelationId, AccessShareLock);
	ScanKeyData skey[1];
	ScanKeyInit(&skey[0],
				Anum_pg_constraint_contype,
				BTEqualStrategyNumber,
				F_CHAREQ,
				CharGetDatum(CONSTRAINT_FOREIGN));

	SysScanDesc scan = systable_beginscan(conrel, InvalidOid, false, NULL, 1, skey);
	HeapTuple tup;
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(tup);

		if (OidIsValid(con->confrelid) && ts_is_hypertable(con->confrelid))
		{
			conrelids = lappend_oid(conrelids, con->conrelid);
			conoids = lappend_oid(conoids, con->oid);
		}
	}
	systable_endscan(scan);
	table_close(conrel, AccessShareLock);

	/* Swap after the scan so we don't modify catalogs mid-scan. */
	forboth (lc1, conrelids, lc2, conoids)
	{
		ts_fk_swap_check_triggers(lfirst_oid(lc1), lfirst_oid(lc2));
	}
#endif

	PG_RETURN_VOID();
}
