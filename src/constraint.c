/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/genam.h>
#include <access/table.h>
#include <catalog/index.h>
#include <catalog/indexing.h>
#include <catalog/pg_constraint.h>
#include <utils/builtins.h>
#include <utils/fmgroids.h>
#include <utils/rel.h>

#include "constraint.h"
#include "indexing.h"

/*
 * Process constraints that belong to a given relation.
 *
 * Returns the number of constraints processed.
 */
int
ts_constraint_process(Oid relid, constraint_func process_func, void *ctx)
{
	ScanKeyData skey;
	Relation rel;
	SysScanDesc scan;
	HeapTuple htup;
	bool should_continue = true;
	int count = 0;

	ScanKeyInit(&skey, Anum_pg_constraint_conrelid, BTEqualStrategyNumber, F_OIDEQ, relid);

	rel = table_open(ConstraintRelationId, AccessShareLock);
	scan = systable_beginscan(rel, ConstraintRelidTypidNameIndexId, true, NULL, 1, &skey);

	while (HeapTupleIsValid(htup = systable_getnext(scan)) && should_continue)
	{
		switch (process_func(htup, ctx))
		{
			case CONSTR_PROCESSED:
				count++;
				break;
			case CONSTR_PROCESSED_DONE:
				count++;
				should_continue = false;
				break;
			case CONSTR_IGNORED:
				break;
			case CONSTR_IGNORED_DONE:
				should_continue = false;
				break;
		}
	}

	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	return count;
}

/*
 * Search for an existing constraint in the chunk that matches the given
 * hypertable constraint. This is used to avoid creating duplicate constraints
 * when creating chunks.
 *
 * Returns true if a matching constraint is found, false otherwise.
 */
Form_pg_constraint
ts_constraint_find_matching(HeapTuple ht_tup, Relation chunk_rel)
{
	ScanKeyData skey;
	Relation constraint_rel;
	SysScanDesc scan;
	HeapTuple chunk_tup;
	Form_pg_constraint ht_con = (Form_pg_constraint) GETSTRUCT(ht_tup);
	Form_pg_constraint chunk_con;
	Relation ht_rel = RelationIdGetRelation(ht_con->conrelid);
	Form_pg_constraint result = NULL;

	constraint_rel = table_open(ConstraintRelationId, RowExclusiveLock);
	/* Search for a constraint matching this one */
	ScanKeyInit(&skey,
				Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(chunk_rel)));
	scan =
		systable_beginscan(constraint_rel, ConstraintRelidTypidNameIndexId, true, NULL, 1, &skey);

	while (HeapTupleIsValid(chunk_tup = systable_getnext(scan)))
	{
		chunk_con = (Form_pg_constraint) GETSTRUCT(chunk_tup);

		/* Check constraints are handled by CreateInheritance() */
		if (ht_con->contype != chunk_con->contype || ht_con->contype == CONSTRAINT_CHECK)
			continue;

		/*
		 * Get the main name for chunk constraint and check whether it matches
		 * the hypertable constraint name. If names match, then we compare indexes
		 * to ensure that the constraints are equivalent. If the names do not
		 * match, we assume that the constraints are not equivalent.
		 */
		if (ht_con->contype == CONSTRAINT_PRIMARY || ht_con->contype == CONSTRAINT_UNIQUE ||
			ht_con->contype == CONSTRAINT_EXCLUSION)
		{
			if (ts_indexing_compare(ht_con->conindid, chunk_con->conindid))
			{
				/*
				 * pfree'ing this form is up to the caller. It is not expected to
				 * consume a lot of memory, as only one form is allocated per
				 * constraint.
				 */
				result = (Form_pg_constraint) palloc(sizeof(FormData_pg_constraint));
				memcpy(result, chunk_con, sizeof(FormData_pg_constraint));
				break;
			}
		}
	}
	systable_endscan(scan);

	table_close(constraint_rel, RowExclusiveLock);
	RelationClose(ht_rel);

	return result;
}
