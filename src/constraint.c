/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/genam.h>
#include <access/table.h>
#include <catalog/indexing.h>
#include <catalog/pg_constraint.h>
#include <utils/fmgroids.h>

#include "constraint.h"

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
