/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * This file contains a definition replacing PG function to get a list of FK constraints
 * using a struct according PG11 and PG12, which contains a data member missing in PG96 and
 * PG10.
 */

#include <postgres.h>
#include <access/genam.h>
#include <access/heapam.h>
#include <access/htup.h>
#include <access/htup_details.h>
#include <catalog/indexing.h>
#include <catalog/pg_constraint.h>
#include <utils/fmgroids.h>

#include "fkeylist.h"

List *
ts_relation_get_fk_list(Relation relation)
{
	List *result = NIL;
	Relation conrel;
	SysScanDesc conscan;
	ScanKeyData skey;
	HeapTuple htup;

	ScanKeyInit(&skey,
				Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(relation)));

	conrel = heap_open(ConstraintRelationId, AccessShareLock);
	conscan = systable_beginscan(conrel, ConstraintRelidTypidNameIndexId, true, NULL, 1, &skey);

	while (HeapTupleIsValid(htup = systable_getnext(conscan)))
	{
		Form_pg_constraint constraint = (Form_pg_constraint) GETSTRUCT(htup);
		ForeignKeyCacheInfoCompat *info;
		if (constraint->contype != CONSTRAINT_FOREIGN)
			continue;
		info = (ForeignKeyCacheInfoCompat *) newNode(sizeof(ForeignKeyCacheInfoCompat),
													 T_ForeignKeyCacheInfoCompat);
		info->conoid = HeapTupleGetOid(htup);
		result = lappend(result, info);
	}
	systable_endscan(conscan);
	heap_close(conrel, AccessShareLock);

	return result;
}
