/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/heapam.h>
#include <access/table.h>
#include <catalog/pg_class.h>
#include <utils/rel.h>
#include <utils/syscache.h>

#include "relstats.h"

void
relstats_fetch(Oid relid, RelStats *stats)
{
	Relation rd = table_open(RelationRelationId, AccessShareLock);
	HeapTuple ctup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));

	if (!HeapTupleIsValid(ctup))
		elog(ERROR, "pg_class entry for relid %u vanished when updating relstats", relid);

	Form_pg_class pgcform = (Form_pg_class) GETSTRUCT(ctup);
	stats->reltuples = pgcform->reltuples;
	stats->relpages = pgcform->relpages;
	stats->relallvisible = pgcform->relallvisible;
	ReleaseSysCache(ctup);

	table_close(rd, AccessShareLock);
}

void
relstats_update(Oid relid, const RelStats *stats)
{
	Relation rd = table_open(RelationRelationId, RowExclusiveLock);

	/* Fetch a copy of the tuple to scribble on */
	HeapTuple ctup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(ctup))
		elog(ERROR, "pg_class entry for relid %u vanished when updating relstats", relid);

	Form_pg_class pgcform = (Form_pg_class) GETSTRUCT(ctup);
	pgcform->reltuples = stats->reltuples;
	pgcform->relpages = stats->relpages;
	pgcform->relallvisible = stats->relallvisible;
	heap_inplace_update(rd, ctup);

	table_close(rd, RowExclusiveLock);
}
