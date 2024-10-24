/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/table.h>
#include <catalog/dependency.h>
#include <catalog/indexing.h>
#include <catalog/objectaddress.h>
#include <catalog/pg_am.h>
#include <catalog/pg_class.h>
#include <commands/defrem.h>
#include <nodes/makefuncs.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>

#include "extension_constants.h"
#include "utils.h"
#include <src/utils.h>

/*
 * Make a relation use hypercore without rewriting any data, simply by
 * updating the AM in pg_class. This only works if the relation is already
 * using (non-hypercore) compression.
 */
void
hypercore_set_am(const RangeVar *rv)
{
	HeapTuple tp;
	Oid relid = RangeVarGetRelid(rv, NoLock, false);

	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (HeapTupleIsValid(tp))
	{
		Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);
		Oid hypercore_amoid = get_table_am_oid(TS_HYPERCORE_TAM_NAME, false);
		Relation class_rel = table_open(RelationRelationId, RowExclusiveLock);

		elog(DEBUG1, "migrating table \"%s\" to hypercore", get_rel_name(relid));

		reltup->relam = hypercore_amoid;
		/* Set the new table access method */
		CatalogTupleUpdate(class_rel, &tp->t_self, tp);
		/* Also update pg_am dependency for the relation */
		ObjectAddress depender = {
			.classId = RelationRelationId,
			.objectId = relid,
		};
		ObjectAddress referenced = {
			.classId = AccessMethodRelationId,
			.objectId = hypercore_amoid,
		};

		recordDependencyOn(&depender, &referenced, DEPENDENCY_NORMAL);
		table_close(class_rel, RowExclusiveLock);
		ReleaseSysCache(tp);

		/*
		 * On compressed tables, indexes only contain non-compressed data, so
		 * need to rebuild indexes.
		 */
		ReindexParams params = {
			.options = 0,
			.tablespaceOid = InvalidOid,
		};

#if PG17_GE
		ReindexStmt stmt = { .kind = REINDEX_OBJECT_TABLE,
							 .relation = (RangeVar *) rv,
							 .params = NULL };
		reindex_relation(&stmt, relid, 0, &params);
#else
		reindex_relation(relid, 0, &params);
#endif
	}
}
