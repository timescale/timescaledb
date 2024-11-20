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
#include <storage/lmgr.h>
#include <storage/lockdefs.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>

#include "compat/compat.h"
#include "chunk.h"
#include "extension_constants.h"
#include "src/utils.h"
#include "utils.h"

/*
 * Set reloptions for chunks using hypercore TAM.
 *
 * This sets all reloptions needed for chunks using the hypercore table access
 * method. Right now this means turning of autovacuum for the compressed chunk
 * associated with the hypercore chunk by setting the "autovacuum_enabled"
 * option to "0" (false).
 *
 * It is (currently) not necessary to clear this reloption anywhere since we
 * (currently) delete the compressed chunk when changing the table access
 * method back to "heap".
 */
void
hypercore_set_reloptions(Chunk *chunk)
{
	/*
	 * Update the tuple for the compressed chunk and disable autovacuum on
	 * it. This requires locking the relation (to prevent changes to the
	 * definition), but it is sufficient to take an access share lock.
	 */
	Chunk *cchunk = ts_chunk_get_by_id(chunk->fd.compressed_chunk_id, true);
	Relation compressed_rel = table_open(cchunk->table_id, AccessShareLock);

	/* We use makeInteger since makeBoolean does not exist prior to PG15 */
	List *options = list_make1(makeDefElem("autovacuum_enabled", (Node *) makeInteger(0), -1));
	ts_relation_set_reloption(compressed_rel, options, AccessShareLock);

	table_close(compressed_rel, AccessShareLock);
}

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
	Relation class_rel = table_open(RelationRelationId, RowExclusiveLock);

	tp = SearchSysCacheLockedCopy1(RELOID, ObjectIdGetDatum(relid));

	if (HeapTupleIsValid(tp))
	{
		Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);
		Oid hypercore_amoid = get_table_am_oid(TS_HYPERCORE_TAM_NAME, false);
#ifdef SYSCACHE_TUPLE_LOCK_NEEDED
		ItemPointerData otid = tp->t_self;
#endif
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
		UnlockSysCacheTuple(class_rel, &otid);

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

	table_close(class_rel, RowExclusiveLock);
}
