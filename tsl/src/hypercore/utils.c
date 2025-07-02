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
#include <postgres_ext.h>
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
 * Set autovacuum reloption for chunks using hypercore TAM.
 *
 * This function turns autovacuum on or off for the (internal) compressed
 * chunk associated with the hypercore chunk by setting the
 * "autovacuum_enabled" option to "0" (false) or 1 (true). For Hypercore TAM,
 * vacuum is triggered via the TAM on the "main" chunk, which is then relayed
 * to the internal compressed chunk.
 *
 * Note that it is not necessary to clear this reloption when decompressing
 * since that deletes the compressed chunk.
 */
void
hypercore_set_compressed_autovacuum_reloption(Chunk *chunk, bool enabled)
{
	/*
	 * Update the tuple for the compressed chunk and disable autovacuum on
	 * it. This requires locking the relation (to prevent changes to the
	 * definition), but it is sufficient to take an access share lock.
	 */
	Chunk *cchunk = ts_chunk_get_by_id(chunk->fd.compressed_chunk_id, true);
	Relation compressed_rel = table_open(cchunk->table_id, AccessShareLock);

	/* We use makeInteger since makeBoolean does not exist prior to PG15 */
	List *options =
		list_make1(makeDefElem("autovacuum_enabled", (Node *) makeInteger(enabled ? 1 : 0), -1));
	ts_relation_set_reloption(compressed_rel, options, AccessShareLock);

	table_close(compressed_rel, AccessShareLock);
}

/*
 * Switch a relation's TAM between heap and hypercore (and vice versa) without
 * rewriting any data in the heap table. This is done by updating the AM in
 * pg_class. When switching to hypercore, this only works if the relation is
 * already using (non-hypercore) compression.
 *
 * Note that indexes need to be rebuilt, which can take some time depending on
 * the number of indexes.
 */
void
hypercore_set_am(const RangeVar *rv, const char *amname)
{
	HeapTuple tp;
	Oid relid = RangeVarGetRelid(rv, NoLock, false);
	Relation class_rel = table_open(RelationRelationId, RowExclusiveLock);
	bool to_hypercore = strcmp(amname, TS_HYPERCORE_TAM_NAME) == 0;

	Ensure(strcmp(amname, "heap") == 0 || to_hypercore,
		   "can only migrate between heap and %s",
		   TS_HYPERCORE_TAM_NAME);

	tp = SearchSysCacheLockedCopy1(RELOID, ObjectIdGetDatum(relid));

	if (HeapTupleIsValid(tp))
	{
		Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);
		Oid amoid = get_table_am_oid(amname, false);
#ifdef SYSCACHE_TUPLE_LOCK_NEEDED
		ItemPointerData otid = tp->t_self;
#endif
		elog(DEBUG1, "migrating table \"%s\" to %s", get_rel_name(relid), amname);

		reltup->relam = amoid;
		/* Set the new table access method */
		CatalogTupleUpdate(class_rel, &tp->t_self, tp);
		/* Also update pg_am dependency for the relation */

		if (to_hypercore)
		{
			ObjectAddress depender = {
				.classId = RelationRelationId,
				.objectId = relid,
			};
			ObjectAddress referenced = {
				.classId = AccessMethodRelationId,
				.objectId = amoid,
			};

			recordDependencyOn(&depender, &referenced, DEPENDENCY_NORMAL);
		}
		else
		{
			deleteDependencyRecordsForClass(RelationRelationId,
											relid,
											AccessMethodRelationId,
											DEPENDENCY_NORMAL);
		}

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
