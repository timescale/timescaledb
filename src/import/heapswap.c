/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * This file contains source code that was copied and/or modified from
 * the PostgreSQL database, which is licensed under the open-source
 * PostgreSQL License. Please see the NOTICE at the top level
 * directory for a copy of the PostgreSQL License.
 */
#include <postgres.h>
#include <access/htup.h>
#include <access/multixact.h>
#include <access/relation.h>
#include <access/table.h>
#include <access/toast_internals.h>
#include <access/xact.h>
#include <catalog/catalog.h>
#include <catalog/dependency.h>
#include <catalog/heap.h>
#include <catalog/index.h>
#include <catalog/indexing.h>
#include <catalog/objectaccess.h>
#include <catalog/pg_am.h>
#include <catalog/pg_class.h>
#include <commands/defrem.h>
#include <commands/progress.h>
#include <commands/tablecmds.h>
#include <executor/spi.h>
#include <nodes/makefuncs.h>
#include <nodes/value.h>
#include <storage/lmgr.h>
#include <storage/lockdefs.h>
#include <utils/backend_progress.h>
#include <utils/inval.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/relcache.h>
#include <utils/relmapper.h>
#include <utils/snapmgr.h>
#include <utils/syscache.h>

#include "compat/compat.h"
#include "heapswap.h"

#if PG16_LT
typedef Oid RelFileNumber;
#define RelFileNumberIsValid OidIsValid
#define RelationMapOidToFilenumber RelationMapOidToFilenode
#define rd_newRelfilelocatorSubid rd_newRelfilenodeSubid
#define rd_firstRelfilelocatorSubid rd_firstRelfilenodeSubid
#define RelationAssumeNewRelfilelocator RelationAssumeNewRelfilenode
#endif

/**
 * The code in this file is imported from PostgreSQL and slightly modified to:
 *
 * 1. Make swap_relation_files() a public function.
 * 2. Optionally build indexes in finish_heap_swap().
 *
 * The above changes are needed to decouple index building from heap swaps, needed for reorder and
 * merge chunks.
 */

/*
 * Swap the physical files of two given relations.
 *
 * We swap the physical identity (reltablespace, relfilenumber) while keeping
 * the same logical identities of the two relations.  relpersistence is also
 * swapped, which is critical since it determines where buffers live for each
 * relation.
 *
 * We can swap associated TOAST data in either of two ways: recursively swap
 * the physical content of the toast tables (and their indexes), or swap the
 * TOAST links in the given relations' pg_class entries.  The former is needed
 * to manage rewrites of shared catalogs (where we cannot change the pg_class
 * links) while the latter is the only way to handle cases in which a toast
 * table is added or removed altogether.
 *
 * Additionally, the first relation is marked with relfrozenxid set to
 * frozenXid.  It seems a bit ugly to have this here, but the caller would
 * have to do it anyway, so having it here saves a heap_update.  Note: in
 * the swap-toast-links case, we assume we don't need to change the toast
 * table's relfrozenxid: the new version of the toast table should already
 * have relfrozenxid set to RecentXmin, which is good enough.
 *
 * Lastly, if r2 and its toast table and toast index (if any) are mapped,
 * their OIDs are emitted into mapped_tables[].  This is hacky but beats
 * having to look the information up again later in finish_heap_swap.
 */
void
ts_swap_relation_files(Oid r1, Oid r2, bool target_is_pg_class, bool swap_toast_by_content,
					   bool is_internal, TransactionId frozenXid, MultiXactId cutoffMulti,
					   Oid *mapped_tables)
{
	Relation relRelation;
	HeapTuple reltup1, reltup2;
	Form_pg_class relform1, relform2;
	RelFileNumber relfilenumber1, relfilenumber2;
	RelFileNumber swaptemp;

	char swptmpchr;
	Oid relam1, relam2;

	/* We need writable copies of both pg_class tuples. */
	relRelation = table_open(RelationRelationId, RowExclusiveLock);

	reltup1 = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(r1));
	if (!HeapTupleIsValid(reltup1))
		elog(ERROR, "cache lookup failed for relation %u", r1);
	relform1 = (Form_pg_class) GETSTRUCT(reltup1);

	reltup2 = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(r2));
	if (!HeapTupleIsValid(reltup2))
		elog(ERROR, "cache lookup failed for relation %u", r2);
	relform2 = (Form_pg_class) GETSTRUCT(reltup2);

	relfilenumber1 = relform1->relfilenode;
	relfilenumber2 = relform2->relfilenode;
	relam1 = relform1->relam;
	relam2 = relform2->relam;

	if (RelFileNumberIsValid(relfilenumber1) && RelFileNumberIsValid(relfilenumber2))
	{
		/*
		 * Normal non-mapped relations: swap relfilenumbers, reltablespaces,
		 * relpersistence
		 */
		Assert(!target_is_pg_class);

		swaptemp = relform1->relfilenode;
		relform1->relfilenode = relform2->relfilenode;
		relform2->relfilenode = swaptemp;

		swaptemp = relform1->reltablespace;
		relform1->reltablespace = relform2->reltablespace;
		relform2->reltablespace = swaptemp;

		swaptemp = relform1->relam;
		relform1->relam = relform2->relam;
		relform2->relam = swaptemp;

		swptmpchr = relform1->relpersistence;
		relform1->relpersistence = relform2->relpersistence;
		relform2->relpersistence = swptmpchr;

		/* Also swap toast links, if we're swapping by links */
		if (!swap_toast_by_content)
		{
			swaptemp = relform1->reltoastrelid;
			relform1->reltoastrelid = relform2->reltoastrelid;
			relform2->reltoastrelid = swaptemp;
		}
	}
	else
	{
		/*
		 * Mapped-relation case.  Here we have to swap the relation mappings
		 * instead of modifying the pg_class columns.  Both must be mapped.
		 */
		if (RelFileNumberIsValid(relfilenumber1) || RelFileNumberIsValid(relfilenumber2))
			elog(ERROR,
				 "cannot swap mapped relation \"%s\" with non-mapped relation",
				 NameStr(relform1->relname));

		/*
		 * We can't change the tablespace nor persistence of a mapped rel, and
		 * we can't handle toast link swapping for one either, because we must
		 * not apply any critical changes to its pg_class row.  These cases
		 * should be prevented by upstream permissions tests, so these checks
		 * are non-user-facing emergency backstop.
		 */
		if (relform1->reltablespace != relform2->reltablespace)
			elog(ERROR,
				 "cannot change tablespace of mapped relation \"%s\"",
				 NameStr(relform1->relname));
		if (relform1->relpersistence != relform2->relpersistence)
			elog(ERROR,
				 "cannot change persistence of mapped relation \"%s\"",
				 NameStr(relform1->relname));
		if (relform1->relam != relform2->relam)
			elog(ERROR,
				 "cannot change access method of mapped relation \"%s\"",
				 NameStr(relform1->relname));
		if (!swap_toast_by_content && (relform1->reltoastrelid || relform2->reltoastrelid))
			elog(ERROR,
				 "cannot swap toast by links for mapped relation \"%s\"",
				 NameStr(relform1->relname));

		/*
		 * Fetch the mappings --- shouldn't fail, but be paranoid
		 */
		relfilenumber1 = RelationMapOidToFilenumber(r1, relform1->relisshared);
		if (!RelFileNumberIsValid(relfilenumber1))
			elog(ERROR,
				 "could not find relation mapping for relation \"%s\", OID %u",
				 NameStr(relform1->relname),
				 r1);
		relfilenumber2 = RelationMapOidToFilenumber(r2, relform2->relisshared);
		if (!RelFileNumberIsValid(relfilenumber2))
			elog(ERROR,
				 "could not find relation mapping for relation \"%s\", OID %u",
				 NameStr(relform2->relname),
				 r2);

		/*
		 * Send replacement mappings to relmapper.  Note these won't actually
		 * take effect until CommandCounterIncrement.
		 */
		RelationMapUpdateMap(r1, relfilenumber2, relform1->relisshared, false);
		RelationMapUpdateMap(r2, relfilenumber1, relform2->relisshared, false);

		/* Pass OIDs of mapped r2 tables back to caller */
		*mapped_tables++ = r2;
	}

	/*
	 * Recognize that rel1's relfilenumber (swapped from rel2) is new in this
	 * subtransaction. The rel2 storage (swapped from rel1) may or may not be
	 * new.
	 */
	{
		Relation rel1, rel2;

		rel1 = relation_open(r1, NoLock);
		rel2 = relation_open(r2, NoLock);
		rel2->rd_createSubid = rel1->rd_createSubid;
		rel2->rd_newRelfilelocatorSubid = rel1->rd_newRelfilelocatorSubid;
		rel2->rd_firstRelfilelocatorSubid = rel1->rd_firstRelfilelocatorSubid;
		RelationAssumeNewRelfilelocator(rel1);
		relation_close(rel1, NoLock);
		relation_close(rel2, NoLock);
	}

	/*
	 * In the case of a shared catalog, these next few steps will only affect
	 * our own database's pg_class row; but that's okay, because they are all
	 * noncritical updates.  That's also an important fact for the case of a
	 * mapped catalog, because it's possible that we'll commit the map change
	 * and then fail to commit the pg_class update.
	 */

	/* set rel1's frozen Xid and minimum MultiXid */
	if (relform1->relkind != RELKIND_INDEX)
	{
		Assert(!TransactionIdIsValid(frozenXid) || TransactionIdIsNormal(frozenXid));
		relform1->relfrozenxid = frozenXid;
		relform1->relminmxid = cutoffMulti;
	}

	/* swap size statistics too, since new rel has freshly-updated stats */
	{
		int32 swap_pages;
		float4 swap_tuples;
		int32 swap_allvisible;

		swap_pages = relform1->relpages;
		relform1->relpages = relform2->relpages;
		relform2->relpages = swap_pages;

		swap_tuples = relform1->reltuples;
		relform1->reltuples = relform2->reltuples;
		relform2->reltuples = swap_tuples;

		swap_allvisible = relform1->relallvisible;
		relform1->relallvisible = relform2->relallvisible;
		relform2->relallvisible = swap_allvisible;
	}

	/*
	 * Update the tuples in pg_class --- unless the target relation of the
	 * swap is pg_class itself.  In that case, there is zero point in making
	 * changes because we'd be updating the old data that we're about to throw
	 * away.  Because the real work being done here for a mapped relation is
	 * just to change the relation map settings, it's all right to not update
	 * the pg_class rows in this case. The most important changes will instead
	 * performed later, in finish_heap_swap() itself.
	 */
	if (!target_is_pg_class)
	{
		CatalogIndexState indstate;

		indstate = CatalogOpenIndexes(relRelation);
		CatalogTupleUpdateWithInfo(relRelation, &reltup1->t_self, reltup1, indstate);
		CatalogTupleUpdateWithInfo(relRelation, &reltup2->t_self, reltup2, indstate);
		CatalogCloseIndexes(indstate);
	}
	else
	{
		/* no update ... but we do still need relcache inval */
		CacheInvalidateRelcacheByTuple(reltup1);
		CacheInvalidateRelcacheByTuple(reltup2);
	}

	/*
	 * Now that pg_class has been updated with its relevant information for
	 * the swap, update the dependency of the relations to point to their new
	 * table AM, if it has changed.
	 */
	if (relam1 != relam2)
	{
		if (changeDependencyFor(RelationRelationId, r1, AccessMethodRelationId, relam1, relam2) !=
			1)
			elog(ERROR,
				 "could not change access method dependency for relation \"%s.%s\"",
				 get_namespace_name(get_rel_namespace(r1)),
				 get_rel_name(r1));
		if (changeDependencyFor(RelationRelationId, r2, AccessMethodRelationId, relam2, relam1) !=
			1)
			elog(ERROR,
				 "could not change access method dependency for relation \"%s.%s\"",
				 get_namespace_name(get_rel_namespace(r2)),
				 get_rel_name(r2));
	}

	/*
	 * Post alter hook for modified relations. The change to r2 is always
	 * internal, but r1 depends on the invocation context.
	 */
	InvokeObjectPostAlterHookArg(RelationRelationId, r1, 0, InvalidOid, is_internal);
	InvokeObjectPostAlterHookArg(RelationRelationId, r2, 0, InvalidOid, true);

	/*
	 * If we have toast tables associated with the relations being swapped,
	 * deal with them too.
	 */
	if (relform1->reltoastrelid || relform2->reltoastrelid)
	{
		if (swap_toast_by_content)
		{
			if (relform1->reltoastrelid && relform2->reltoastrelid)
			{
				/* Recursively swap the contents of the toast tables */
				ts_swap_relation_files(relform1->reltoastrelid,
									   relform2->reltoastrelid,
									   target_is_pg_class,
									   swap_toast_by_content,
									   is_internal,
									   frozenXid,
									   cutoffMulti,
									   mapped_tables);
			}
			else
			{
				/* caller messed up */
				elog(ERROR, "cannot swap toast files by content when there's only one");
			}
		}
		else
		{
			/*
			 * We swapped the ownership links, so we need to change dependency
			 * data to match.
			 *
			 * NOTE: it is possible that only one table has a toast table.
			 *
			 * NOTE: at present, a TOAST table's only dependency is the one on
			 * its owning table.  If more are ever created, we'd need to use
			 * something more selective than deleteDependencyRecordsFor() to
			 * get rid of just the link we want.
			 */
			ObjectAddress baseobject, toastobject;
			long count;

			/*
			 * We disallow this case for system catalogs, to avoid the
			 * possibility that the catalog we're rebuilding is one of the
			 * ones the dependency changes would change.  It's too late to be
			 * making any data changes to the target catalog.
			 */
			if (IsSystemClass(r1, relform1))
				elog(ERROR, "cannot swap toast files by links for system catalogs");

			/* Delete old dependencies */
			if (relform1->reltoastrelid)
			{
				count =
					deleteDependencyRecordsFor(RelationRelationId, relform1->reltoastrelid, false);
				if (count != 1)
					elog(ERROR, "expected one dependency record for TOAST table, found %ld", count);
			}
			if (relform2->reltoastrelid)
			{
				count =
					deleteDependencyRecordsFor(RelationRelationId, relform2->reltoastrelid, false);
				if (count != 1)
					elog(ERROR, "expected one dependency record for TOAST table, found %ld", count);
			}

			/* Register new dependencies */
			baseobject.classId = RelationRelationId;
			baseobject.objectSubId = 0;
			toastobject.classId = RelationRelationId;
			toastobject.objectSubId = 0;

			if (relform1->reltoastrelid)
			{
				baseobject.objectId = r1;
				toastobject.objectId = relform1->reltoastrelid;
				recordDependencyOn(&toastobject, &baseobject, DEPENDENCY_INTERNAL);
			}

			if (relform2->reltoastrelid)
			{
				baseobject.objectId = r2;
				toastobject.objectId = relform2->reltoastrelid;
				recordDependencyOn(&toastobject, &baseobject, DEPENDENCY_INTERNAL);
			}
		}
	}

	/*
	 * If we're swapping two toast tables by content, do the same for their
	 * valid index. The swap can actually be safely done only if the relations
	 * have indexes.
	 */
	if (swap_toast_by_content && relform1->relkind == RELKIND_TOASTVALUE &&
		relform2->relkind == RELKIND_TOASTVALUE)
	{
		Oid toastIndex1, toastIndex2;

		/* Get valid index for each relation */
		toastIndex1 = toast_get_valid_index(r1, AccessExclusiveLock);
		toastIndex2 = toast_get_valid_index(r2, AccessExclusiveLock);

		ts_swap_relation_files(toastIndex1,
							   toastIndex2,
							   target_is_pg_class,
							   swap_toast_by_content,
							   is_internal,
							   InvalidTransactionId,
							   InvalidMultiXactId,
							   mapped_tables);
	}

	/* Clean up. */
	heap_freetuple(reltup1);
	heap_freetuple(reltup2);

	table_close(relRelation, RowExclusiveLock);

	/*
	 * Close both relcache entries' smgr links.  We need this kludge because
	 * both links will be invalidated during upcoming CommandCounterIncrement.
	 * Whichever of the rels is the second to be cleared will have a dangling
	 * reference to the other's smgr entry.  Rather than trying to avoid this
	 * by ordering operations just so, it's easiest to close the links first.
	 * (Fortunately, since one of the entries is local in our transaction,
	 * it's sufficient to clear out our own relcache this way; the problem
	 * cannot arise for other backends when they see our update on the
	 * non-transient relation.)
	 *
	 * Caution: the placement of this step interacts with the decision to
	 * handle toast rels by recursion.  When we are trying to rebuild pg_class
	 * itself, the smgr close on pg_class must happen after all accesses in
	 * this function.
	 */

#if PG17_LT
	/* Not needed as of 21d9c3ee4ef7 in the upstream */
	RelationCloseSmgrByOid(r1);
	RelationCloseSmgrByOid(r2);
#endif
}

/*
 * Remove the transient table that was built by make_new_heap, and finish
 * cleaning up (including rebuilding all indexes on the old heap).
 */
void
ts_finish_heap_swap(Oid OIDOldHeap, Oid OIDNewHeap, bool is_system_catalog,
					bool swap_toast_by_content, bool check_constraints, bool is_internal,
					bool reindex, TransactionId frozenXid, MultiXactId cutoffMulti,
					char newrelpersistence)
{
	ObjectAddress object;
	Oid mapped_tables[4];
	int i;

	/* Report that we are now swapping relation files */
	pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE, PROGRESS_CLUSTER_PHASE_SWAP_REL_FILES);

	/* Zero out possible results from swapped_relation_files */
	memset(mapped_tables, 0, sizeof(mapped_tables));

	/*
	 * Swap the contents of the heap relations (including any toast tables).
	 * Also set old heap's relfrozenxid to frozenXid.
	 */
	ts_swap_relation_files(OIDOldHeap,
						   OIDNewHeap,
						   (OIDOldHeap == RelationRelationId),
						   swap_toast_by_content,
						   is_internal,
						   frozenXid,
						   cutoffMulti,
						   mapped_tables);

	/*
	 * If it's a system catalog, queue a sinval message to flush all catcaches
	 * on the catalog when we reach CommandCounterIncrement.
	 */
	if (is_system_catalog)
		CacheInvalidateCatalog(OIDOldHeap);

	if (reindex)
	{
		int reindex_flags;
		ReindexParams reindex_params = { 0 };

		/*
		 * Rebuild each index on the relation (but not the toast table, which
		 * is all-new at this point).  It is important to do this before the
		 * DROP step because if we are processing a system catalog that will
		 * be used during DROP, we want to have its indexes available.  There
		 * is no advantage to the other order anyway because this is all
		 * transactional, so no chance to reclaim disk space before commit. We
		 * do not need a final CommandCounterIncrement() because
		 * reindex_relation does it.
		 *
		 * Note: because index_build is called via reindex_relation, it will
		 * never set indcheckxmin true for the indexes.  This is OK even
		 * though in some sense we are building new indexes rather than
		 * rebuilding existing ones, because the new heap won't contain any
		 * HOT chains at all, let alone broken ones, so it can't be necessary
		 * to set indcheckxmin.
		 */
		reindex_flags = REINDEX_REL_SUPPRESS_INDEX_USE;
		if (check_constraints)
			reindex_flags |= REINDEX_REL_CHECK_CONSTRAINTS;

		/*
		 * Ensure that the indexes have the same persistence as the parent
		 * relation.
		 */
		if (newrelpersistence == RELPERSISTENCE_UNLOGGED)
			reindex_flags |= REINDEX_REL_FORCE_INDEXES_UNLOGGED;
		else if (newrelpersistence == RELPERSISTENCE_PERMANENT)
			reindex_flags |= REINDEX_REL_FORCE_INDEXES_PERMANENT;

		/* Report that we are now reindexing relations */
		pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE, PROGRESS_CLUSTER_PHASE_REBUILD_INDEX);

#if PG17_LT
		reindex_relation(OIDOldHeap, reindex_flags, &reindex_params);
#else
		reindex_relation(NULL, OIDOldHeap, reindex_flags, &reindex_params);
#endif
	}
	else
	{
		/* Must make changes visible after swap. Normally reindex does it
		 * implicitly. */
		CommandCounterIncrement();
	}

	/* Report that we are now doing clean up */
	pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE, PROGRESS_CLUSTER_PHASE_FINAL_CLEANUP);

	/*
	 * If the relation being rebuilt is pg_class, swap_relation_files()
	 * couldn't update pg_class's own pg_class entry (check comments in
	 * swap_relation_files()), thus relfrozenxid was not updated. That's
	 * annoying because a potential reason for doing a VACUUM FULL is a
	 * imminent or actual anti-wraparound shutdown.  So, now that we can
	 * access the new relation using its indices, update relfrozenxid.
	 * pg_class doesn't have a toast relation, so we don't need to update the
	 * corresponding toast relation. Not that there's little point moving all
	 * relfrozenxid updates here since swap_relation_files() needs to write to
	 * pg_class for non-mapped relations anyway.
	 */
	if (OIDOldHeap == RelationRelationId)
	{
		Relation relRelation;
		HeapTuple reltup;
		Form_pg_class relform;

		relRelation = table_open(RelationRelationId, RowExclusiveLock);

		reltup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(OIDOldHeap));
		if (!HeapTupleIsValid(reltup))
			elog(ERROR, "cache lookup failed for relation %u", OIDOldHeap);
		relform = (Form_pg_class) GETSTRUCT(reltup);

		relform->relfrozenxid = frozenXid;
		relform->relminmxid = cutoffMulti;

		CatalogTupleUpdate(relRelation, &reltup->t_self, reltup);

		table_close(relRelation, RowExclusiveLock);
	}

	/* Destroy new heap with old filenumber */
	object.classId = RelationRelationId;
	object.objectId = OIDNewHeap;
	object.objectSubId = 0;

	/*
	 * The new relation is local to our transaction and we know nothing
	 * depends on it, so DROP_RESTRICT should be OK.
	 */
	performDeletion(&object, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);

	/* performDeletion does CommandCounterIncrement at end */

	/*
	 * Now we must remove any relation mapping entries that we set up for the
	 * transient table, as well as its toast table and toast index if any. If
	 * we fail to do this before commit, the relmapper will complain about new
	 * permanent map entries being added post-bootstrap.
	 */
	for (i = 0; OidIsValid(mapped_tables[i]); i++)
		RelationMapRemoveMapping(mapped_tables[i]);

	/*
	 * At this point, everything is kosher except that, if we did toast swap
	 * by links, the toast table's name corresponds to the transient table.
	 * The name is irrelevant to the backend because it's referenced by OID,
	 * but users looking at the catalogs could be confused.  Rename it to
	 * prevent this problem.
	 *
	 * Note no lock required on the relation, because we already hold an
	 * exclusive lock on it.
	 */
	if (!swap_toast_by_content)
	{
		Relation newrel;

		newrel = table_open(OIDOldHeap, NoLock);

		if (OidIsValid(newrel->rd_rel->reltoastrelid))
		{
			Oid toastidx;
			char NewToastName[NAMEDATALEN];

			/* Get the associated valid index to be renamed */
			toastidx = toast_get_valid_index(newrel->rd_rel->reltoastrelid, NoLock);

			/* rename the toast table ... */
			snprintf(NewToastName, NAMEDATALEN, "pg_toast_%u", OIDOldHeap);
			RenameRelationInternal(newrel->rd_rel->reltoastrelid, NewToastName, true, false);

			/* ... and its valid index too. */
			snprintf(NewToastName, NAMEDATALEN, "pg_toast_%u_index", OIDOldHeap);

			RenameRelationInternal(toastidx, NewToastName, true, true);

			/*
			 * Reset the relrewrite for the toast. The command-counter
			 * increment is required here as we are about to update the tuple
			 * that is updated as part of RenameRelationInternal.
			 */
			CommandCounterIncrement();
			ResetRelRewrite(newrel->rd_rel->reltoastrelid);
		}
		relation_close(newrel, NoLock);
	}

	/* if it's not a catalog table, clear any missing attribute settings */
	if (!is_system_catalog)
	{
		Relation newrel;

		newrel = table_open(OIDOldHeap, NoLock);
		RelationClearMissing(newrel);
		relation_close(newrel, NoLock);
	}
}
