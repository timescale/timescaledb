/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/attnum.h>
#include <access/htup.h>
#include <access/htup_details.h>
#include <access/stratnum.h>
#include <access/tableam.h>
#include <catalog/dependency.h>
#include <catalog/objectaddress.h>
#include <catalog/pg_class_d.h>
#include <executor/tuptable.h>
#include <miscadmin.h>
#include <nodes/lockoptions.h>
#include <nodes/parsenodes.h>
#include <storage/itemptr.h>
#include <storage/lmgr.h>
#include <storage/lockdefs.h>
#include <utils/acl.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>

#include "chunk_rewrite.h"
#include "scan_iterator.h"
#include "ts_catalog/catalog.h"

/*
 * chunk_rewrite:
 *
 * This catalog table tracks pending rewrite operations for chunks. It is used when merging chunks
 * in concurrent mode to track temporarily written relations/heaps that might orphaned in case
 * of a failed rewrite. For example, a multi-transactional chunk merge could fail in the second
 * transaction and leave behind orphaned rewritten heaps it created in the first transaction.
 *
 * Each entry in the catalog is a mapping from a current relation to its new heap. For merges, this
 * is a many-to-one relation for each merge.
 *
 * Future operations, like concurrent split, might also use this catalog table.
 */

static HeapTuple
chunk_rewrite_make_tuple(Oid chunk_relid, Oid new_relid, TupleDesc desc)
{
	Datum values[Natts_chunk_rewrite];
	bool nulls[Natts_chunk_rewrite] = { false };

	memset(values, 0, sizeof(Datum) * Natts_chunk_rewrite);

	values[AttrNumberGetAttrOffset(Anum_chunk_rewrite_chunk_relid)] = ObjectIdGetDatum(chunk_relid);
	values[AttrNumberGetAttrOffset(Anum_chunk_rewrite_new_relid)] = ObjectIdGetDatum(new_relid);

	return heap_form_tuple(desc, values, nulls);
}

/*
 * Add an entry to the chunk_rewrite table.
 */
void
ts_chunk_rewrite_add(Oid chunk_relid, Oid new_relid)
{
	Catalog *catalog = ts_catalog_get();
	Oid cat_relid = catalog_get_table_id(catalog, CHUNK_REWRITE);
	HeapTuple new_tuple;
	CatalogSecurityContext sec_ctx;
	Relation catrel;

	catrel = table_open(cat_relid, RowExclusiveLock);
	new_tuple = chunk_rewrite_make_tuple(chunk_relid, new_relid, catrel->rd_att);
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_only(catrel, new_tuple);
	ts_catalog_restore_user(&sec_ctx);
	heap_freetuple(new_tuple);
	table_close(catrel, NoLock);
}

/*
 * Look up an entry based on the original chunk_id. The entry is locked FOR UPDATE.
 */
bool
ts_chunk_rewrite_get_with_lock(Oid chunk_relid, Form_chunk_rewrite form, ItemPointer tid)
{
	Catalog *catalog = ts_catalog_get();
	ScanIterator it;
	ScanTupLock tuplock = {
		.waitpolicy = LockWaitBlock,
		.lockmode = LockTupleExclusive,
	};
	bool found = false;

	it = ts_scan_iterator_create(CHUNK_REWRITE, RowShareLock, CurrentMemoryContext);
	it.ctx.tuplock = &tuplock;
	it.ctx.flags = SCANNER_F_KEEPLOCK;
	it.ctx.index = catalog_get_index(catalog, CHUNK_REWRITE, CHUNK_REWRITE_IDX);
	ts_scan_iterator_scan_key_init(&it,
								   Anum_chunk_rewrite_key_chunk_relid,
								   BTEqualStrategyNumber,
								   F_OIDEQ,
								   ObjectIdGetDatum(chunk_relid));

	ts_scanner_foreach(&it)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&it);

		switch (ti->lockresult)
		{
			case TM_Ok:
				found = true;

				if (tid)
					ItemPointerCopy(&ti->slot->tts_tid, tid);

				if (form)
				{
					bool should_free;
					HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
					memcpy(form, GETSTRUCT(tuple), sizeof(FormData_chunk_rewrite));

					if (should_free)
						heap_freetuple(tuple);
				}
				break;
			case TM_Deleted:
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("chunk merge state deleted by concurrent transaction")));
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("unable to lock chunk rewrite catalog tuple, lock result is %d for "
								"chunk (%u)",
								ti->lockresult,
								chunk_relid)));
				break;
		}
	}

	ts_scan_iterator_close(&it);

	return found;
}

/*
 * Delete an entry from the chunk_rewrite table based on TID.
 */
void
ts_chunk_rewrite_delete_by_tid(const ItemPointer tid)
{
	Catalog *catalog = ts_catalog_get();
	Oid cat_relid = catalog_get_table_id(catalog, CHUNK_REWRITE);
	CatalogSecurityContext sec_ctx;
	Relation catrel;

	catrel = table_open(cat_relid, RowExclusiveLock);
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_delete_tid_only(catrel, tid);
	ts_catalog_restore_user(&sec_ctx);
	table_close(catrel, NoLock);
}

/*
 * Delete an entry from the chunk_rewrite table and drop the orphaned heap (if it exists).
 *
 * The delete result indicates whether both the entry and the orphaned heap was dropped,
 * or if only the entry was deleted (in case the heap was already dropped), or a failure occurred.
 *
 * If the "conditional" parameter is specified, the entry will only be deleted if the referenced
 * heap relation can be immediately locked without waiting. This is useful in order to skip entries
 * that are locked by ongoing merges.
 */
ChunkRewriteDeleteResult
ts_chunk_rewrite_delete(Oid chunk_relid, bool conditional)
{
	ItemPointerData tid;
	FormData_chunk_rewrite form;
	ChunkRewriteDeleteResult result;

	if (!ts_chunk_rewrite_get_with_lock(chunk_relid, &form, &tid))
		return ChunkRewriteEntryDoesNotExist;

	if (conditional)
	{
		if (!ConditionalLockRelationOid(form.new_relid, AccessExclusiveLock))
			return ChunkRewriteOngoing;
	}

	/*
	 * Check if the new heap still exists by trying to get a lock.
	 */
	Relation newrel = try_table_open(form.new_relid, AccessExclusiveLock);

	if (newrel)
	{
		ObjectAddress tableaddr;
		/* New heap still exists, so delete it */
		table_close(newrel, NoLock);
		ObjectAddressSet(tableaddr, RelationRelationId, form.new_relid);
		performDeletion(&tableaddr, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);
		result = ChunkRewriteEntryDeletedAndTableDropped;
	}
	else
	{
		result = ChunkRewriteEntryDeleted;
	}

	ts_chunk_rewrite_delete_by_tid(&tid);

	return result;
}

TS_FUNCTION_INFO_V1(ts_chunk_rewrite_cleanup);

/*
 * Clean up failed chunk rewrites.
 *
 * This function cleans up all non-ongoing chunk rewrites listed in the chunk_rewrite catalog,
 * including any "orphaned" heaps referenced in the table.
 *
 */
Datum
ts_chunk_rewrite_cleanup(PG_FUNCTION_ARGS)
{
	ScanIterator it;
	ObjectAddresses *objaddrs = new_object_addresses();
	unsigned int cleanup_count = 0;
	unsigned int skipped_count = 0;
	Oid userid = GetUserId();
	CatalogSecurityContext sec_ctx;
	ScanTupLock tuplock = {
		.lockmode = LockTupleExclusive,
		.waitpolicy = LockWaitSkip,
		.lockflags = TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
	};

	it = ts_scan_iterator_create(CHUNK_REWRITE, RowShareLock, CurrentMemoryContext);
	it.ctx.tuplock = &tuplock;

	ts_scanner_foreach(&it)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&it);
		bool isnull = false;
		bool entry_cleaned = false;

		Datum chunk_relid_dat = slot_getattr(ti->slot, Anum_chunk_rewrite_chunk_relid, &isnull);
		Assert(!isnull);
		Datum new_relid_dat = slot_getattr(ti->slot, Anum_chunk_rewrite_new_relid, &isnull);
		Assert(!isnull);

		if (ti->lockresult == TM_Ok)
		{
			Oid chunk_relid = DatumGetObjectId(chunk_relid_dat);
			Oid new_relid = DatumGetObjectId(new_relid_dat);

			/*
			 * A concurrent merge might be in progress, so try to lock the "new"
			 * relation and only delete it if the lock can be acquired
			 * immediately. If a lock cannot be acquired, a merge is probably
			 * ongoing and it might still complete successfully.
			 */
			if (ConditionalLockRelationOid(new_relid, AccessExclusiveLock))
			{
				ObjectAddress new_objaddr = {
					.objectId = DatumGetObjectId(new_relid),
					.classId = RelationRelationId,
				};
				/*
				 * Check that the merge relation still exists and get its owner.
				 */
				HeapTuple tuple;
				Oid ownerid = InvalidOid;

				tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(chunk_relid));

				if (HeapTupleIsValid(tuple))
					ownerid = ((Form_pg_class) GETSTRUCT(tuple))->relowner;

				ReleaseSysCache(tuple);

				/*
				 * Only clean an entry if the user has the privileges of the owner of the relation.
				 * Also clean up if the relation doesn't exist anymore (relowner is InvalidOid).
				 */
				if (!OidIsValid(ownerid) || has_privs_of_role(userid, ownerid))
				{
					/*
					 * Check that the relation still exists. If it does, add to delete objects.
					 * Otherwise release lock.
					 */
					if (SearchSysCacheExists1(RELOID, ObjectIdGetDatum(new_relid)))
						add_exact_object_address(&new_objaddr, objaddrs);
					else
						UnlockRelationOid(new_relid, AccessExclusiveLock);

					ItemPointer tid = ts_scanner_get_tuple_tid(ti);

					ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
					ts_catalog_delete_tid_only(it.ctx.tablerel, tid);
					ts_catalog_restore_user(&sec_ctx);
					entry_cleaned = true;
				}
			}
		}

		if (entry_cleaned)
		{
			cleanup_count++;
		}
		else
		{
			Oid chunk_relid = DatumGetObjectId(chunk_relid_dat);
			elog(DEBUG1, "chunk merge in progress for \"%s\", skipping", get_rel_name(chunk_relid));
			skipped_count++;
		}
	}

	ts_scan_iterator_close(&it);
	performMultipleDeletions(objaddrs, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);

	elog(NOTICE,
		 "cleaned up %u orphaned rewrite relations, skipped %u",
		 cleanup_count,
		 skipped_count);

	PG_RETURN_VOID();
}
