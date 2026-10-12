/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * This file contains source code that was copied and/or modified from
 * the PostgreSQL database, which is licensed under the open-source
 * PostgreSQL License. Please see the NOTICE at the top level
 * directory for a copy of the PostgreSQL License.
 *
 * This mirrors backend/access/heap/heapam.c; see
 * import/compression_toast.h for what the fork as a whole is for, and the
 * comment above each function for which core function it copies and what
 * was changed.
 */

#include <postgres.h>

#include <access/heapam.h>
#include <access/heapam_xlog.h>
#include <access/heaptoast.h>
#include <access/hio.h>
#include <access/htup_details.h>
#include <access/visibilitymap.h>
#include <access/xact.h>
#include <access/xloginsert.h>
#include <catalog/catalog.h>
#include <miscadmin.h>
#include <pgstat.h>
#include <storage/bufmgr.h>
#include <storage/predicate.h>
#include <utils/inval.h>
#include <utils/rel.h>
#include <utils/snapmgr.h>

#include "debug_assert.h"
#include "import/compression_toast.h"

static HeapTuple
compression_heap_prepare_insert(BulkWriter *writer, HeapTuple tup, TransactionId xid);

/*
 * This is a copy of the static inline AssertHasSnapshotForToast() in
 * backend/access/heap/heapam.c from PG 18.6, git commit sha
 * 724edf9bde9d356724ad384a2e196edc3c9f80f7. It has one modification: it is
 * renamed, since the original is static and can't be called from this file.
 */
static inline void
compression_assert_has_snapshot_for_toast(Relation rel)
{
#ifdef USE_ASSERT_CHECKING

	/* bootstrap mode in particular breaks this rule */
	if (!IsNormalProcessingMode())
		return;

	/* if the relation doesn't have a TOAST table, we are good */
	if (!OidIsValid(rel->rd_rel->reltoastrelid))
		return;

	Assert(HaveRegisteredOrActiveSnapshot());

#endif							/* USE_ASSERT_CHECKING */
}

/*
 * This is a copy of heap_insert() in backend/access/heap/heapam.c from PG
 * 18.6, git commit sha 724edf9bde9d356724ad384a2e196edc3c9f80f7. It has a few
 * modifications: it calls compression_heap_prepare_insert() instead of
 * heap_prepare_insert(), so a compressed row whose compresseddata
 * attribute(s) need toasting goes through the heap_multi_insert()-based
 * toast writer instead of core's one-chunk-at-a-time path, change to the
 * function signature to accommodate for the said writer and a change for
 * dealing with catalog tables and logical decoding which is not applicable
 * here. It also opens with an Ensure() that this path is supported on the
 * running version, since callers are expected to keep it off where it is not.
 */
void
compression_heap_insert(BulkWriter *writer, HeapTuple tup)
{
	Relation relation = writer->out_rel;
	int options = writer->insert_options;
	BulkInsertState bistate = writer->bistate;
	TransactionId xid = GetCurrentTransactionId();
	HeapTuple	heaptup;
	Buffer		buffer;
	Page		page;
	Buffer		vmbuffer = InvalidBuffer;
	bool		clear_all_visible = false;
	bool		vmbuffer_modified = false;

	/*
	 * Callers are expected to keep this path off where it is not supported;
	 * see COMPRESSION_TOASTER_SUPPORTED in the header for which versions
	 * qualify and why.
	 */
	Ensure(COMPRESSION_TOASTER_SUPPORTED,
		   "custom compression toaster is not supported on this PostgreSQL version");

	/* Cheap, simplistic check that the tuple matches the rel's rowtype. */
	Assert(HeapTupleHeaderGetNatts(tup->t_data) <=
		   RelationGetNumberOfAttributes(relation));

	compression_assert_has_snapshot_for_toast(relation);

	/*
	 * Fill in tuple header fields and toast the tuple if necessary.
	 *
	 * Note: below this point, heaptup is the data we actually intend to store
	 * into the relation; tup is the caller's original untoasted data.
	 */
	heaptup = compression_heap_prepare_insert(writer, tup, xid);

	/*
	 * Find buffer to insert this tuple into.  If the page is all visible,
	 * this will also pin the requisite visibility map page.
	 */
	buffer = RelationGetBufferForTuple(relation, heaptup->t_len,
									   InvalidBuffer, options, bistate,
									   &vmbuffer, NULL,
									   0);
	page = BufferGetPage(buffer);

	/*
	 * We're about to do the actual insert -- but check for conflict first, to
	 * avoid possibly having to roll back work we've just done.
	 *
	 * This is safe without a recheck as long as there is no possibility of
	 * another process scanning the page between this check and the insert
	 * being visible to the scan (i.e., an exclusive buffer content lock is
	 * continuously held from this point until the tuple insert is visible).
	 *
	 * For a heap insert, we only need to check for table-level SSI locks. Our
	 * new tuple can't possibly conflict with existing tuple locks, and heap
	 * page locks are only consolidated versions of tuple locks; they do not
	 * lock "gaps" as index page locks do.  So we don't need to specify a
	 * buffer when making the call, which makes for a faster check.
	 */
	CheckForSerializableConflictIn(relation, NULL, InvalidBlockNumber);

	/* Lock the vmbuffer before the critical section */
	if (PageIsAllVisible(page))
	{
		LockBuffer(vmbuffer, BUFFER_LOCK_EXCLUSIVE);
		clear_all_visible = true;
	}

	/* NO EREPORT(ERROR) from here till changes are logged */
	START_CRIT_SECTION();

	RelationPutHeapTuple(relation, buffer, heaptup,
						 (options & HEAP_INSERT_SPECULATIVE) != 0);

	if (clear_all_visible)
	{
		/* It's possible the VM bits were already clear */
		if (visibilitymap_clear_locked(relation,
									   ItemPointerGetBlockNumber(&(heaptup->t_self)),
									   vmbuffer, VISIBILITYMAP_VALID_BITS))
			vmbuffer_modified = true;

		PageClearAllVisible(page);
	}

	/*
	 * XXX Should we set PageSetPrunable on this page ?
	 *
	 * The inserting transaction may eventually abort thus making this tuple
	 * DEAD and hence available for pruning. Though we don't want to optimize
	 * for aborts, if no other tuple in this page is UPDATEd/DELETEd, the
	 * aborted tuple will never be pruned until next vacuum is triggered.
	 *
	 * If you do add PageSetPrunable here, add it in heap_xlog_insert too.
	 */

	MarkBufferDirty(buffer);

	/* XLOG stuff */
	if (RelationNeedsWAL(relation))
	{
		xl_heap_insert xlrec;
		xl_heap_header xlhdr;
		XLogRecPtr	recptr;
		uint8		info = XLOG_HEAP_INSERT;
		int			bufflags = 0;

		/*
		 * Core calls log_heap_new_cid() here, but it's static and can't be
		 * called from this file. That's fine: it's only needed for catalog
		 * tables, and this code never runs on a catalog table.
		 */
		Assert(!RelationIsAccessibleInLogicalDecoding(relation));

		/*
		 * If this is the single and first tuple on page, we can reinit the
		 * page instead of restoring the whole thing.  Set flag, and hide
		 * buffer references from XLogInsert.
		 */
		if (ItemPointerGetOffsetNumber(&(heaptup->t_self)) == FirstOffsetNumber &&
			PageGetMaxOffsetNumber(page) == FirstOffsetNumber)
		{
			info |= XLOG_HEAP_INIT_PAGE;
			bufflags |= REGBUF_WILL_INIT;
		}

		xlrec.offnum = ItemPointerGetOffsetNumber(&heaptup->t_self);
		xlrec.flags = 0;
		if (clear_all_visible)
			xlrec.flags |= XLH_INSERT_ALL_VISIBLE_CLEARED;
		if (options & HEAP_INSERT_SPECULATIVE)
			xlrec.flags |= XLH_INSERT_IS_SPECULATIVE;
		Assert(ItemPointerGetBlockNumber(&heaptup->t_self) == BufferGetBlockNumber(buffer));

		/*
		 * For logical decoding, we need the tuple even if we're doing a full
		 * page write, so make sure it's included even if we take a full-page
		 * image. (XXX We could alternatively store a pointer into the FPW).
		 */
		if (RelationIsLogicallyLogged(relation) &&
			!(options & HEAP_INSERT_NO_LOGICAL))
		{
			xlrec.flags |= XLH_INSERT_CONTAINS_NEW_TUPLE;
			bufflags |= REGBUF_KEEP_DATA;

			if (IsToastRelation(relation))
				xlrec.flags |= XLH_INSERT_ON_TOAST_RELATION;
		}

		XLogBeginInsert();
		XLogRegisterData((char *)&xlrec, SizeOfHeapInsert);

		xlhdr.t_infomask2 = heaptup->t_data->t_infomask2;
		xlhdr.t_infomask = heaptup->t_data->t_infomask;
		xlhdr.t_hoff = heaptup->t_data->t_hoff;

		/*
		 * note we mark xlhdr as belonging to buffer; if XLogInsert decides to
		 * write the whole page to the xlog, we don't need to store
		 * xl_heap_header in the xlog.
		 */
		XLogRegisterBuffer(HEAP_INSERT_BLKREF_HEAP, buffer,
						   REGBUF_STANDARD | bufflags);
		XLogRegisterBufData(HEAP_INSERT_BLKREF_HEAP, (char *) &xlhdr,
							SizeOfHeapHeader);
		/* PG73FORMAT: write bitmap [+ padding] [+ oid] + data */
		XLogRegisterBufData(HEAP_INSERT_BLKREF_HEAP,
							(char *) heaptup->t_data + SizeofHeapTupleHeader,
							heaptup->t_len - SizeofHeapTupleHeader);

		/* filtering by origin on a row level is much more efficient */
		XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

		if (vmbuffer_modified)
			XLogRegisterBuffer(HEAP_INSERT_BLKREF_VM, vmbuffer, 0);

		recptr = XLogInsert(RM_HEAP_ID, info);

		PageSetLSN(page, recptr);

		if (vmbuffer_modified)
			PageSetLSN(BufferGetPage(vmbuffer), recptr);
	}

	END_CRIT_SECTION();

	UnlockReleaseBuffer(buffer);

	/*
	 * We locked vmbuffer if clear_all_visible was true regardless of whether
	 * or not we ended up modifying the vmbuffer.
	 */
	if (clear_all_visible)
		LockBuffer(vmbuffer, BUFFER_LOCK_UNLOCK);
	if (BufferIsValid(vmbuffer))
		ReleaseBuffer(vmbuffer);

	/*
	 * If tuple is cacheable, mark it for invalidation from the caches in case
	 * we abort.  Note it is OK to do this after releasing the buffer, because
	 * the heaptup data structure is all in local memory, not in the shared
	 * buffer.
	 */
	CacheInvalidateHeapTuple(relation, heaptup, NULL);

	/* Note: speculative insertions are counted too, even if aborted later */
	pgstat_count_heap_insert(relation, 1);

	/*
	 * If heaptup is a private copy, release it.  Don't forget to copy t_self
	 * back to the caller's image, too.
	 */
	if (heaptup != tup)
	{
		tup->t_self = heaptup->t_self;
		heap_freetuple(heaptup);
	}
}

/*
 * This is a copy of the static heap_prepare_insert() in
 * backend/access/heap/heapam.c from PG 18.6, git commit sha
 * 724edf9bde9d356724ad384a2e196edc3c9f80f7. It has a few modifications:
 *
 * 1. It calls compression_toast_insert_or_update() instead of
 *    heap_toast_insert_or_update() whenever toasting is needed.
 * 2. Core's IsParallelWorker() error and its branch for relations that
 *    aren't RELKIND_RELATION/RELKIND_MATVIEW (a table's own toast rel
 *    recursing into itself) are replaced with Asserts, since neither can
 *    happen for a compressed chunk table.
 * 3. Change to function signature to accommodate for the writer.
 */
static HeapTuple
compression_heap_prepare_insert(BulkWriter *writer, HeapTuple tup, TransactionId xid)
{
	CommandId cid = writer->mycid;
	int options = writer->insert_options;

	Assert(!IsParallelWorker());

	tup->t_data->t_infomask &= ~(HEAP_XACT_MASK);
	tup->t_data->t_infomask2 &= ~(HEAP2_XACT_MASK);
	tup->t_data->t_infomask |= HEAP_XMAX_INVALID;
	HeapTupleHeaderSetXmin(tup->t_data, xid);
	if (options & HEAP_INSERT_FROZEN)
		HeapTupleHeaderSetXminFrozen(tup->t_data);

	HeapTupleHeaderSetCmin(tup->t_data, cid);
	HeapTupleHeaderSetXmax(tup->t_data, 0); /* for cleanliness */
	tup->t_tableOid = RelationGetRelid(writer->out_rel);

	Assert(writer->out_rel->rd_rel->relkind == RELKIND_RELATION);

	if (HeapTupleHasExternal(tup) || tup->t_len > TOAST_TUPLE_THRESHOLD)
		return compression_toast_insert_or_update(writer, tup);
	else
		return tup;
}
