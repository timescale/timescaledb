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

#include "import/compression_toast.h"

/*
 * This is a copy of the static inline AssertHasSnapshotForToast() in
 * backend/access/heap/heapam.c from PG 18.4, git commit sha
 * f5cc81719e6da4cbdb1f797c48b693e91018153a. It has one modification: it is
 * renamed, since the original is static and can't be called from this file.
 */
static inline void
compression_assert_has_snapshot_for_toast(Relation rel)
{
#ifdef USE_ASSERT_CHECKING
	if (!IsNormalProcessingMode())
	{
		return;
	}
	if (!OidIsValid(rel->rd_rel->reltoastrelid))
	{
		return;
	}
	Assert(HaveRegisteredOrActiveSnapshot());
#endif
}

/*
 * This is a copy of the static heap_prepare_insert() in
 * backend/access/heap/heapam.c from PG 18.4, git commit sha
 * f5cc81719e6da4cbdb1f797c48b693e91018153a. It has two modifications:
 *
 * 1. It calls compression_toast_insert_or_update() instead of
 *    heap_toast_insert_or_update() whenever toasting is needed.
 * 2. Core's IsParallelWorker() error and its branch for relations that
 *    aren't RELKIND_RELATION/RELKIND_MATVIEW (a table's own toast rel
 *    recursing into itself) are replaced with Asserts, since neither can
 *    happen for a compressed chunk table.
 */
static HeapTuple
compression_heap_prepare_insert(BulkWriter *writer, HeapTuple tup, TransactionId xid)
{
	CommandId cid = writer->mycid;
	int options = writer->insert_options;

	Assert(!IsParallelWorker());
	Assert(writer->out_rel->rd_rel->relkind == RELKIND_RELATION);

	tup->t_data->t_infomask &= ~(HEAP_XACT_MASK);
	tup->t_data->t_infomask2 &= ~(HEAP2_XACT_MASK);
	tup->t_data->t_infomask |= HEAP_XMAX_INVALID;
	HeapTupleHeaderSetXmin(tup->t_data, xid);
	if (options & HEAP_INSERT_FROZEN)
	{
		HeapTupleHeaderSetXminFrozen(tup->t_data);
	}

	HeapTupleHeaderSetCmin(tup->t_data, cid);
	HeapTupleHeaderSetXmax(tup->t_data, 0);
	tup->t_tableOid = RelationGetRelid(writer->out_rel);

	if (HeapTupleHasExternal(tup) || tup->t_len > TOAST_TUPLE_THRESHOLD)
	{
		return compression_toast_insert_or_update(writer, tup);
	}
	else
	{
		return tup;
	}
}

/*
 * This is a copy of heap_insert() in backend/access/heap/heapam.c from PG
 * 18.4, git commit sha f5cc81719e6da4cbdb1f797c48b693e91018153a. It has one
 * modification: it calls compression_heap_prepare_insert() instead of
 * heap_prepare_insert(), so a compressed row whose compresseddata
 * attribute(s) need toasting goes through the heap_multi_insert()-based
 * toast writer instead of core's one-chunk-at-a-time path.
 */
void
compression_heap_insert(BulkWriter *writer, HeapTuple tup)
{
	Relation relation = writer->out_rel;
	int options = writer->insert_options;
	BulkInsertState bistate = writer->bistate;
	TransactionId xid = GetCurrentTransactionId();
	HeapTuple heaptup;
	Buffer buffer;
	Buffer vmbuffer = InvalidBuffer;
	bool all_visible_cleared = false;

	Assert(HeapTupleHeaderGetNatts(tup->t_data) <= RelationGetNumberOfAttributes(relation));
	compression_assert_has_snapshot_for_toast(relation);

	heaptup = compression_heap_prepare_insert(writer, tup, xid);

	buffer = RelationGetBufferForTuple(relation,
									   heaptup->t_len,
									   InvalidBuffer,
									   options,
									   bistate,
									   &vmbuffer,
									   NULL,
									   0);

	CheckForSerializableConflictIn(relation, NULL, InvalidBlockNumber);

	START_CRIT_SECTION();

	RelationPutHeapTuple(relation, buffer, heaptup, (options & HEAP_INSERT_SPECULATIVE) != 0);

	if (PageIsAllVisible(BufferGetPage(buffer)))
	{
		all_visible_cleared = true;
		PageClearAllVisible(BufferGetPage(buffer));
		visibilitymap_clear(relation,
							ItemPointerGetBlockNumber(&(heaptup->t_self)),
							vmbuffer,
							VISIBILITYMAP_VALID_BITS);
	}

	/*
	 * PG19 note for future porting: PG19's heap_insert()/heap_multi_insert()
	 * add a PageSetPrunable(page, xid) call here (skipped when
	 * HEAP_INSERT_FROZEN is set) so an aborted, non-frozen insert's dead
	 * tuple can be opportunistically pruned instead of waiting for the next
	 * VACUUM. PG18 (what this fork targets) has no such call. If this file
	 * is ever ported to PG19, add the same call here.
	 */

	MarkBufferDirty(buffer);

	if (RelationNeedsWAL(relation))
	{
		xl_heap_insert xlrec;
		xl_heap_header xlhdr;
		XLogRecPtr recptr;
		Page page = BufferGetPage(buffer);
		uint8 info = XLOG_HEAP_INSERT;
		int bufflags = 0;

		/*
		 * Core calls log_heap_new_cid() here, but it's static and can't be
		 * called from this file. That's fine: it's only needed for catalog
		 * tables, and this code never runs on a catalog table.
		 */
		Assert(!RelationIsAccessibleInLogicalDecoding(relation));

		if (ItemPointerGetOffsetNumber(&(heaptup->t_self)) == FirstOffsetNumber &&
			PageGetMaxOffsetNumber(page) == FirstOffsetNumber)
		{
			info |= XLOG_HEAP_INIT_PAGE;
			bufflags |= REGBUF_WILL_INIT;
		}

		xlrec.offnum = ItemPointerGetOffsetNumber(&heaptup->t_self);
		xlrec.flags = 0;
		if (all_visible_cleared)
		{
			xlrec.flags |= XLH_INSERT_ALL_VISIBLE_CLEARED;
		}
		if (options & HEAP_INSERT_SPECULATIVE)
		{
			xlrec.flags |= XLH_INSERT_IS_SPECULATIVE;
		}
		Assert(ItemPointerGetBlockNumber(&heaptup->t_self) == BufferGetBlockNumber(buffer));

		if (RelationIsLogicallyLogged(relation) && !(options & HEAP_INSERT_NO_LOGICAL))
		{
			xlrec.flags |= XLH_INSERT_CONTAINS_NEW_TUPLE;
			bufflags |= REGBUF_KEEP_DATA;

			if (IsToastRelation(relation))
			{
				xlrec.flags |= XLH_INSERT_ON_TOAST_RELATION;
			}
		}

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, SizeOfHeapInsert);

		xlhdr.t_infomask2 = heaptup->t_data->t_infomask2;
		xlhdr.t_infomask = heaptup->t_data->t_infomask;
		xlhdr.t_hoff = heaptup->t_data->t_hoff;

		XLogRegisterBuffer(0, buffer, REGBUF_STANDARD | bufflags);
		XLogRegisterBufData(0, (char *) &xlhdr, SizeOfHeapHeader);
		XLogRegisterBufData(0,
							(char *) heaptup->t_data + SizeofHeapTupleHeader,
							heaptup->t_len - SizeofHeapTupleHeader);

		XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

		recptr = XLogInsert(RM_HEAP_ID, info);

		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	UnlockReleaseBuffer(buffer);
	if (vmbuffer != InvalidBuffer)
	{
		ReleaseBuffer(vmbuffer);
	}

	CacheInvalidateHeapTuple(relation, heaptup, NULL);

	pgstat_count_heap_insert(relation, 1);

	if (heaptup != tup)
	{
		tup->t_self = heaptup->t_self;
		heap_freetuple(heaptup);
	}
}
