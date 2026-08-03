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
 * The functions below are each copies of a PostgreSQL core heap/toast insert
 * function; see the comment above each one for which core function it
 * copies, from which file, and what was changed.
 */

#include "compression_toast.h"

#include <postgres.h>

#include <access/detoast.h>
#include <access/genam.h>
#include <access/heapam.h>
#include <access/heapam_xlog.h>
#include <access/heaptoast.h>
#include <access/hio.h>
#include <access/htup_details.h>
#include <access/tableam.h>
#include <access/toast_helper.h>
#include <access/toast_internals.h>
#include <access/visibilitymap.h>
#include <access/xact.h>
#include <access/xloginsert.h>
#include <catalog/catalog.h>
#include <catalog/index.h>
#include <executor/tuptable.h>
#include <miscadmin.h>
#include <pgstat.h>
#include <storage/bufmgr.h>
#include <storage/predicate.h>
#include <utils/inval.h>
#include <utils/rel.h>
#include <utils/snapmgr.h>
#include <varatt.h>

#include "debug_assert.h"

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
 * This is a copy of toast_save_datum() in backend/access/common/toast_internals.c
 * from PG 18.4, git commit sha f5cc81719e6da4cbdb1f797c48b693e91018153a. It
 * has three modifications:
 *
 * 1. The per-chunk "heap_form_tuple + heap_insert + index_insert" loop is
 *    replaced by: form every chunk tuple up front, hand them all to a single
 *    heap_multi_insert() call, then index each chunk using the tid it was
 *    assigned.
 * 2. The toast relation and its indexes are opened once per BulkWriter
 *    (lazily, on first use here) and closed by compression_toast_writer_close(),
 *    instead of being opened and closed on every call -- row_compressor_flush()
 *    calls this once per toasted compresseddata column per batch, so a
 *    compressed chunk with several batches and/or several toasted columns
 *    would otherwise repeat that open/close needlessly.
 * 3. The rewrite-preservation branch (reusing oldexternal's value id via
 *    toastid_valueid_exists()) is replaced with an Ensure(): the
 *    compressed-row insert path never runs under a toast-table-preserving
 *    rewrite (CLUSTER/VACUUM FULL-style), so that branch is never reachable
 *    here.
 */
static Datum
compression_toast_save_datum_multi(BulkWriter *writer, Datum value, struct varlena *oldexternal)
{
	Relation rel = writer->out_rel;
	int options = writer->insert_options;
	TupleDesc toasttupDesc;
	Datum t_values[3];
	bool t_isnull[3];
	CommandId mycid = GetCurrentCommandId(true);
	struct varlena *result;
	struct varatt_external toast_pointer;
	union
	{
		struct varlena hdr;
		char data[TOAST_MAX_CHUNK_SIZE + VARHDRSZ];
		int32 align_it;
	} chunk_data = { 0 };
	int32 chunk_size;
	char *data_p;
	int32 data_todo;
	Pointer dval = DatumGetPointer(value);
	int nchunks;
	int32 *chunk_seqs;
	HeapTuple *toasttups;
	TupleTableSlot **slots;
	int i;

	Assert(!VARATT_IS_EXTERNAL(dval));

	if (writer->toast_rel == NULL)
	{
		/*
		 * Allocated in the writer's query context, not the caller's current
		 * context: row_compressor_flush() runs in row_compressor->per_row_ctx,
		 * which is reset after every batch (row_compressor_clear_batch()), but
		 * this handle must survive for the writer's whole lifetime.
		 */
		MemoryContext old_cxt = MemoryContextSwitchTo(writer->estate->es_query_cxt);

		writer->toast_rel = table_open(rel->rd_rel->reltoastrelid, RowExclusiveLock);
		writer->toast_valid_index = toast_open_indexes(writer->toast_rel,
													   RowExclusiveLock,
													   &writer->toast_indexes,
													   &writer->num_toast_indexes);
		writer->toast_bistate = GetBulkInsertState();

		MemoryContextSwitchTo(old_cxt);
	}
	toasttupDesc = writer->toast_rel->rd_att;

	if (VARATT_IS_SHORT(dval))
	{
		data_p = VARDATA_SHORT(dval);
		data_todo = VARSIZE_SHORT(dval) - VARHDRSZ_SHORT;
		toast_pointer.va_rawsize = data_todo + VARHDRSZ;
		toast_pointer.va_extinfo = data_todo;
	}
	else if (VARATT_IS_COMPRESSED(dval))
	{
		data_p = VARDATA(dval);
		data_todo = VARSIZE(dval) - VARHDRSZ;
		toast_pointer.va_rawsize = VARDATA_COMPRESSED_GET_EXTSIZE(dval) + VARHDRSZ;
		VARATT_EXTERNAL_SET_SIZE_AND_COMPRESS_METHOD(toast_pointer,
													 data_todo,
													 VARDATA_COMPRESSED_GET_COMPRESS_METHOD(dval));
		/*
		 * VARATT_EXTERNAL_IS_COMPRESSED() compares va_extinfo (uint32)
		 * against va_rawsize (int32), same as core's toast_save_datum() does
		 * at this exact spot -- silence the sign-compare warning rather than
		 * touch the core macro.
		 */
#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#endif
		Assert(VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer));
#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif
	}
	else
	{
		data_p = VARDATA(dval);
		data_todo = VARSIZE(dval) - VARHDRSZ;
		toast_pointer.va_rawsize = VARSIZE(dval);
		toast_pointer.va_extinfo = data_todo;
	}

	if (OidIsValid(rel->rd_toastoid))
	{
		toast_pointer.va_toastrelid = rel->rd_toastoid;
	}
	else
	{
		toast_pointer.va_toastrelid = RelationGetRelid(writer->toast_rel);
	}

	/*
	 * The compressed-row insert path never runs under a toast-table-preserving
	 * rewrite (CLUSTER/VACUUM FULL-style), so rd_toastoid should never be set
	 * here. Core's rewrite-preservation branch (reusing oldexternal's value id
	 * via toastid_valueid_exists()) isn't reachable and isn't replicated;
	 * fail loudly rather than silently skip it if that assumption ever
	 * changes.
	 */
	Ensure(!OidIsValid(rel->rd_toastoid), "unexpected toast relation missing during compression");
	toast_pointer.va_valueid =
		GetNewOidWithIndex(writer->toast_rel,
						   RelationGetRelid(writer->toast_indexes[writer->toast_valid_index]),
						   (AttrNumber) 1);

	t_values[0] = ObjectIdGetDatum(toast_pointer.va_valueid);
	t_isnull[0] = false;
	t_isnull[1] = false;
	t_isnull[2] = false;

	nchunks = data_todo > 0 ? (data_todo + TOAST_MAX_CHUNK_SIZE - 1) / TOAST_MAX_CHUNK_SIZE : 0;

	toasttups = nchunks > 0 ? palloc(nchunks * sizeof(HeapTuple)) : NULL;
	slots = nchunks > 0 ? palloc(nchunks * sizeof(TupleTableSlot *)) : NULL;
	chunk_seqs = nchunks > 0 ? palloc(nchunks * sizeof(int32)) : NULL;

	for (i = 0; data_todo > 0; i++)
	{
		CHECK_FOR_INTERRUPTS();

		/*
		 * TOAST_MAX_CHUNK_SIZE is derived from several sizeof()s and so is
		 * unsigned, compared here against int32 data_todo -- same
		 * sign-compare core's toast_save_datum() has at this exact line.
		 */
#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#endif
		chunk_size = Min(TOAST_MAX_CHUNK_SIZE, data_todo);
#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif
		chunk_seqs[i] = i;

		t_values[1] = Int32GetDatum(i);
		SET_VARSIZE(&chunk_data, chunk_size + VARHDRSZ);
		memcpy(VARDATA(&chunk_data), data_p, chunk_size);
		t_values[2] = PointerGetDatum(&chunk_data);

		toasttups[i] = heap_form_tuple(toasttupDesc, t_values, t_isnull);
		slots[i] = MakeSingleTupleTableSlot(toasttupDesc, &TTSOpsHeapTuple);
		/*
		 * shouldFree = true: heap_multi_insert() fetches each slot's tuple
		 * with materialize = true, and tts_heap_materialize() only skips its
		 * copy when the slot already owns the tuple (TTS_SHOULDFREE). With
		 * shouldFree = false here, it would silently substitute a copy, and
		 * RelationPutHeapTuple()'s t_self update would land on that copy
		 * instead of toasttups[i] -- leaving toasttups[i]->t_self invalid and
		 * corrupting the index entries built from it below.
		 */
		ExecStoreHeapTuple(toasttups[i], slots[i], true);

		data_todo -= chunk_size;
		data_p += chunk_size;
	}
	Assert(i == nchunks);

	if (nchunks > 0)
	{
		heap_multi_insert(writer->toast_rel, slots, nchunks, mycid, options, writer->toast_bistate);
	}

	/*
	 * Index each chunk individually, same as core: the toast index's columns
	 * are the leading columns of the toast table, so we can hand index_insert()
	 * the same t_values/t_isnull we built the tuple with, without a full
	 * FormIndexDatum().
	 */
	for (i = 0; i < nchunks; i++)
	{
		int j;

		t_values[1] = Int32GetDatum(chunk_seqs[i]);
		for (j = 0; j < writer->num_toast_indexes; j++)
		{
			Relation toastidx = writer->toast_indexes[j];

			if (toastidx->rd_index->indisready)
			{
				index_insert(toastidx,
							 t_values,
							 t_isnull,
							 &(toasttups[i]->t_self),
							 writer->toast_rel,
							 toastidx->rd_index->indisunique ? UNIQUE_CHECK_YES : UNIQUE_CHECK_NO,
							 false,
							 NULL);
			}
		}
		/* Slot owns toasttups[i] (shouldFree = true above); dropping it frees
		 * the tuple along with the slot. */
		ExecDropSingleTupleTableSlot(slots[i]);
	}

	result = (struct varlena *) palloc(TOAST_POINTER_SIZE);
	SET_VARTAG_EXTERNAL(result, VARTAG_ONDISK);
	memcpy(VARDATA_EXTERNAL(result), &toast_pointer, sizeof(toast_pointer));

	return PointerGetDatum(result);
}

/*
 * This is a copy of toast_tuple_externalize() in backend/access/table/toast_helper.c
 * from PG 18.4, git commit sha f5cc81719e6da4cbdb1f797c48b693e91018153a. It
 * has one modification: it calls compression_toast_save_datum_multi() instead
 * of toast_save_datum().
 */
static void
compression_toast_tuple_externalize(BulkWriter *writer, ToastTupleContext *ttc, int attribute)
{
	Datum *value = &ttc->ttc_values[attribute];
	Datum old_value = *value;
	ToastAttrInfo *attr = &ttc->ttc_attr[attribute];

	attr->tai_colflags |= TOASTCOL_IGNORE;
	*value = compression_toast_save_datum_multi(writer, old_value, attr->tai_oldexternal);
	if ((attr->tai_colflags & TOASTCOL_NEEDS_FREE) != 0)
	{
		pfree(DatumGetPointer(old_value));
	}
	attr->tai_colflags |= TOASTCOL_NEEDS_FREE;
	ttc->ttc_flags |= (TOAST_NEEDS_CHANGE | TOAST_NEEDS_FREE);
}

/*
 * This is a copy of heap_toast_insert_or_update() in backend/access/heap/heaptoast.c
 * from PG 18.4, git commit sha f5cc81719e6da4cbdb1f797c48b693e91018153a. It
 * has one modification: it calls compression_toast_tuple_externalize()
 * instead of toast_tuple_externalize(), so externalized values go through
 * the heap_multi_insert()-based writer above.
 */
static HeapTuple
compression_toast_insert_or_update(BulkWriter *writer, HeapTuple newtup)
{
	Relation rel = writer->out_rel;
	HeapTuple result_tuple;
	TupleDesc tupleDesc;
	int numAttrs;

	Size maxDataLen;
	Size hoff;

	bool toast_isnull[MaxHeapAttributeNumber];
	Datum toast_values[MaxHeapAttributeNumber];
	ToastAttrInfo toast_attr[MaxHeapAttributeNumber];
	ToastTupleContext ttc;

	Assert(rel->rd_rel->relkind == RELKIND_RELATION);

	tupleDesc = rel->rd_att;
	numAttrs = tupleDesc->natts;

	Assert(numAttrs <= MaxHeapAttributeNumber);
	heap_deform_tuple(newtup, tupleDesc, toast_values, toast_isnull);

	ttc.ttc_rel = rel;
	ttc.ttc_values = toast_values;
	ttc.ttc_isnull = toast_isnull;
	ttc.ttc_oldvalues = NULL;
	ttc.ttc_oldisnull = NULL;
	ttc.ttc_attr = toast_attr;
	toast_tuple_init(&ttc);

	hoff = SizeofHeapTupleHeader;
	if ((ttc.ttc_flags & TOAST_HAS_NULLS) != 0)
	{
		hoff += BITMAPLEN(numAttrs);
	}
	hoff = MAXALIGN(hoff);
	maxDataLen = RelationGetToastTupleTarget(rel, TOAST_TUPLE_TARGET) - hoff;

	while (heap_compute_data_size(tupleDesc, toast_values, toast_isnull) > maxDataLen)
	{
		int biggest_attno;

		biggest_attno = toast_tuple_find_biggest_attribute(&ttc, true, false);
		if (biggest_attno < 0)
		{
			break;
		}

		if (TupleDescAttr(tupleDesc, biggest_attno)->attstorage == TYPSTORAGE_EXTENDED)
		{
			toast_tuple_try_compression(&ttc, biggest_attno);
		}
		else
		{
			toast_attr[biggest_attno].tai_colflags |= TOASTCOL_INCOMPRESSIBLE;
		}

		if ((Size) toast_attr[biggest_attno].tai_size > maxDataLen &&
			OidIsValid(rel->rd_rel->reltoastrelid))
		{
			compression_toast_tuple_externalize(writer, &ttc, biggest_attno);
		}
	}

	while (heap_compute_data_size(tupleDesc, toast_values, toast_isnull) > maxDataLen &&
		   OidIsValid(rel->rd_rel->reltoastrelid))
	{
		int biggest_attno;

		biggest_attno = toast_tuple_find_biggest_attribute(&ttc, false, false);
		if (biggest_attno < 0)
		{
			break;
		}
		compression_toast_tuple_externalize(writer, &ttc, biggest_attno);
	}

	while (heap_compute_data_size(tupleDesc, toast_values, toast_isnull) > maxDataLen)
	{
		int biggest_attno;

		biggest_attno = toast_tuple_find_biggest_attribute(&ttc, true, true);
		if (biggest_attno < 0)
		{
			break;
		}

		toast_tuple_try_compression(&ttc, biggest_attno);
	}

	maxDataLen = TOAST_TUPLE_TARGET_MAIN - hoff;

	while (heap_compute_data_size(tupleDesc, toast_values, toast_isnull) > maxDataLen &&
		   OidIsValid(rel->rd_rel->reltoastrelid))
	{
		int biggest_attno;

		biggest_attno = toast_tuple_find_biggest_attribute(&ttc, false, true);
		if (biggest_attno < 0)
		{
			break;
		}

		compression_toast_tuple_externalize(writer, &ttc, biggest_attno);
	}

	if ((ttc.ttc_flags & TOAST_NEEDS_CHANGE) != 0)
	{
		HeapTupleHeader olddata = newtup->t_data;
		HeapTupleHeader new_data;
		int32 new_header_len;
		int32 new_data_len;
		int32 new_tuple_len;

		new_header_len = SizeofHeapTupleHeader;
		if ((ttc.ttc_flags & TOAST_HAS_NULLS) != 0)
		{
			new_header_len += BITMAPLEN(numAttrs);
		}
		new_header_len = MAXALIGN(new_header_len);
		new_data_len = heap_compute_data_size(tupleDesc, toast_values, toast_isnull);
		new_tuple_len = new_header_len + new_data_len;

		result_tuple = (HeapTuple) palloc0(HEAPTUPLESIZE + new_tuple_len);
		result_tuple->t_len = new_tuple_len;
		result_tuple->t_self = newtup->t_self;
		result_tuple->t_tableOid = newtup->t_tableOid;
		new_data = (HeapTupleHeader) ((char *) result_tuple + HEAPTUPLESIZE);
		result_tuple->t_data = new_data;

		memcpy(new_data, olddata, SizeofHeapTupleHeader);
		HeapTupleHeaderSetNatts(new_data, numAttrs);
		new_data->t_hoff = new_header_len;

		heap_fill_tuple(tupleDesc,
						toast_values,
						toast_isnull,
						(char *) new_data + new_header_len,
						new_data_len,
						&(new_data->t_infomask),
						((ttc.ttc_flags & TOAST_HAS_NULLS) != 0) ? new_data->t_bits : NULL);
	}
	else
	{
		result_tuple = newtup;
	}

	toast_tuple_cleanup(&ttc);

	return result_tuple;
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
 * toast writer above instead of core's one-chunk-at-a-time path.
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

/*
 * Close the toast relation/indexes lazily opened by
 * compression_toast_save_datum_multi(), if any were. Mirrors core's own
 * toast_save_datum() in keeping the lock until commit (NoLock here), so a
 * concurrent reindex on the toast relation waits for this transaction rather
 * than racing it.
 */
void
compression_toast_writer_close(BulkWriter *writer)
{
	if (writer->toast_rel == NULL)
	{
		return;
	}

	FreeBulkInsertState(writer->toast_bistate);
	toast_close_indexes(writer->toast_indexes, writer->num_toast_indexes, NoLock);
	table_close(writer->toast_rel, NoLock);
	writer->toast_rel = NULL;
}
