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
 * This mirrors backend/access/common/toast_internals.c; see
 * import/compression_toast.h for what the fork as a whole is for, and the
 * comment above each function for which core function it copies and what
 * was changed.
 */

#include <postgres.h>

#include <access/detoast.h>
#include <access/genam.h>
#include <access/heapam.h>
#include <access/heaptoast.h>
#include <access/htup_details.h>
#include <access/table.h>
#include <access/toast_internals.h>
#include <catalog/catalog.h>
#include <catalog/index.h>
#include <executor/tuptable.h>
#include <miscadmin.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <varatt.h>

#include "custom_type_cache.h"
#include "debug_assert.h"
#include "guc.h"
#include "import/compression_toast.h"
#include "ts_catalog/compression_settings.h"

/*
 * Rank above all type-ladder ranks: values whose batch fell back to the
 * array algorithm are written last, after every column that kept its
 * type-specific algorithm.
 */
#define TOAST_COLD_RANK 1000

/*
 * Type-ladder rank of a column type, smallest first: bool < int2 < int4 <
 * int8 (and the int8-compressed date/time types) < float4 < float8 < text
 * family < json/bson < bytea. Types not in the ladder sort with the text
 * family.
 */
static int32
compression_toast_type_rank(Oid typoid)
{
	switch (typoid)
	{
		case BOOLOID:
			return 0;
		case INT2OID:
			return 1;
		case INT4OID:
			return 2;
		case INT8OID:
		case DATEOID:
		case TIMEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			return 3;
		case FLOAT4OID:
			return 4;
		case FLOAT8OID:
			return 5;
		case JSONOID:
		case JSONBOID:
			return 7;
		case BYTEAOID:
			return 8;
		default:
			/* text family and everything unlisted */
			return 6;
	}
}

/*
 * Build writer->toast_attr_rank: the type-ladder rank of every attribute of
 * the output relation, indexed like its tuple descriptor.
 *
 * The ladder needs the column's uncompressed type, and the output relation
 * usually does not carry it: when out_rel is a compressed chunk, every
 * compressed column is typed compresseddata. So the compressed chunk is
 * mapped back to its uncompressed relation through the compression settings
 * catalog and the type is looked up there by column name (compressed columns
 * keep the names of the columns they compress). Attributes without a
 * counterpart keep the type from out_rel's own descriptor: the _ts_meta_*
 * metadata columns, and every attribute when out_rel is not a compressed
 * chunk at all (the DML decompression path writes through a BulkWriter on
 * the uncompressed chunk).
 *
 * Runs once per writer, on the first deferred toast write. The table lives
 * in the executor query context like the rest of the deferred queue.
 */
static void
compression_toast_build_attr_ranks(BulkWriter *writer)
{
	TupleDesc	desc = RelationGetDescr(writer->out_rel);
	Oid			uncompressed_relid =
		ts_relation_get_uncompressed_relid(RelationGetRelid(writer->out_rel));
	int32	   *ranks;

	ranks = MemoryContextAlloc(writer->estate->es_query_cxt, desc->natts * sizeof(int32));

	for (int i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(desc, i);
		Oid			typoid = attr->atttypid;

		if (OidIsValid(uncompressed_relid) && !attr->attisdropped)
		{
			AttrNumber	uattno = get_attnum(uncompressed_relid, NameStr(attr->attname));

			if (uattno != InvalidAttrNumber)
			{
				typoid = get_atttype(uncompressed_relid, uattno);
			}
		}

		ranks[i] = compression_toast_type_rank(typoid);
	}

	writer->toast_attr_rank = ranks;
}

/*
 * Flush-order rank for a toasted value. The primary ordering is the type
 * ladder of the column's uncompressed type, see
 * compression_toast_build_attr_ranks().
 *
 * On top of that, a value whose batch actually used the array fallback
 * algorithm is demoted to TOAST_COLD_RANK regardless of the column's type.
 * The algorithm is the first payload byte (CompressedDataHeader), readable
 * only when the value was not compressed by PG on the way out -- array and
 * dictionary columns use EXTENDED storage and may be, but their types
 * (json/bson/text/bytea) already rank at the cold end of the ladder, so the
 * demotion only needs to catch EXTERNAL-storage columns (e.g. uuid) whose
 * batch fell back. The header byte is only looked at for compresseddata
 * attributes; anything else is a plain user value.
 */
static int32
compression_toast_value_rank(BulkWriter *writer, int attno, const char *data_p, int32 data_todo,
							 bool pg_compressed)
{
	TupleDesc	desc = RelationGetDescr(writer->out_rel);
	int32		rank;

	if (writer->toast_attr_rank == NULL)
	{
		compression_toast_build_attr_ranks(writer);
	}
	Assert(attno >= 0 && attno < desc->natts);
	rank = writer->toast_attr_rank[attno];

	if (!pg_compressed && data_todo > 0 &&
		TupleDescAttr(desc, attno)->atttypid ==
			ts_custom_type_cache_get(CUSTOM_TYPE_COMPRESSED_DATA)->type_oid &&
		(uint8) data_p[0] == COMPRESSION_ALGORITHM_ARRAY)
	{
		rank = TOAST_COLD_RANK;
	}

	return rank;
}

/*
 * list_sort comparator for the flush: by rank (type ladder with array
 * demotion), then by column, then by queue order. The seq tie-break is
 * required because list_sort is qsort-based and not stable.
 */
static int
pending_toast_cmp(const ListCell *a, const ListCell *b)
{
	const PendingToastValue *pa = lfirst(a);
	const PendingToastValue *pb = lfirst(b);

	if (pa->rank != pb->rank)
		return pg_cmp_s32(pa->rank, pb->rank);
	if (pa->attno != pb->attno)
		return pg_cmp_s32(pa->attno, pb->attno);
	return pg_cmp_u64(pa->seq, pb->seq);
}

/*
 * Write the chunk rows and index entries for one toasted value. This is the
 * second half of compression_toast_save_datum_multi(), split out so the
 * chunk write can be deferred and replayed in a different order at flush
 * time. The toast relation and its indexes must already be open (they are
 * opened lazily on first use in compression_toast_save_datum_multi()).
 */
static void
compression_toast_write_value(BulkWriter *writer, Oid valueid, const char *data_p,
							  int32 data_todo)
{
	int			options = writer->insert_options;
	TupleDesc	toasttupDesc;
	Datum		t_values[3];
	bool		t_isnull[3];
	CommandId	mycid = GetCurrentCommandId(true);
	union
	{
		struct varlena hdr;
		/* this is to make the union big enough for a chunk: */
		char		data[TOAST_MAX_CHUNK_SIZE + VARHDRSZ];
		/* ensure union is aligned well enough: */
		int32		align_it;
	}			chunk_data = {0};
	int32		chunk_size;
	int nchunks;
	int32 *chunk_seqs;
	HeapTuple *toasttups;
	TupleTableSlot **slots;
	int i;

	toasttupDesc = writer->toast_rel->rd_att;

	/*
	 * Initialize constant parts of the tuple data
	 */
	t_values[0] = ObjectIdGetDatum(valueid);
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
	 * Insert all the chunks into the indexes.
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
}

/*
 * This is a copy of toast_save_datum() in backend/access/common/toast_internals.c
 * from PG 18.6, git commit sha 724edf9bde9d356724ad384a2e196edc3c9f80f7. It
 * has three modifications:
 *
 * 1. The per-chunk "heap_form_tuple + heap_insert + index_insert" loop is
 *    replaced by compression_toast_write_value(), which forms every chunk
 *    tuple up front, hands them all to a single heap_multi_insert() call,
 *    then indexes each chunk using the tid it was assigned.
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
 * 4. The chunk write is deferred: the value payload is copied into the
 *    writer's executor query context and queued on writer->pending_toast,
 *    and the chunks are only written when the queue is flushed (currently
 *    from compression_toast_writer_close()). Deferring lets the flush order
 *    the chunk writes by column rather than by batch, so the same column of
 *    consecutive batches lands on adjacent toast pages. The toast pointer
 *    returned here is complete without the chunks: va_valueid is allocated
 *    up front, and no reader can see the main tuple before this transaction
 *    commits, by which time the flush has run. The new attno parameter
 *    records which attribute the value came from for that ordering.
 */
Datum
compression_toast_save_datum_multi(BulkWriter *writer, Datum value, struct varlena *oldexternal,
								   int attno)
{
	Relation	rel = writer->out_rel;
	struct varlena *result;
	struct varatt_external toast_pointer;
	char	   *data_p;
	int32		data_todo;
	Pointer		dval = DatumGetPointer(value);

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

	/*
	 * Get the data pointer and length, and compute va_rawsize and va_extinfo.
	 *
	 * va_rawsize is the size of the equivalent fully uncompressed datum, so
	 * we have to adjust for short headers.
	 *
	 * va_extinfo stored the actual size of the data payload in the toast
	 * records and the compression method in first 2 bits if data is
	 * compressed.
	 */
	if (VARATT_IS_SHORT(dval))
	{
		data_p = VARDATA_SHORT(dval);
		data_todo = VARSIZE_SHORT(dval) - VARHDRSZ_SHORT;
		toast_pointer.va_rawsize = data_todo + VARHDRSZ;	/* as if not short */
		toast_pointer.va_extinfo = data_todo;
	}
	else if (VARATT_IS_COMPRESSED(dval))
	{
		data_p = VARDATA(dval);
		data_todo = VARSIZE(dval) - VARHDRSZ;
		/* rawsize in a compressed datum is just the size of the payload */
		toast_pointer.va_rawsize = VARDATA_COMPRESSED_GET_EXTSIZE(dval) + VARHDRSZ;

		/* set external size and compression method */
		VARATT_EXTERNAL_SET_SIZE_AND_COMPRESS_METHOD(toast_pointer, data_todo,
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

	/*
	 * Insert the correct table OID into the result TOAST pointer.
	 *
	 * Normally this is the actual OID of the target toast table, but during
	 * table-rewriting operations such as CLUSTER, we have to insert the OID
	 * of the table's real permanent toast table instead.  rd_toastoid is set
	 * if we have to substitute such an OID.
	 */
	if (OidIsValid(rel->rd_toastoid))
		toast_pointer.va_toastrelid = rel->rd_toastoid;
	else
		toast_pointer.va_toastrelid = RelationGetRelid(writer->toast_rel);

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

	/*
	 * Defer the chunk write: copy the payload into the writer's query context
	 * (data_p points into the caller's datum, which lives in the per-batch
	 * memory context that is reset after every batch) and queue it for the
	 * flush. Empty values have no chunks, so there is nothing to defer.
	 */
	if (data_todo > 0)
	{
		MemoryContext old_cxt = MemoryContextSwitchTo(writer->estate->es_query_cxt);
		PendingToastValue *pending = palloc0(sizeof(PendingToastValue) + data_todo);

		pending->valueid = toast_pointer.va_valueid;
		pending->attno = attno;
		pending->rank = compression_toast_value_rank(writer, attno, data_p, data_todo,
													 VARATT_IS_COMPRESSED(dval));
		pending->seq = writer->pending_seq++;
		pending->data_len = data_todo;
		memcpy(pending->data, data_p, data_todo);
		writer->pending_toast = lappend(writer->pending_toast, pending);
		writer->pending_toast_bytes += data_todo;

		MemoryContextSwitchTo(old_cxt);

		/*
		 * Byte-threshold safety valve: flush even mid-batch if the buffer
		 * grows past the configured size. The batch-count bound is checked
		 * per batch in compression_heap_insert().
		 */
		if (writer->pending_toast_bytes >= (int64) ts_guc_compression_toast_buffer_size * 1024)
		{
			compression_toast_flush_pending(writer);
		}
	}

	/*
	 * Create the TOAST pointer value that we'll return
	 */
	result = (struct varlena *) palloc(TOAST_POINTER_SIZE);
	SET_VARTAG_EXTERNAL(result, VARTAG_ONDISK);
	memcpy(VARDATA_EXTERNAL(result), &toast_pointer, sizeof(toast_pointer));

	return PointerGetDatum(result);
}

/*
 * Write all deferred toast chunks and free the queue. Called from
 * compression_toast_writer_close() and from the buffer bounds checks in
 * compression_toast_save_datum_multi() (byte threshold) and
 * compression_heap_insert() (batch count). The queue is sorted before
 * writing: values are grouped by column (column-major), and the columns are
 * ordered by the type ladder with array-fallback values demoted to the end,
 * so the same column of consecutive batches lands on adjacent toast pages
 * and small hot columns precede the bulky cold ones.
 *
 * This is not a forked/mirrored function.
 */
void
compression_toast_flush_pending(BulkWriter *writer)
{
	ListCell   *lc;

	if (writer->pending_toast == NIL)
	{
		return;
	}

	Assert(writer->toast_rel != NULL);

	list_sort(writer->pending_toast, pending_toast_cmp);

	foreach (lc, writer->pending_toast)
	{
		PendingToastValue *pending = lfirst(lc);

		compression_toast_write_value(writer, pending->valueid, pending->data, pending->data_len);
		pfree(pending);
	}

	list_free(writer->pending_toast);
	writer->pending_toast = NIL;
	writer->pending_toast_bytes = 0;
	writer->pending_batches = 0;
}

/*
 * Close the toast relation/indexes lazily opened by
 * compression_toast_save_datum_multi(), if any were. Mirrors core's own
 * toast_save_datum() in keeping the lock until commit (NoLock here), so a
 * concurrent reindex on the toast relation waits for this transaction rather
 * than racing it.
 *
 * This is not a forked/mirrored function.
 */
void
compression_toast_writer_close(BulkWriter *writer)
{
	compression_toast_flush_pending(writer);

	if (writer->toast_rel == NULL)
	{
		return;
	}

	FreeBulkInsertState(writer->toast_bistate);
	toast_close_indexes(writer->toast_indexes, writer->num_toast_indexes, NoLock);
	table_close(writer->toast_rel, NoLock);
	writer->toast_rel = NULL;
}
