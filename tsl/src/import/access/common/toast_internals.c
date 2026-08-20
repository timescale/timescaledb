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
#include <utils/rel.h>
#include <varatt.h>

#include "debug_assert.h"
#include "import/compression_toast.h"

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
Datum
compression_toast_save_datum_multi(BulkWriter *writer, Datum value, struct varlena *oldexternal)
{
	Relation rel = writer->out_rel;
	int options = writer->insert_options;
	TupleDesc	toasttupDesc;
	Datum		t_values[3];
	bool		t_isnull[3];
	CommandId	mycid = GetCurrentCommandId(true);
	struct varlena *result;
	struct varatt_external toast_pointer;
	union
	{
		struct varlena hdr;
		/* this is to make the union big enough for a chunk: */
		char		data[TOAST_MAX_CHUNK_SIZE + VARHDRSZ];
		/* ensure union is aligned well enough: */
		int32		align_it;
	}			chunk_data = {0};	/* silence compiler warning */
	int32		chunk_size;
	char	   *data_p;
	int32		data_todo;
	Pointer		dval = DatumGetPointer(value);
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
	 * Initialize constant parts of the tuple data
	 */
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

	/*
	 * Create the TOAST pointer value that we'll return
	 */
	result = (struct varlena *) palloc(TOAST_POINTER_SIZE);
	SET_VARTAG_EXTERNAL(result, VARTAG_ONDISK);
	memcpy(VARDATA_EXTERNAL(result), &toast_pointer, sizeof(toast_pointer));

	return PointerGetDatum(result);
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
	if (writer->toast_rel == NULL)
	{
		return;
	}

	FreeBulkInsertState(writer->toast_bistate);
	toast_close_indexes(writer->toast_indexes, writer->num_toast_indexes, NoLock);
	table_close(writer->toast_rel, NoLock);
	writer->toast_rel = NULL;
}
