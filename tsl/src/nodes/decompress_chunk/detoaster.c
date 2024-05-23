/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "detoaster.h"

#include <access/detoast.h>
#include <access/genam.h>
#include <access/heaptoast.h>
#include <access/relscan.h>
#include <access/skey.h>
#include <access/stratnum.h>
#include <access/table.h>
#include <access/tableam.h>
#include <access/toast_internals.h>
#include <utils/fmgroids.h>
#include <utils/expandeddatum.h>
#include <utils/rel.h>
#include <utils/relcache.h>

#include <compat/compat.h>
#include <compression/compression.h>
#include "debug_assert.h"

/* We redefine this postgres macro to fix a warning about signed integer comparison. */
#define TS_VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer)                                            \
	(((int32) VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer)) < (toast_pointer).va_rawsize - VARHDRSZ)

/*
 * Fetch a TOAST slice from a heap table.
 *
 * This function is a modified copy of heap_fetch_toast_slice(). The difference
 * is that it holds the open toast relation, index and other intermediate data
 * for detoasting in the Detoaster struct, to allow them to be reused over many
 * input tuples.
 */
static void
ts_fetch_toast(Detoaster *detoaster, struct varatt_external *toast_pointer, struct varlena *result)
{
	const Oid valueid = toast_pointer->va_valueid;

	/*
	 * Open the toast relation and its indexes
	 */
	if (detoaster->toastrel == NULL)
	{
		MemoryContext old_mctx = MemoryContextSwitchTo(detoaster->mctx);
		detoaster->toastrel = table_open(toast_pointer->va_toastrelid, AccessShareLock);

		int num_indexes;
		Relation *toastidxs;
		/* Look for the valid index of toast relation */
		const int validIndex =
			toast_open_indexes(detoaster->toastrel, AccessShareLock, &toastidxs, &num_indexes);
		detoaster->index = toastidxs[validIndex];
		for (int i = 0; i < num_indexes; i++)
		{
			if (i != validIndex)
			{
				index_close(toastidxs[i], AccessShareLock);
			}
		}

		/* Set up a scan key to fetch from the index. */
		ScanKeyInit(&detoaster->toastkey,
					(AttrNumber) 1,
					BTEqualStrategyNumber,
					F_OIDEQ,
					ObjectIdGetDatum(valueid));

		/* Prepare for scan */
		init_toast_snapshot(&detoaster->SnapshotToast);
		detoaster->toastscan = systable_beginscan_ordered(detoaster->toastrel,
														  detoaster->index,
														  &detoaster->SnapshotToast,
														  1,
														  &detoaster->toastkey);
		MemoryContextSwitchTo(old_mctx);
	}
	else
	{
		Ensure(detoaster->toastrel->rd_id == toast_pointer->va_toastrelid,
			   "unexpected toast pointer relid %d, expected %d",
			   toast_pointer->va_toastrelid,
			   detoaster->toastrel->rd_id);
		detoaster->toastkey.sk_argument = ObjectIdGetDatum(valueid);
		index_rescan(detoaster->toastscan->iscan, &detoaster->toastkey, 1, NULL, 0);
	}

	TupleDesc toasttupDesc = detoaster->toastrel->rd_att;

	///////////////////////////////////////////////

	/*
	 * Read the chunks by index
	 *
	 * The index is on (valueid, chunkidx) so they will come in order
	 */
	const int32 attrsize = VARATT_EXTERNAL_GET_EXTSIZE(*toast_pointer);
	const int32 totalchunks = ((attrsize - 1) / TOAST_MAX_CHUNK_SIZE) + 1;
	const int startchunk = 0;
	const int endchunk = (attrsize - 1) / TOAST_MAX_CHUNK_SIZE;
	Assert(endchunk <= totalchunks);
	HeapTuple ttup;
	int32 expectedchunk = startchunk;
	while ((ttup = systable_getnext_ordered(detoaster->toastscan, ForwardScanDirection)) != NULL)
	{
		int32 curchunk;
		Pointer chunk;
		bool isnull;
		char *chunkdata;
		int32 chunksize;
		int32 expected_size;
		int32 chcpystrt;
		int32 chcpyend;

		/*
		 * Have a chunk, extract the sequence number and the data
		 */
		curchunk = DatumGetInt32(fastgetattr(ttup, 2, toasttupDesc, &isnull));
		Assert(!isnull);
		chunk = DatumGetPointer(fastgetattr(ttup, 3, toasttupDesc, &isnull));
		Assert(!isnull);
		if (!VARATT_IS_EXTENDED(chunk))
		{
			chunksize = VARSIZE(chunk) - VARHDRSZ;
			chunkdata = VARDATA(chunk);
		}
		else if (VARATT_IS_SHORT(chunk))
		{
			/* could happen due to heap_form_tuple doing its thing */
			chunksize = VARSIZE_SHORT(chunk) - VARHDRSZ_SHORT;
			chunkdata = VARDATA_SHORT(chunk);
		}
		else
		{
			/* should never happen */
			elog(ERROR,
				 "found toasted toast chunk for toast value %u in %s",
				 valueid,
				 RelationGetRelationName(detoaster->toastrel));
			chunksize = 0; /* keep compiler quiet */
			chunkdata = NULL;
		}

		/*
		 * Some checks on the data we've found
		 */
		if (curchunk != expectedchunk)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg_internal("unexpected chunk number %d (expected %d) for toast value %u "
									 "in %s",
									 curchunk,
									 expectedchunk,
									 valueid,
									 RelationGetRelationName(detoaster->toastrel))));
		if (curchunk > endchunk)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg_internal("unexpected chunk number %d (out of range %d..%d) for toast "
									 "value %u in %s",
									 curchunk,
									 startchunk,
									 endchunk,
									 valueid,
									 RelationGetRelationName(detoaster->toastrel))));
		expected_size = curchunk < totalchunks - 1 ?
							TOAST_MAX_CHUNK_SIZE :
							attrsize - ((totalchunks - 1) * TOAST_MAX_CHUNK_SIZE);
		if (chunksize != expected_size)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg_internal("unexpected chunk size %d (expected %d) in chunk %d of %d for "
									 "toast value %u in %s",
									 chunksize,
									 expected_size,
									 curchunk,
									 totalchunks,
									 valueid,
									 RelationGetRelationName(detoaster->toastrel))));

		/*
		 * Copy the data into proper place in our result
		 */
		chcpystrt = 0;
		chcpyend = chunksize - 1;
		if (curchunk == startchunk)
			chcpystrt = 0;
		if (curchunk == endchunk)
			chcpyend = (attrsize - 1) % TOAST_MAX_CHUNK_SIZE;

		memcpy(VARDATA(result) + (curchunk * TOAST_MAX_CHUNK_SIZE) + chcpystrt,
			   chunkdata + chcpystrt,
			   (chcpyend - chcpystrt) + 1);

		expectedchunk++;
	}

	/*
	 * Final checks that we successfully fetched the datum
	 */
	if (expectedchunk != (endchunk + 1))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("missing chunk number %d for toast value %u in %s",
								 expectedchunk,
								 valueid,
								 RelationGetRelationName(detoaster->toastrel))));
}

/*
 * The memory context is used to store intermediate data, and is supposed to
 * live over the calls to detoaster_detoast_attr_copy().
 * That function itself can be called in a short-lived memory context.
 */
void
detoaster_init(Detoaster *detoaster, MemoryContext mctx)
{
	detoaster->toastrel = NULL;
	detoaster->mctx = mctx;
}

void
detoaster_close(Detoaster *detoaster)
{
	/* Close toast table */
	if (detoaster->toastrel != NULL)
	{
		systable_endscan_ordered(detoaster->toastscan);
		table_close(detoaster->toastrel, AccessShareLock);
		index_close(detoaster->index, AccessShareLock);
		detoaster->toastrel = NULL;
		detoaster->index = NULL;
	}
}

/*
 * Copy of Postgres' toast_fetch_datum(): Reconstruct an in memory Datum from
 * the chunks saved in the toast relation.
 */
static struct varlena *
ts_toast_fetch_datum(struct varlena *attr, Detoaster *detoaster, MemoryContext dest_mctx)
{
	struct varlena *result;
	struct varatt_external toast_pointer;
	int32 attrsize;

	if (!VARATT_IS_EXTERNAL_ONDISK(attr))
		elog(ERROR, "toast_fetch_datum shouldn't be called for non-ondisk datums");

	/* Must copy to access aligned fields */
	VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);

	attrsize = VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer);

	result = (struct varlena *) MemoryContextAlloc(dest_mctx, attrsize + VARHDRSZ);

	if (TS_VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer))
		SET_VARSIZE_COMPRESSED(result, attrsize + VARHDRSZ);
	else
		SET_VARSIZE(result, attrsize + VARHDRSZ);

	if (attrsize == 0)
		return result; /* Probably shouldn't happen, but just in
						* case. */

	/* Fetch all chunks */
	ts_fetch_toast(detoaster, &toast_pointer, result);

	return result;
}

#include <access/toast_compression.h>

static struct varlena *
ts_toast_decompress_datum(struct varlena *attr)
{
	ToastCompressionId cmid;

	Assert(VARATT_IS_COMPRESSED(attr));

	/*
	 * Fetch the compression method id stored in the compression header and
	 * decompress the data using the appropriate decompression routine.
	 */
	cmid = TOAST_COMPRESS_METHOD(attr);
	switch (cmid)
	{
		case TOAST_PGLZ_COMPRESSION_ID:
			return pglz_decompress_datum(attr);
		case TOAST_LZ4_COMPRESSION_ID:
			return lz4_decompress_datum(attr);
		default:
			elog(ERROR, "invalid compression method id %d", cmid);
			return NULL; /* keep compiler quiet */
	}
}

/*
 * Modification of Postgres' detoast_attr() where we use the stateful Detoaster
 * and skip some cases that don't occur for the toasted compressed data. Even if
 * the data is inline and no detoasting is needed, copies it into the destination
 * memory context.
 */
struct varlena *
detoaster_detoast_attr_copy(struct varlena *attr, Detoaster *detoaster, MemoryContext dest_mctx)
{
	if (!VARATT_IS_EXTENDED(attr))
	{
		/*
		 * This case is unlikely because the compressed data is almost always
		 * toasted and not inline, but we still have to copy the data into the
		 * destination memory context. The source compressed tuple may have
		 * independent unknown lifetime.
		 */
		Size len = VARSIZE(attr);
		struct varlena *result = (struct varlena *) MemoryContextAlloc(dest_mctx, len);
		memcpy(result, attr, len);
		return result;
	}

	if (VARATT_IS_EXTERNAL_ONDISK(attr))
	{
		/*
		 * This is an externally stored datum --- fetch it back from there.
		 */
		attr = ts_toast_fetch_datum(attr, detoaster, dest_mctx);

		/* If it's compressed, decompress it */
		if (VARATT_IS_COMPRESSED(attr))
		{
			struct varlena *tmp = attr;

			MemoryContext old_context = MemoryContextSwitchTo(dest_mctx);
			attr = ts_toast_decompress_datum(tmp);
			MemoryContextSwitchTo(old_context);

			pfree(tmp);
		}

		return attr;
	}

	/*
	 * Can't get indirect TOAST here (out-of-line Datum that's stored in memory),
	 * because we're reading from the compressed chunk table.
	 */
	Ensure(!VARATT_IS_EXTERNAL_INDIRECT(attr), "got indirect TOAST for compressed data");

	/*
	 * Compressed data doesn't have an expanded representation.
	 */
	Ensure(!VARATT_IS_EXTERNAL_EXPANDED(attr), "got expanded TOAST for compressed data");

	if (VARATT_IS_COMPRESSED(attr))
	{
		/*
		 * This is a compressed value stored inline in the main tuple. It rarely
		 * occurs in practice, because we set a low toast_tuple_target = 128
		 * for the compressed chunks, but is still technically possible.
		 *
		 * Note that the attr comes from the compressed tuple slot here, so we
		 * don't have to free it unlike the above case of decompression.
		 */
		MemoryContext old_context = MemoryContextSwitchTo(dest_mctx);
		attr = ts_toast_decompress_datum(attr);
		MemoryContextSwitchTo(old_context);

		return attr;
	}

	/*
	 * The only option left is a short-header varlena --- convert to 4-byte
	 * header format.
	 */
	Ensure(VARATT_IS_SHORT(attr), "got unexpected TOAST type for compressed data");

	/*
	 * Check that the size of datum is not less than the size of header, which
	 * could lead to data_size of UINT64_MAX. This is possible in case of
	 * TOAST data corruption. Postgres doesn't specifically check for this,
	 * because in any case it will be detected by the subsequent palloc call,
	 * but we do it to silence the Coverity warning.
	 */
	CheckCompressedData(VARSIZE_SHORT(attr) >= VARHDRSZ_SHORT);
	Size data_size = VARSIZE_SHORT(attr) - VARHDRSZ_SHORT;
	Size new_size = data_size + VARHDRSZ;
	struct varlena *new_attr;

	new_attr = (struct varlena *) MemoryContextAlloc(dest_mctx, new_size);
	SET_VARSIZE(new_attr, new_size);
	memcpy(VARDATA(new_attr), VARDATA_SHORT(attr), data_size);
	attr = new_attr;

	return attr;
}
