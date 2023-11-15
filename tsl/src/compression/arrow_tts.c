/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/attnum.h>
#include <catalog/pg_attribute.h>
#include <executor/tuptable.h>
#include <utils/expandeddatum.h>

#include "arrow_tts.h"
#include "compression.h"
#include "custom_type_cache.h"
#include "utils/palloc.h"

/*
 * Get a map of attribute offsets that maps non-compressed offsets to
 * compressed offsets.
 *
 * The map is needed since the compressed relation has additional metadata
 * columns that the non-compressed relation doesn't have. When adding new
 * columns, the new column's attribute number will be higher on the compressed
 * relation compared to the regular one.
 */
static const int16 *
arrow_slot_get_attribute_offset_map(TupleTableSlot *slot)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;
	const TupleDesc tupdesc = slot->tts_tupleDescriptor;
	const TupleDesc ctupdesc = aslot->compressed_slot->tts_tupleDescriptor;

	if (NULL == aslot->attrs_offset_map)
	{
		MemoryContext oldmcxt = MemoryContextSwitchTo(slot->tts_mcxt);
		aslot->attrs_offset_map = build_attribute_offset_map(tupdesc, ctupdesc, NULL);
		MemoryContextSwitchTo(oldmcxt);
	}

	return aslot->attrs_offset_map;
}

static void
tts_arrow_init(TupleTableSlot *slot)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;
	aslot->arrow_columns = NULL;
	aslot->compressed_slot = NULL;
	aslot->segmentby_columns = NULL;
	aslot->decompression_mcxt = AllocSetContextCreate(slot->tts_mcxt,
													  "bulk decompression",
													  /* minContextSize = */ 0,
													  /* initBlockSize = */ 64 * 1024,
													  /* maxBlockSize = */ 64 * 1024);
}

static void
tts_arrow_release(TupleTableSlot *slot)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;

	if (NULL != aslot->arrow_columns)
	{
		aslot->arrow_columns = NULL;
	}
}

static void
tts_arrow_clear(TupleTableSlot *slot)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;

	if (unlikely(TTS_SHOULDFREE(slot)))
	{
		/* The tuple is materialized, so free materialized memory */
	}

	if (aslot->arrow_columns)
	{
		for (int i = 0; i < slot->tts_tupleDescriptor->natts; i++)
		{
			ArrowArray *arr = aslot->arrow_columns[i];

			if (arr)
			{
				pfree(arr);
			}

			aslot->arrow_columns[i] = NULL;
		}

		pfree(aslot->arrow_columns);
		aslot->arrow_columns = NULL;
	}

	aslot->compressed_slot = NULL;

	slot->tts_nvalid = 0;
	slot->tts_flags |= TTS_FLAG_EMPTY;
	ItemPointerSetInvalid(&slot->tts_tid);
}

static inline void
tts_arrow_store_tuple(TupleTableSlot *slot, TupleTableSlot *compressed_slot, uint16 tuple_index)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;

	slot->tts_flags &= ~TTS_FLAG_EMPTY;
	aslot->compressed_slot = compressed_slot;
	aslot->tuple_index = tuple_index;
	tid_to_compressed_tid(&slot->tts_tid, &compressed_slot->tts_tid, tuple_index);
	Assert(!TTS_EMPTY(aslot->compressed_slot));
}

TupleTableSlot *
ExecStoreArrowTuple(TupleTableSlot *slot, TupleTableSlot *compressed_slot, uint16 tuple_index)
{
	Assert(slot != NULL);
	Assert(slot->tts_tupleDescriptor != NULL);
	Assert(!TTS_EMPTY(compressed_slot));

	if (unlikely(!TTS_IS_ARROWTUPLE(slot)))
		elog(ERROR, "trying to store an on-disk parquet tuple into wrong type of slot");

	ExecClearTuple(slot);
	tts_arrow_store_tuple(slot, compressed_slot, tuple_index);

	Assert(!TTS_EMPTY(slot));
	Assert(!TTS_SHOULDFREE(slot));

	return slot;
}

TupleTableSlot *
ExecStoreArrowTupleExisting(TupleTableSlot *slot, uint16 tuple_index)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));
	aslot->tuple_index = tuple_index;
	slot->tts_tid.ip_blkid.bi_hi = aslot->tuple_index;
	slot->tts_nvalid = 0;

	return slot;
}

/*
 * Materialize an Arrow slot.
 */
static void
tts_arrow_materialize(TupleTableSlot *slot)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));

	TTSOpsBufferHeapTuple.materialize(slot);

	if (aslot->compressed_slot)
		ExecMaterializeSlot(aslot->compressed_slot);
}

static bool
is_compressed_col(const TupleDesc tupdesc, AttrNumber attno)
{
	static CustomTypeInfo *typinfo = NULL;
	Oid coltypid = tupdesc->attrs[AttrNumberGetAttrOffset(attno)].atttypid;

	if (typinfo == NULL)
		typinfo = ts_custom_type_cache_get(CUSTOM_TYPE_COMPRESSED_DATA);

	return coltypid == typinfo->type_oid;
}

static void
tts_arrow_getsomeattrs(TupleTableSlot *slot, int natts)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;
	const TupleDesc tupdesc = slot->tts_tupleDescriptor;
	const TupleDesc compressed_tupdesc = aslot->compressed_slot->tts_tupleDescriptor;
	const int16 *attrs_map;
	int16 cnatts = -1;

	if (natts < 1 || natts > slot->tts_tupleDescriptor->natts)
		elog(ERROR, "invalid number of attributes requested");

	attrs_map = arrow_slot_get_attribute_offset_map(slot);

	/* Find highest attribute number/offset in compressed relation */
	for (int i = 0; i < natts; i++)
	{
		int16 coff = attrs_map[AttrNumberGetAttrOffset(natts)];

		if (AttrOffsetGetAttrNumber(coff) > cnatts)
			cnatts = AttrOffsetGetAttrNumber(coff);
	}

	Assert(cnatts > 0);

	slot_getsomeattrs(aslot->compressed_slot, cnatts);

	if (NULL == aslot->arrow_columns)
	{
		aslot->arrow_columns =
			MemoryContextAllocZero(slot->tts_mcxt,
								   sizeof(ArrowArray *) * slot->tts_tupleDescriptor->natts);
	}

	for (int i = 0; i < natts; i++)
	{
		const int16 cattoff = attrs_map[i];
		const AttrNumber attno = AttrOffsetGetAttrNumber(i);
		const AttrNumber cattno = AttrOffsetGetAttrNumber(cattoff);

		/* Decompress the column if not already done. */
		if (aslot->arrow_columns[i] == NULL)
		{
			if (is_compressed_col(compressed_tupdesc, cattno))
			{
				bool isnull;
				Datum value = slot_getattr(aslot->compressed_slot, cattno, &isnull);

				if (isnull)
				{
					// do nothing
				}
				else
				{
					const Form_pg_attribute attr = &tupdesc->attrs[i];
					const CompressedDataHeader *header =
						(CompressedDataHeader *) PG_DETOAST_DATUM(value);
					DecompressAllFunction decompress_all =
						tsl_get_decompress_all_function(header->compression_algorithm,
														attr->atttypid);
					Assert(decompress_all != NULL);
					MemoryContext oldcxt = MemoryContextSwitchTo(aslot->decompression_mcxt);
					aslot->arrow_columns[i] =
						decompress_all(PointerGetDatum(header),
									   slot->tts_tupleDescriptor->attrs[i].atttypid,
									   slot->tts_mcxt);
					MemoryContextReset(aslot->decompression_mcxt);
					MemoryContextSwitchTo(oldcxt);
				}
			}
			else
			{
				/* Since we are looping over the attributes of the
				 * non-compressed slot, we will either see only compressed
				 * columns or the segment-by column. If the column is not
				 * compressed, it must be the segment-by columns. The
				 * segment-by column is not compressed and the value is the
				 * same for all rows in the compressed tuple. */
				aslot->arrow_columns[i] = NULL;
				slot->tts_values[i] =
					slot_getattr(aslot->compressed_slot, cattno, &slot->tts_isnull[i]);

				/* Remember the segment-by column */
				MemoryContext oldcxt = MemoryContextSwitchTo(slot->tts_mcxt);
				aslot->segmentby_columns = bms_add_member(aslot->segmentby_columns, attno);
				MemoryContextSwitchTo(oldcxt);
			}
		}

		/* At this point the column should be decompressed, if it is a
		 * compressed column. */
		if (bms_is_member(attno, aslot->segmentby_columns))
		{
			/* Segment-by column. Value already set. */
		}
		else if (aslot->arrow_columns[i] == NULL)
		{
			/* Since the column is not the segment-by column, and there is no
			 * decompressed data, the column must be NULL. Use the default
			 * value. */
			slot->tts_values[i] = getmissingattr(slot->tts_tupleDescriptor,
												 AttrOffsetGetAttrNumber(i),
												 &slot->tts_isnull[i]);
		}
		else
		{
			const char *restrict values = aslot->arrow_columns[i]->buffers[1];
			const uint64 *restrict validity = aslot->arrow_columns[i]->buffers[0];
			int16 value_bytes = get_typlen(slot->tts_tupleDescriptor->attrs[i].atttypid);

			/*
			 * The conversion of Datum to more narrow types will truncate
			 * the higher bytes, so we don't care if we read some garbage
			 * into them, and can always read 8 bytes. These are unaligned
			 * reads, so technically we have to do memcpy.
			 */
			uint64 value;
			memcpy(&value, &values[value_bytes * (aslot->tuple_index - 1)], 8);

#ifdef USE_FLOAT8_BYVAL
			Datum datum = Int64GetDatum(value);
#else
			/*
			 * On 32-bit systems, the data larger than 4 bytes go by
			 * reference, so we have to jump through these hoops.
			 */
			Datum datum;
			if (value_bytes <= 4)
			{
				datum = Int32GetDatum((uint32) value);
			}
			else
			{
				datum = Int64GetDatum(value);
			}
#endif
			slot->tts_values[i] = datum;
			slot->tts_isnull[i] = !arrow_row_is_valid(validity, (aslot->tuple_index - 1));
		}
	}

	slot->tts_nvalid = natts;
}

/*
 */
static Datum
tts_arrow_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
	Assert(!TTS_EMPTY(slot));

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot retrieve a system column in this context")));

	return 0; /* silence compiler warnings */
}

static void
tts_arrow_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
	tts_arrow_clear(dstslot);
	slot_getallattrs(srcslot);

	dstslot->tts_flags &= ~TTS_FLAG_EMPTY;

	/* make sure storage doesn't depend on external memory */
	tts_arrow_materialize(dstslot);
}

static HeapTuple
tts_arrow_copy_heap_tuple(TupleTableSlot *slot)
{
	HeapTuple tuple;

	Assert(!TTS_EMPTY(slot));

	tts_arrow_materialize(slot);
	tuple = heap_form_tuple(slot->tts_tupleDescriptor, slot->tts_values, slot->tts_isnull);
	ItemPointerCopy(&slot->tts_tid, &tuple->t_self);

	return tuple;
}

static MinimalTuple
tts_arrow_copy_minimal_tuple(TupleTableSlot *slot)
{
	Assert(!TTS_EMPTY(slot));
	tts_arrow_materialize(slot);

	return heap_form_minimal_tuple(slot->tts_tupleDescriptor, slot->tts_values, slot->tts_isnull);
}

void
tts_arrow_set_heaptuple_mode(TupleTableSlot *slot)
{
	TupleTableSlotOps **ops = (TupleTableSlotOps **) &slot->tts_ops;
	*ops = (TupleTableSlotOps *) &TTSOpsBufferHeapTuple;
}

const TupleTableSlotOps TTSOpsArrowTuple = { .base_slot_size = sizeof(ArrowTupleTableSlot),
											 .init = tts_arrow_init,
											 .release = tts_arrow_release,
											 .clear = tts_arrow_clear,
											 .getsomeattrs = tts_arrow_getsomeattrs,
											 .getsysattr = tts_arrow_getsysattr,
											 .materialize = tts_arrow_materialize,
											 .copyslot = tts_arrow_copyslot,
											 .get_heap_tuple = NULL,
											 .get_minimal_tuple = NULL,
											 .copy_heap_tuple = tts_arrow_copy_heap_tuple,
											 .copy_minimal_tuple = tts_arrow_copy_minimal_tuple };
