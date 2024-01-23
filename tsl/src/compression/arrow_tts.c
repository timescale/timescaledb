/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/attnum.h>
#include <catalog/pg_attribute.h>
#include <executor/tuptable.h>
#include <storage/itemptr.h>

#include "arrow_cache.h"
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
const int16 *
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

/*
 * The init function is called by:
 *
 * - MakeTupletableslot()
 *
 */
static void
tts_arrow_init(TupleTableSlot *slot)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;
	MemoryContext oldmctx;

	aslot->arrow_columns = NULL;
	aslot->segmentby_columns = NULL;
	aslot->valid_columns = NULL;
	aslot->tuple_index = InvalidTupleIndex;
	aslot->total_row_count = 0;
	aslot->decompression_mcxt = AllocSetContextCreate(slot->tts_mcxt,
													  "bulk decompression",
													  /* minContextSize = */ 0,
													  /* initBlockSize = */ 64 * 1024,
													  /* maxBlockSize = */ 64 * 1024);

	/*
	 * Set up child slots, one for the non-compressed relation and one for the
	 * compressed relation.
	 *
	 * It is only possible to initialize the non-compressed child slot here
	 * because its tuple descriptor matches the descriptor of the
	 * slot. However, the compressed relation has a different tuple descriptor
	 * that is unknown at this point, and therefore defer initialization of
	 * the compressed child slot until the tuple descriptor is known.
	 */
	aslot->compressed_slot = NULL;
	oldmctx = MemoryContextSwitchTo(slot->tts_mcxt);
	aslot->noncompressed_slot =
		MakeSingleTupleTableSlot(slot->tts_tupleDescriptor, &TTSOpsBufferHeapTuple);
	aslot->child_slot = aslot->noncompressed_slot;
	MemoryContextSwitchTo(oldmctx);
	ItemPointerSetInvalid(&slot->tts_tid);

	arrow_column_cache_init(&aslot->arrow_cache, slot->tts_mcxt);
}

/*
 * The release function is called by:
 *
 * - ExecDropsingletupletableslot()
 * - ExecResetTupleTable()
 *
 * Should only release resources not known and released by these functions.
 */
static void
tts_arrow_release(TupleTableSlot *slot)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;

	arrow_column_cache_release(&aslot->arrow_cache);

	MemoryContextDelete(aslot->decompression_mcxt);
	ExecDropSingleTupleTableSlot(aslot->noncompressed_slot);

	/* compressed slot was lazily initialized */
	if (NULL != aslot->compressed_slot)
		ExecDropSingleTupleTableSlot(aslot->compressed_slot);

	/* Do we need these? The slot is being released after all. */
	aslot->arrow_columns = NULL;
	aslot->compressed_slot = NULL;
	aslot->noncompressed_slot = NULL;
}

/*
 * Clear only parent, but not child slots.
 */
static void
clear_arrow_parent(TupleTableSlot *slot)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;

	Assert(TTS_IS_ARROWTUPLE(slot));

	aslot->arrow_columns = NULL;
	slot->tts_nvalid = 0;
	slot->tts_flags |= TTS_FLAG_EMPTY;
	ItemPointerSetInvalid(&slot->tts_tid);
}

/*
 * Called by ExecClearTuple().
 *
 * Note that ExecClearTuple() doesn't do any work itself, so need to handle
 * all the clearing of the base slot implementation.
 */
static void
tts_arrow_clear(TupleTableSlot *slot)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;

	/* Clear child slots */
	if (aslot->compressed_slot)
		ExecClearTuple(aslot->compressed_slot);

	ExecClearTuple(aslot->noncompressed_slot);

	/* Clear parent */
	clear_arrow_parent(slot);
}

static inline void
tts_arrow_store_tuple(TupleTableSlot *slot, TupleTableSlot *child_slot, uint16 tuple_index)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(child_slot));

	if (tuple_index == InvalidTupleIndex)
	{
		clear_arrow_parent(slot);
		ItemPointerCopy(&child_slot->tts_tid, &slot->tts_tid);
		/* Stored a non-compressed tuple so clear the compressed slot */
		if (NULL != aslot->compressed_slot)
			ExecClearTuple(aslot->compressed_slot);

		/* Total row count is always 1 for a regular (non-compressed) tuple */
		aslot->total_row_count = 1;
	}
	else
	{
		bool isnull;

		/* The slot could already hold the decompressed data for the index we
		 * are storing, so only clear the decompressed data if this is truly a
		 * new compressed tuple being stored. */
		if (ItemPointerIsValid(&slot->tts_tid))
		{
			/* If the existing tuple is not a compressed tuple, then it cannot
			 * be the same tuple */
			if (!is_compressed_tid(&slot->tts_tid))
				clear_arrow_parent(slot);
			else
			{
				/* The existing tuple in the slot is a compressed tuple. Need
				 * to compare TIDs to identify if we are storing the same
				 * compressed tuple again, just with a new tuple index */
				ItemPointerData decoded_tid;

				compressed_tid_to_tid(&decoded_tid, &slot->tts_tid);

				if (!ItemPointerEquals(&decoded_tid, &child_slot->tts_tid))
					clear_arrow_parent(slot);
			}
		}

		tid_to_compressed_tid(&slot->tts_tid, &child_slot->tts_tid, tuple_index);
		/* Stored a compressed tuple so clear the non-compressed slot */
		ExecClearTuple(aslot->noncompressed_slot);

		/* Set total row count */
		aslot->total_row_count = slot_getattr(child_slot, aslot->count_attnum, &isnull);
		Assert(!isnull);
	}

	slot->tts_flags &= ~TTS_FLAG_EMPTY;
	slot->tts_nvalid = 0;

	if (aslot->valid_columns != NULL)
	{
		pfree(aslot->valid_columns);
		aslot->valid_columns = NULL;
	}
	aslot->child_slot = child_slot;
	aslot->tuple_index = tuple_index;
}

TupleTableSlot *
ExecStoreArrowTuple(TupleTableSlot *slot, uint16 tuple_index)
{
	TupleTableSlot *child_slot;

	Assert(slot != NULL);
	Assert(slot->tts_tupleDescriptor != NULL);

	if (unlikely(!TTS_IS_ARROWTUPLE(slot)))
		elog(ERROR, "trying to store an on-disk arrow tuple into wrong type of slot");

	if (tuple_index == InvalidTupleIndex)
		child_slot = arrow_slot_get_noncompressed_slot(slot);
	else
		child_slot = arrow_slot_get_compressed_slot(slot, NULL);

	if (unlikely(TTS_EMPTY(child_slot)))
		elog(ERROR, "trying to store an empty tuple in an arrow slot");

	/* A child slot already stores the data, so only clear the parent
	 * metadata. The task here is only to mark the parent as storing a tuple,
	 * which is already in one of the children. */
	tts_arrow_store_tuple(slot, child_slot, tuple_index);
	Assert(!TTS_EMPTY(slot));

	return slot;
}

static void
copy_slot_values(const TupleTableSlot *from, TupleTableSlot *to, int natts)
{
	Assert(!TTS_EMPTY(from));

	for (int i = 0; i < natts; i++)
	{
		to->tts_values[i] = from->tts_values[i];
		to->tts_isnull[i] = from->tts_isnull[i];
	}

	to->tts_flags &= ~TTS_FLAG_EMPTY;
	to->tts_nvalid = natts;
}

/*
 * Materialize an Arrow slot.
 *
 * The data to materialize can be stored in either the parent or a child,
 * i.e., there are three cases:
 *
 * 1. Values stored in parent tts_values[]
 * 2. Values stored in non-compressed child slot
 * 3. Values stored in compressed child slot (note different tuple descriptor)
 *
 * If the child slots are empty, any data must be stored in the parent. This
 * can happen if the slot is used to store transient data, e.g., in an agg
 * node. In that case, the slot is used more as a VirtualTupleTableSlot.
 *
 * In all cases above, keep the invariant that data is always materialized in
 * a child slot. So, if the source data is in the parent's tts_values[] array,
 * first copy it to the non-compressed child slot and materialize it there
 * instead.
 */
static void
tts_arrow_materialize(TupleTableSlot *slot)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));

	/* Check if the source data is in the parent tts_values[] (i.e., both
	 * children are empty). Copy it from the parent to the non-compressed
	 * child and materialize it there. */
	if (TTS_EMPTY(aslot->noncompressed_slot) &&
		(aslot->compressed_slot == NULL || TTS_EMPTY(aslot->compressed_slot)))
	{
		Assert(aslot->child_slot == aslot->noncompressed_slot);
		copy_slot_values(slot, aslot->noncompressed_slot, slot->tts_nvalid);
	}

	Assert(!TTS_EMPTY(aslot->child_slot));
	ExecMaterializeSlot(aslot->child_slot);
	/* The data is now materialized in a child, so need to refetch it from
	 * there into the parent tts_values[] via getsomeattrs() */
	slot->tts_nvalid = 0;
}

bool
is_compressed_col(const TupleDesc tupdesc, AttrNumber attno)
{
	static CustomTypeInfo *typinfo = NULL;
	Oid coltypid = tupdesc->attrs[AttrNumberGetAttrOffset(attno)].atttypid;

	if (typinfo == NULL)
		typinfo = ts_custom_type_cache_get(CUSTOM_TYPE_COMPRESSED_DATA);

	return coltypid == typinfo->type_oid;
}

static void
set_attr_value(TupleTableSlot *slot, AttrNumber attno)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;
	const int16 *attrs_map = arrow_slot_get_attribute_offset_map(slot);
	const int16 attoff = AttrNumberGetAttrOffset(attno);
	const int16 cattoff = attrs_map[attoff];
	const AttrNumber cattno = AttrOffsetGetAttrNumber(cattoff);

	/* Check if value is already set */
	if (bms_is_member(attno, aslot->valid_columns))
		return;

	if (bms_is_member(attno, aslot->segmentby_columns))
	{
		/* Segment-by column. Value is not compressed so get directly from
		 * child slot. */
		slot->tts_values[attoff] =
			slot_getattr(aslot->child_slot, cattno, &slot->tts_isnull[attoff]);
	}
	else if (aslot->arrow_columns[attoff] == NULL)
	{
		/* Since the column is not the segment-by column, and there is no
		 * decompressed data, the column must be NULL. Use the default
		 * value. */
		slot->tts_values[attoff] =
			getmissingattr(slot->tts_tupleDescriptor, attno, &slot->tts_isnull[attoff]);
	}
	else
	{
		const char *restrict values = aslot->arrow_columns[attoff]->buffers[1];
		const uint64 *restrict validity = aslot->arrow_columns[attoff]->buffers[0];
		int16 value_bytes = get_typlen(slot->tts_tupleDescriptor->attrs[attoff].atttypid);

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
		Datum datum = (value_bytes <= 4) ? Int32GetDatum((uint32) value) : Int64GetDatum(value);
#endif
		slot->tts_values[attoff] = datum;
		slot->tts_isnull[attoff] = !arrow_row_is_valid(validity, (aslot->tuple_index - 1));
	}

	MemoryContext oldmcxt = MemoryContextSwitchTo(slot->tts_mcxt);
	aslot->valid_columns = bms_add_member(aslot->valid_columns, attno);
	MemoryContextSwitchTo(oldmcxt);
}

static void
tts_arrow_getsomeattrs(TupleTableSlot *slot, int attnum)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;
	const int16 *attrs_map;
	int cattnum = -1;

	if (attnum < 1 || attnum > slot->tts_tupleDescriptor->natts)
		elog(ERROR, "invalid number of attributes requested");

	/* Check if attributes are already retrieved */
	if (attnum <= slot->tts_nvalid)
		return;

	if (aslot->tuple_index == InvalidTupleIndex)
	{
		slot_getsomeattrs(aslot->child_slot, attnum);

		/* The child slot points to an non-compressed tuple, so just copy over
		 * the values from the child. */
		copy_slot_values(aslot->child_slot, slot, attnum);
		return;
	}

	attrs_map = arrow_slot_get_attribute_offset_map(slot);

	/* Find highest attribute number/offset in compressed relation in order to
	 * make slot_getsomeattrs() get all the required attributes in the
	 * compressed tuple. */
	for (int i = 0; i < attnum; i++)
	{
		int16 coff = attrs_map[AttrNumberGetAttrOffset(attnum)];

		if (AttrOffsetGetAttrNumber(coff) > cattnum)
			cattnum = AttrOffsetGetAttrNumber(coff);
	}

	Assert(cattnum > 0);
	slot_getsomeattrs(aslot->child_slot, cattnum);

	/* The child slot points to a compressed tuple, so read the tuple. */
	ArrowColumnCacheEntry *restrict entry = arrow_column_cache_read(aslot, attnum);

	/* Copy over cached data references to the slot */
	aslot->segmentby_columns = entry->segmentby_columns;
	aslot->arrow_columns = entry->arrow_columns;

	/* Build the non-compressed tuple values array from the cached data. */
	for (int i = 0; i < attnum; i++)
		set_attr_value(slot, AttrOffsetGetAttrNumber(i));

	slot->tts_nvalid = attnum;
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

/*
 * Copy a slot tuple to an arrow slot.
 *
 * Note that the source slot could be another arrow slot or another slot
 * implementation altogether (e.g., virtual).
 *
 * If the source slot is also an arrow slot, just copy the child slots and
 * parent state.
 *
 * If the source slot is not an arrow slot, then copy the source slot directly
 * into the matching child slot (either non-compressed or compressed child
 * slot) and update the parent slot metadata.
 */
static void
tts_arrow_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
	ArrowTupleTableSlot *adstslot = (ArrowTupleTableSlot *) dstslot;
	TupleTableSlot *child_dstslot = NULL;
	TupleTableSlot *child_srcslot = NULL;

	Assert(TTS_IS_ARROWTUPLE(dstslot));
	ExecClearTuple(dstslot);

	/* Check if copying from another slot implementation */
	if (dstslot->tts_ops != srcslot->tts_ops)
	{
		child_srcslot = srcslot;

		/*
		 * The source slot is not an Arrow slot so it is necessary to identify
		 * which destination child slot to copy the source slot into. It
		 * should normally be the non-compressed slot, but double check the
		 * number of attributes to sure. If the source and the target tuple
		 * descriptor has the same number of attributes, then it should a
		 * non-compressed slot since the compressed slot has extra metadata
		 * attributes. If the compressed table is changed in the future to not
		 * have extra metadata attributes, this check needs to be updated.
		 *
		 * Note that it is not possible to use equalTupleDescs() because it
		 * compares the tuple's composite ID. If the source slot is, e.g.,
		 * virtual, with no connection to a physical relation, the composite
		 * ID is often RECORDID while the arrow slot has the ID of the
		 * relation.
		 */
		if (dstslot->tts_tupleDescriptor->natts == srcslot->tts_tupleDescriptor->natts)
		{
			/* non-compressed tuple slot */
			child_dstslot = arrow_slot_get_noncompressed_slot(dstslot);
			adstslot->tuple_index = InvalidTupleIndex;
		}
		else
		{
			child_dstslot = arrow_slot_get_compressed_slot(dstslot, srcslot->tts_tupleDescriptor);
			/* compressed tuple slot */
			adstslot->tuple_index = 1;
		}
	}
	else
	{
		/* The source slot is also an arrow slot. */

		/* NOTE: in practice, there might not be a case for copying one arrow
		 * slot to another, so should consider just throwing en error. */
		ArrowTupleTableSlot *asrcslot = (ArrowTupleTableSlot *) srcslot;

		if (!TTS_EMPTY(asrcslot->noncompressed_slot))
		{
			child_srcslot = asrcslot->noncompressed_slot;
			child_dstslot = arrow_slot_get_noncompressed_slot(dstslot);
		}
		else
		{
			Assert(!TTS_EMPTY(asrcslot->compressed_slot));
			child_srcslot = asrcslot->compressed_slot;
			child_dstslot = arrow_slot_get_compressed_slot(dstslot, srcslot->tts_tupleDescriptor);
		}

		/*
		 * Since the lifetime of the new slot might be different from the
		 * lifetime of the old slot, the new slot will have its own empty
		 * arrow cache. This might be less efficient than sharing the cache
		 * since the new slot copy might have to decompress again, but this
		 * can be optimized further when necessary.
		 */

		adstslot->tuple_index = asrcslot->tuple_index;
		ItemPointerCopy(&srcslot->tts_tid, &dstslot->tts_tid);
	}

	ExecClearTuple(child_dstslot);
	ExecCopySlot(child_dstslot, child_srcslot);
	adstslot->child_slot = child_dstslot;
	dstslot->tts_flags &= ~TTS_FLAG_EMPTY;
	dstslot->tts_nvalid = 0;
}

/*
 * Produce a HeapTuple copy from the slot Datum values.
 *
 * Exploit the non-compressed child slot to achieve this functionality since
 * it has the "right" tuple descriptor and will produce the correct tuple.
 */
static HeapTuple
tts_arrow_copy_heap_tuple(TupleTableSlot *slot)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));
	/* Make sure the child slot has the parent's datums or otherwise it won't
	 * have any data to produce the tuple from */
	slot_getallattrs(slot);
	copy_slot_values(slot, aslot->noncompressed_slot, slot->tts_tupleDescriptor->natts);
	return ExecCopySlotHeapTuple(aslot->noncompressed_slot);
}

/*
 * Produce a Minimal tuple copy from the slot Datum values.
 *
 * Exploit the non-compressed child slot to achieve this functionality since
 * it has the "right" tuple descriptor and will produce the correct tuple.
 */
static MinimalTuple
tts_arrow_copy_minimal_tuple(TupleTableSlot *slot)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));
	/* Make sure the child slot has the parent's datums or otherwise it won't
	 * have any data to produce the tuple from */
	slot_getallattrs(slot);
	copy_slot_values(slot, aslot->noncompressed_slot, slot->tts_tupleDescriptor->natts);
	return ExecCopySlotMinimalTuple(aslot->noncompressed_slot);
}

const ArrowArray *
arrow_slot_get_array(TupleTableSlot *slot, AttrNumber attno)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;
	int attoff = AttrNumberGetAttrOffset(attno);

	Assert(TTS_IS_ARROWTUPLE(slot));

	if (attno > slot->tts_tupleDescriptor->natts)
		elog(ERROR, "invalid attribute number");

	/* Regular (non-compressed) tuple has no arrow array */
	if (aslot->tuple_index == InvalidTupleIndex)
	{
		slot_getsomeattrs(slot, attno);
		return NULL;
	}

	if (aslot->arrow_columns == NULL)
	{
		ArrowColumnCacheEntry *restrict entry = arrow_column_cache_read(aslot, attno);

		aslot->segmentby_columns = entry->segmentby_columns;
		aslot->arrow_columns = entry->arrow_columns;
		set_attr_value(slot, attno);
	}

	return aslot->arrow_columns[attoff];
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
