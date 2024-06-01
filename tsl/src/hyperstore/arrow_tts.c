/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/attnum.h>
#include <access/tupdesc.h>
#include <catalog/pg_attribute.h>
#include <executor/execdebug.h>
#include <executor/tuptable.h>
#include <storage/itemptr.h>

#include "arrow_array.h"
#include "arrow_cache.h"
#include "arrow_tts.h"
#include "compression/compression.h"
#include "compression/create.h"
#include "custom_type_cache.h"
#include "hyperstore_handler.h"
#include "utils/palloc.h"

Datum
tsl_is_compressed_tid(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(is_compressed_tid((ItemPointer) PG_GETARG_POINTER(0)));
}

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
	Oid relid =
		OidIsValid(slot->tts_tableOid) ? slot->tts_tableOid : TupleDescAttr(tupdesc, 0)->attrelid;

	if (aslot->attrs_offset_map)
		return aslot->attrs_offset_map;

	Ensure(OidIsValid(relid), "invalid relation for ArrowTupleTableSlot");

	aslot->attrs_offset_map =
		MemoryContextAllocZero(slot->tts_mcxt, sizeof(int16) * tupdesc->natts);

	/* Get the mappings from the relation cache, but put them in the slot's
	 * memory context since the cache might become invalidated and rebuilt. */
	const Relation rel = RelationIdGetRelation(relid);
	const HyperstoreInfo *hsinfo = RelationGetHyperstoreInfo(rel);

	for (int i = 0; i < hsinfo->num_columns; i++)
	{
		if (hsinfo->columns[i].is_dropped)
		{
			Assert(TupleDescAttr(tupdesc, i)->attisdropped);
			aslot->attrs_offset_map[i] = -1;
		}
		else
		{
			Assert(hsinfo->columns[i].attnum == TupleDescAttr(tupdesc, i)->attnum);
			Assert(hsinfo->columns[i].cattnum != InvalidAttrNumber);
			aslot->attrs_offset_map[i] = AttrNumberGetAttrOffset(hsinfo->columns[i].cattnum);
		}
	}

	RelationClose(rel);

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

	aslot->segmentby_attrs = NULL;
	aslot->valid_attrs = NULL;
	aslot->attrs_offset_map = NULL;
	aslot->tuple_index = InvalidTupleIndex;
	aslot->total_row_count = 0;
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
	TS_WITH_MEMORY_CONTEXT(slot->tts_mcxt, {
		aslot->noncompressed_slot =
			MakeSingleTupleTableSlot(slot->tts_tupleDescriptor, &TTSOpsBufferHeapTuple);
		aslot->child_slot = aslot->noncompressed_slot;
	});
	ItemPointerSetInvalid(&slot->tts_tid);

	arrow_column_cache_init(&aslot->arrow_cache, slot->tts_mcxt);

	Assert(TTS_EMPTY(slot));
	Assert(TTS_EMPTY(aslot->noncompressed_slot));
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

	ExecDropSingleTupleTableSlot(aslot->noncompressed_slot);

	/* compressed slot was lazily initialized */
	if (NULL != aslot->compressed_slot)
		ExecDropSingleTupleTableSlot(aslot->compressed_slot);

	/* Do we need these? The slot is being released after all. */
	aslot->compressed_slot = NULL;
	aslot->noncompressed_slot = NULL;
}

static Bitmapset *
build_segmentby_attrs(TupleTableSlot *slot)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;
	const TupleDesc tupdesc = slot->tts_tupleDescriptor;
	const TupleDesc ctupdesc = aslot->compressed_slot->tts_tupleDescriptor;
	const int16 *attrs_offset_map = arrow_slot_get_attribute_offset_map(slot);
	Bitmapset *segmentby_attrs = NULL;

	/*
	 * Populate the segmentby bitmap with information about *all* attributes in
	 * the non-compressed version of the tuple. That way we do not have to
	 * update this field if we start fetching more attributes.
	 */
	for (int i = 0; i < tupdesc->natts; i++)
	{
		if (!TupleDescAttr(tupdesc, i)->attisdropped)
		{
			const AttrNumber attno = AttrOffsetGetAttrNumber(i);
			const AttrNumber cattno = AttrOffsetGetAttrNumber(attrs_offset_map[i]);

			if (!is_compressed_col(ctupdesc, cattno))
				segmentby_attrs = bms_add_member(segmentby_attrs, attno);
		}
	}

	return segmentby_attrs;
}

/*
 * Get the child slot for compressed data.
 *
 * Since the tuple format (as provided by the tuple descriptor) is different
 * from that of the (non-compressed) parent slot it is necessary to provide a
 * tuple descriptor for the compressed relation if the slot has not yet been
 * initialized. The compressed tuple descriptor is, unfortunately, not known
 * at time of the initialization of the parent, so it needs lazy
 * initialization.
 */
TupleTableSlot *
arrow_slot_get_compressed_slot(TupleTableSlot *slot, const TupleDesc tupdesc)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;

	Assert(TTS_IS_ARROWTUPLE(slot));

	if (NULL == aslot->compressed_slot)
	{
		MemoryContext oldmctx;

		if (NULL == tupdesc)
			elog(ERROR, "cannot make compressed table slot without tuple descriptor");

		oldmctx = MemoryContextSwitchTo(slot->tts_mcxt);
		aslot->compressed_slot = MakeSingleTupleTableSlot(tupdesc, &TTSOpsBufferHeapTuple);
		/* Set total row count */

		aslot->count_attnum = InvalidAttrNumber;

		for (int i = 0; i < tupdesc->natts; i++)
		{
			const Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

			if (namestrcmp(&attr->attname, COMPRESSION_COLUMN_METADATA_COUNT_NAME) == 0)
			{
				aslot->count_attnum = attr->attnum;
				break;
			}
		}

		Ensure(aslot->count_attnum != InvalidAttrNumber,
			   "missing count metadata in compressed relation");

		/* Build a bitmap for segmentby columns/attributes */
		aslot->segmentby_attrs = build_segmentby_attrs(slot);
		MemoryContextSwitchTo(oldmctx);
	}

	return aslot->compressed_slot;
}

/*
 * Clear only parent, but not child slots.
 */
static void
clear_arrow_parent(TupleTableSlot *slot)
{
	Assert(TTS_IS_ARROWTUPLE(slot));

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
	Assert(OidIsValid(slot->tts_tableOid));

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

	if (aslot->valid_attrs != NULL)
	{
		pfree(aslot->valid_attrs);
		aslot->valid_attrs = NULL;
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
set_attr_value(TupleTableSlot *slot, ArrowArray **arrow_arrays, const AttrNumber attnum)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;
	const int16 *attrs_offset_map = arrow_slot_get_attribute_offset_map(slot);
	const int16 attoff = AttrNumberGetAttrOffset(attnum);
	const int16 cattoff = attrs_offset_map[attoff]; /* offset in compressed tuple */
	const AttrNumber cattnum = AttrOffsetGetAttrNumber(cattoff);

	TS_DEBUG_LOG("attnum: %d, cattnum: %d, valid: %s, segmentby: %s, array: %s",
				 attnum,
				 cattnum,
				 yes_no(bms_is_member(attnum, aslot->valid_attrs)),
				 yes_no(bms_is_member(attnum, aslot->segmentby_attrs)),
				 yes_no(arrow_arrays[attoff]));

	/* Nothing to do for dropped attribute */
	if (attrs_offset_map[attoff] == -1)
	{
		Assert(TupleDescAttr(slot->tts_tupleDescriptor, attoff)->attisdropped);
		return;
	}

	/* Check if value is already set */
	if (bms_is_member(attnum, aslot->valid_attrs))
		return;

	if (bms_is_member(attnum, aslot->segmentby_attrs))
	{
		/* Segment-by column. Value is not compressed so get directly from
		 * child slot. */
		slot->tts_values[attoff] =
			slot_getattr(aslot->child_slot, cattnum, &slot->tts_isnull[attoff]);
	}
	else if (arrow_arrays[attoff] == NULL)
	{
		/* Since the column is not the segment-by column, and there is no
		 * decompressed data, the column must be NULL. Use the default
		 * value. */
		slot->tts_values[attoff] =
			getmissingattr(slot->tts_tupleDescriptor, attnum, &slot->tts_isnull[attoff]);
	}
	else
	{
		/* Value is compressed, so get it from the decompressed arrow array. */
		const Oid typid = TupleDescAttr(slot->tts_tupleDescriptor, attoff)->atttypid;
		const NullableDatum datum =
			arrow_get_datum(arrow_arrays[attoff], typid, aslot->tuple_index - 1);
		slot->tts_values[attoff] = datum.value;
		slot->tts_isnull[attoff] = datum.isnull;
	}

	TS_WITH_MEMORY_CONTEXT(slot->tts_mcxt,
						   { aslot->valid_attrs = bms_add_member(aslot->valid_attrs, attnum); });
}

static void
tts_arrow_getsomeattrs(TupleTableSlot *slot, int natts)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;
	const int16 *attrs_map;
	int cattnum = -1;
	ArrowArray **arrow_arrays;

	Ensure((natts >= 1), "invalid number of attributes requested");

	/* According to the TupleTableSlot API, getsomeattrs() can be called with
	 * a natts higher than the available attributes, in which case tts_nvalid
	 * should be set to the number of returned columns */
	if (natts > slot->tts_tupleDescriptor->natts)
		natts = slot->tts_tupleDescriptor->natts;

	/* Check if attributes are already retrieved */
	if (natts <= slot->tts_nvalid)
		return;

	if (aslot->tuple_index == InvalidTupleIndex)
	{
		slot_getsomeattrs(aslot->child_slot, natts);

		/* The child slot points to an non-compressed tuple, so just copy over
		 * the values from the child. */
		copy_slot_values(aslot->child_slot, slot, natts);
		return;
	}

	attrs_map = arrow_slot_get_attribute_offset_map(slot);

	/* Find highest attribute number/offset in compressed relation in order to
	 * make slot_getsomeattrs() get all the required attributes in the
	 * compressed tuple. */
	for (int i = 0; i < natts; i++)
	{
		int16 coff = attrs_map[i];

		if (AttrOffsetGetAttrNumber(coff) > cattnum)
			cattnum = AttrOffsetGetAttrNumber(coff);
	}

	Assert(cattnum > 0);
	slot_getsomeattrs(aslot->child_slot, cattnum);

	/* Decompress the values from the compressed tuple */
	arrow_arrays = arrow_column_cache_read_many(aslot, natts);

	/* Build the non-compressed tuple values array from the cached data. */
	for (int i = 0; i < natts; i++)
		set_attr_value(slot, arrow_arrays, AttrOffsetGetAttrNumber(i));

	slot->tts_nvalid = natts;
}

/*
 * Fetch a system attribute of the slot's current tuple.
 */
static Datum
tts_arrow_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));

	/* System attributes are always negative. No need to use the attribute map
	 * for compressed tuples since system attributes are neither compressed
	 * nor mapped to different attribute numbers. */
	Assert(attnum < 0);

	if (NULL == aslot->child_slot)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot retrieve a system column in this context")));

	return aslot->child_slot->tts_ops->getsysattr(aslot->child_slot, attnum, isnull);
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
	ArrowTupleTableSlot *asrcslot = (ArrowTupleTableSlot *) srcslot;
	ArrowTupleTableSlot *adstslot = (ArrowTupleTableSlot *) dstslot;
	TupleTableSlot *child_dstslot = NULL;
	TupleTableSlot *child_srcslot = NULL;

	Assert(TTS_IS_ARROWTUPLE(dstslot));

	/*
	 * If the source and destination slots have the same implementation and
	 * both child slots are empty, this is used as a virtual tuple and we just
	 * forward the call to the virtual method and let it deal with the
	 * copy. This can be the case for COPY FROM since it creates a slot with
	 * the type of the table being copied to and uses ExecStoreVirtualTuple to
	 * store the tuple data from the file.
	 */
	if (dstslot->tts_ops == srcslot->tts_ops && TTS_EMPTY(asrcslot->noncompressed_slot) &&
		(asrcslot->compressed_slot == NULL || TTS_EMPTY(asrcslot->compressed_slot)))
	{
		Assert(!TTS_EMPTY(srcslot));
		TTSOpsVirtual.copyslot(dstslot, srcslot);
		return;
	}

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
	HeapTuple tuple;

	Assert(!TTS_EMPTY(slot));

	if (aslot->child_slot == aslot->compressed_slot)
	{
		/* Make sure the child slot has the parent's datums or otherwise it won't
		 * have any data to produce the tuple from */
		ExecClearTuple(aslot->noncompressed_slot);
		slot_getallattrs(slot);
		copy_slot_values(slot, aslot->noncompressed_slot, slot->tts_tupleDescriptor->natts);
	}

	/* Since this tuple is generated from a baserel, there are cases when PG
	 * code expects the TID (t_self) to be set (e.g., during ANALYZE). But
	 * this doesn't happen if the tuple is formed from values similar to a
	 * virtual tuple. Therefore, explicitly set t_self here to mimic a buffer
	 * heap tuple. */
	tuple = ExecCopySlotHeapTuple(aslot->noncompressed_slot);
	ItemPointerCopy(&slot->tts_tid, &tuple->t_self);

	/* Clean up if the non-compressed slot was "borrowed" */
	if (aslot->child_slot == aslot->compressed_slot)
		ExecClearTuple(aslot->noncompressed_slot);

	return tuple;
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
	MinimalTuple tuple;

	Assert(!TTS_EMPTY(slot));
	/* Make sure the child slot has the parent's datums or otherwise it won't
	 * have any data to produce the tuple from */
	slot_getallattrs(slot);
	copy_slot_values(slot, aslot->noncompressed_slot, slot->tts_tupleDescriptor->natts);
	tuple = ExecCopySlotMinimalTuple(aslot->noncompressed_slot);

	/* Clean up if the non-compressed slot was "borrowed" */
	if (aslot->child_slot == aslot->compressed_slot)
		ExecClearTuple(aslot->noncompressed_slot);

	return tuple;
}

const ArrowArray *
arrow_slot_get_array(TupleTableSlot *slot, AttrNumber attno)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;
	int attoff = AttrNumberGetAttrOffset(attno);
	ArrowArray **arrow_arrays;

	TS_DEBUG_LOG("attno: %d, tuple_index: %d", attno, aslot->tuple_index);

	Assert(TTS_IS_ARROWTUPLE(slot));

	if (attno > slot->tts_tupleDescriptor->natts)
		elog(ERROR, "invalid attribute number");

	/* Regular (non-compressed) tuple has no arrow array */
	if (aslot->tuple_index == InvalidTupleIndex)
	{
		slot_getsomeattrs(slot, attno);
		return NULL;
	}

	arrow_arrays = arrow_column_cache_read_one(aslot, attno);
	set_attr_value(slot, arrow_arrays, attno);

	return arrow_arrays[attoff];
}

/*
 * Store columns that the arrow slot will decompress.
 *
 * Also set the flag that this TTS is using column select so that we can
 * handle uses of slot_getallattrs() that are not part of normal plan
 * execution. */
void
arrow_slot_set_referenced_attrs(TupleTableSlot *slot, Bitmapset *attrs)
{
	Assert(TTS_IS_ARROWTUPLE(slot));

	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;
	TS_WITH_MEMORY_CONTEXT(aslot->arrow_cache.mcxt, { aslot->referenced_attrs = bms_copy(attrs); });
}

void
arrow_slot_set_index_attrs(TupleTableSlot *slot, Bitmapset *attrs)
{
	Assert(TTS_IS_ARROWTUPLE(slot));

	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;
	TS_WITH_MEMORY_CONTEXT(aslot->arrow_cache.mcxt, { aslot->index_attrs = bms_copy(attrs); });
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
