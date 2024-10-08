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
arrow_slot_get_attribute_offset_map_slow(TupleTableSlot *slot)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;
	const TupleDesc tupdesc = slot->tts_tupleDescriptor;
	Oid relid =
		OidIsValid(slot->tts_tableOid) ? slot->tts_tableOid : TupleDescAttr(tupdesc, 0)->attrelid;

	Assert(aslot->attrs_offset_map == NULL);

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

	aslot->arrow_cache_entry = NULL;
	aslot->segmentby_attrs = NULL;
	aslot->attrs_offset_map = NULL;
	aslot->tuple_index = InvalidTupleIndex;
	aslot->total_row_count = 0;
	aslot->referenced_attrs = NULL;

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
	MemoryContext oldmcxt = MemoryContextSwitchTo(slot->tts_mcxt);
	aslot->noncompressed_slot =
		MakeSingleTupleTableSlot(slot->tts_tupleDescriptor, &TTSOpsBufferHeapTuple);
	aslot->child_slot = aslot->noncompressed_slot;
	aslot->valid_attrs = palloc0(sizeof(bool) * slot->tts_tupleDescriptor->natts);
	aslot->segmentby_attrs = palloc0(sizeof(bool) * slot->tts_tupleDescriptor->natts);
	/* Note that aslot->referenced_attrs is initialized on demand, and not
	 * here, because NULL is a valid state for referenced_attrs. */
	MemoryContextSwitchTo(oldmcxt);
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
	aslot->arrow_cache_entry = NULL;
}

static void
fill_segmentby_attrs(TupleTableSlot *slot)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;
	const TupleDesc tupdesc = slot->tts_tupleDescriptor;
	const TupleDesc ctupdesc = aslot->compressed_slot->tts_tupleDescriptor;
	const int16 *attrs_offset_map = arrow_slot_get_attribute_offset_map(slot);

	/*
	 * Populate the segmentby map with information about *all* attributes in
	 * the non-compressed version of the tuple. That way we do not have to
	 * update this field if we start fetching more attributes.
	 */
	for (int i = 0; i < tupdesc->natts; i++)
	{
		if (!TupleDescAttr(tupdesc, i)->attisdropped)
		{
			const AttrNumber cattno = AttrOffsetGetAttrNumber(attrs_offset_map[i]);

			if (!is_compressed_col(ctupdesc, cattno))
				aslot->segmentby_attrs[i] = true;
		}
	}
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
		fill_segmentby_attrs(slot);
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

	/* Clear arrow slot fields */
	memset(aslot->valid_attrs, 0, sizeof(bool) * slot->tts_tupleDescriptor->natts);
	aslot->arrow_cache_entry = NULL;
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

				hyperstore_tid_decode(&decoded_tid, &slot->tts_tid);

				if (!ItemPointerEquals(&decoded_tid, &child_slot->tts_tid))
					clear_arrow_parent(slot);
			}
		}

		hyperstore_tid_encode(&slot->tts_tid, &child_slot->tts_tid, tuple_index);

		/* Stored a compressed tuple so clear the non-compressed slot */
		ExecClearTuple(aslot->noncompressed_slot);

		/* Set total row count */
		Datum d = slot_getattr(child_slot, aslot->count_attnum, &isnull);
		Assert(!isnull);
		aslot->total_row_count = DatumGetInt32(d);
	}

	/* MaxTupleIndex typically used for backwards scans, so store the index
	 * pointing to the last value */
	if (tuple_index == MaxTupleIndex)
		tuple_index = aslot->total_row_count;

	slot->tts_flags &= ~TTS_FLAG_EMPTY;
	slot->tts_nvalid = 0;
	aslot->child_slot = child_slot;
	aslot->tuple_index = tuple_index;
	aslot->arrow_cache_entry = NULL;
	/* Clear valid attributes */
	memset(aslot->valid_attrs, 0, sizeof(bool) * slot->tts_tupleDescriptor->natts);
}

/*
 * Mark the slot as storing a tuple, returning the row at the given index in
 * case of storing multiple compressed rows.
 *
 * A tuple index of InvalidTupleIndex stores a non-compressed tuple.
 *
 * A tuple index of 1 or greater stores a compressed tuple pointing to the
 * row given by the index.
 *
 * A tuple index of MaxTupleIndex means the index of the "last" row in a
 * compressed tuple. In other words, when fetching the data, the arrow slot
 * will return the last row in the compressed tuple and is typically used for
 * backward scanning.
 */
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
	Oid coltypid;

	/* Check if the column is, e.g., dropped */
	if (attno == InvalidAttrNumber)
		return false;

	coltypid = tupdesc->attrs[AttrNumberGetAttrOffset(attno)].atttypid;

	if (typinfo == NULL)
		typinfo = ts_custom_type_cache_get(CUSTOM_TYPE_COMPRESSED_DATA);

	return coltypid == typinfo->type_oid;
}

static pg_attribute_always_inline ArrowArray *
set_attr_value(TupleTableSlot *slot, const int16 attoff)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;
	ArrowArray *arrow_array = NULL;

	TS_DEBUG_LOG("attnum: %d, valid: %s, segmentby: %s",
				 AttrOffsetGetAttrNumber(attoff),
				 yes_no(aslot->valid_attrs[attoff]),
				 yes_no(aslot->segmentby_attrs[attoff]));

	if (aslot->segmentby_attrs[attoff])
	{
		const int16 *attrs_offset_map = arrow_slot_get_attribute_offset_map(slot);
		const int16 cattoff = attrs_offset_map[attoff]; /* offset in compressed tuple */
		const AttrNumber cattnum = AttrOffsetGetAttrNumber(cattoff);

		/* Segment-by column. Value is not compressed so get directly from
		 * child slot. */
		slot->tts_values[attoff] =
			slot_getattr(aslot->child_slot, cattnum, &slot->tts_isnull[attoff]);
	}
	else
	{
		const AttrNumber attnum = AttrOffsetGetAttrNumber(attoff);
		ArrowArray **arrow_arrays = arrow_column_cache_read_one(aslot, attnum);

		arrow_array = arrow_arrays[attoff];

		if (arrow_array == NULL)
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
			const Form_pg_attribute attr = TupleDescAttr(slot->tts_tupleDescriptor, attoff);
			const Oid typid = attr->atttypid;
			const int16 typlen = attr->attlen;
			const NullableDatum datum =
				arrow_get_datum(arrow_arrays[attoff], typid, typlen, aslot->tuple_index - 1);
			slot->tts_values[attoff] = datum.value;
			slot->tts_isnull[attoff] = datum.isnull;
		}
	}

	aslot->valid_attrs[attoff] = true;

	return arrow_array;
}

/*
 * Check if an attribute is actually used.
 *
 * If an attribute is not used in a query, it is not necessary to do the work
 * to materialize it or detoast it into the tts_values Datum array.
 *
 * The decision is based on referenced attributes which is set by inspecting
 * the query plan. If not set, all attributes are assumed necessary.
 *
 * Note that dropped attributes aren't excluded. When PostgreSQL evaluates
 * some expressions that have no referenced attributes (e.g., count(*)), it
 * expects dropped attributes to be set in the slot's tts_values Datum array.
 */
static inline bool
is_used_attr(const TupleTableSlot *slot, const int16 attoff)
{
	const ArrowTupleTableSlot *aslot = (const ArrowTupleTableSlot *) slot;
	return aslot->referenced_attrs == NULL || aslot->referenced_attrs[attoff];
}

static void
tts_arrow_getsomeattrs(TupleTableSlot *slot, int natts)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;

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

	/* Build the non-compressed tuple values array from the cached data. */
	for (int attoff = slot->tts_nvalid; attoff < natts; attoff++)
	{
		if (!aslot->valid_attrs[attoff] && is_used_attr(slot, attoff))
			set_attr_value(slot, attoff);
	}

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

	/* Check if copying from another slot implementation. */
	if (dstslot->tts_ops != srcslot->tts_ops)
	{
		/* If we are copying from another slot implementation to arrow slot,
		   we always copy the data into the non-compressed slot. */
		child_srcslot = srcslot;
		child_dstslot = arrow_slot_get_noncompressed_slot(dstslot);
		adstslot->tuple_index = InvalidTupleIndex;
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

	/* If the non-compressed slot is empty, we are being called as if we are a
	 * virtual TTS (from function `copyfrom` in src/copy.c), so in this case
	 * we should copy the heap tuple from the "root" slot, which is
	 * virtual.
	 *
	 * We already know that the "root" slot is not empty when this function is
	 * being called.
	 *
	 * This does an extra memcpy() of the tts_values and tts_isnull, so might
	 * be possible to optimize away.
	 */
	if (TTS_EMPTY(aslot->noncompressed_slot))
		copy_slot_values(slot, aslot->noncompressed_slot, slot->tts_tupleDescriptor->natts);

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
	const int attoff = AttrNumberGetAttrOffset(attno);
	ArrowArray **arrow_arrays;

	TS_DEBUG_LOG("attno: %d, tuple_index: %d", attno, aslot->tuple_index);

	Assert(TTS_IS_ARROWTUPLE(slot));

	if (attno > slot->tts_tupleDescriptor->natts)
		elog(ERROR, "invalid attribute number");

	/* Regular (non-compressed) tuple has no arrow array */
	if (aslot->tuple_index == InvalidTupleIndex)
	{
		slot_getsomeattrs(slot, attno);
		copy_slot_values(aslot->child_slot, slot, attno);
		return NULL;
	}

	if (!is_used_attr(slot, attoff))
		return NULL;

	if (!aslot->valid_attrs[attoff])
		return set_attr_value(slot, attoff);

	arrow_arrays = arrow_column_cache_read_one(aslot, attno);
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
	if (aslot->referenced_attrs == NULL)
	{
		aslot->referenced_attrs =
			MemoryContextAlloc(aslot->arrow_cache.mcxt,
							   sizeof(bool) * slot->tts_tupleDescriptor->natts);
		for (int i = 0; i < slot->tts_tupleDescriptor->natts; i++)
			aslot->referenced_attrs[i] = bms_is_member(AttrOffsetGetAttrNumber(i), attrs);
	}
}

void
arrow_slot_set_index_attrs(TupleTableSlot *slot, Bitmapset *attrs)
{
	Assert(TTS_IS_ARROWTUPLE(slot));

	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;
	MemoryContext oldmcxt = MemoryContextSwitchTo(aslot->arrow_cache.mcxt);
	aslot->index_attrs = bms_copy(attrs);
	MemoryContextSwitchTo(oldmcxt);
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
