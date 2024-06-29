/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <access/attnum.h>
#include <access/htup_details.h>
#include <access/tupdesc.h>
#include <catalog/index.h>
#include <catalog/pg_attribute.h>
#include <executor/tuptable.h>
#include <nodes/bitmapset.h>
#include <storage/block.h>
#include <storage/itemptr.h>
#include <storage/off.h>
#include <utils/builtins.h>
#include <utils/hsearch.h>
#include <utils/palloc.h>

#include "arrow_cache.h"
#include "compression/arrow_c_data_interface.h"
#include "debug_assert.h"

#include <limits.h>

/*
 * An Arrow tuple slot is a meta-slot representing a compressed and columnar
 * relation that stores data in two separate child relations: one for
 * non-compressed data and one for compressed data.
 *
 * The Arrow tuple slot also gives an abstraction for vectorized data in arrow
 * format (in case of compressed reads), where value-by-value reads of
 * compressed data simply reads from the same compressed child slot until it
 * is completely consumed. Thus, when consuming a compressed child tuple, the
 * child is decompressed on the first read, while subsequent reads of values
 * in the same compressed tuple just increments the index into the
 * decompressed arrow array.
 *
 * Since an Arrow slot contains a reference to the whole decompressed arrow
 * array, it is possible to consume all the Arrow slot's values (rows) in one
 * vectorized read.
 *
 * To enable the abstraction of a single slot and relation, two child slots
 * are needed that match the expected slot type (BufferHeapTupletableslot) and
 * tuple descriptor of the corresponding child relations.
 *
 * The LRU list is sorted in reverse order so the head element is the LRU
 * element. This is because there is a dlist_pop_head, but no dlist_pop_tail.
 *
 */
typedef struct ArrowTupleTableSlot
{
	VirtualTupleTableSlot base;
	/* child slot: points to either noncompressed_slot or compressed_slot,
	 * depending on which slot is currently the "active" child */
	TupleTableSlot *child_slot;
	/* non-compressed slot: used when reading from the non-compressed child relation */
	TupleTableSlot *noncompressed_slot;
	/* compressed slot: used when reading from the compressed child relation */
	TupleTableSlot *compressed_slot;
	AttrNumber count_attnum; /* Attribute number of the count metadata in compressed slot */
	uint16 tuple_index;		 /* Index of this particular tuple in the compressed
							  * (columnar data) child tuple. Note that the first
							  * value has index 1. If the index is 0 it means the
							  * child slot points to a non-compressed tuple. */
	uint16 total_row_count;
	ArrowColumnCache arrow_cache;

	/* If referenced_attrs_valid is true, decompress only the columns in
	 * referenced_attrs. If it is false, no analysis was made of referenced
	 * attributes, so all columns need to be decompressed to be safe. Note, it
	 * is not possible to use referenced_attrs==NULL as a way to indicate that
	 * analysis did not run since an empty set (NULL) is a valid state of the
	 * set after analysis.  */
	bool referenced_attrs_valid;
	Bitmapset *referenced_attrs;
	bool *segmentby_attrs;
	bool *valid_attrs;		 /* Per-column validity up to "tts_nvalid" */
	Bitmapset *index_attrs;	 /* Columns in index during index scan */
	int16 *attrs_offset_map; /* Offset number mappings between the
							  * non-compressed and compressed
							  * relation */
} ArrowTupleTableSlot;

extern const TupleTableSlotOps TTSOpsArrowTuple;

extern const int16 *arrow_slot_get_attribute_offset_map(TupleTableSlot *slot);
extern TupleTableSlot *ExecStoreArrowTuple(TupleTableSlot *slot, uint16 tuple_index);

#define TTS_IS_ARROWTUPLE(slot) ((slot)->tts_ops == &TTSOpsArrowTuple)

/*
 * The encoded Hyperstore TID can address a specific value in a compressed tuple by
 * adding an extra "tuple index" to the TID, which is the index into the array of values
 * in the compressed tuple. The new encoding consists of the block number (CBLOCK) and offset
 * number (COFFSET) of the TID for the compressed row as block number and the
 * tuple_index (TINDEX) as offset number. In addition, we have a compressed
 * flag (F) that is set for compressed TIDs.
 *
 *          32                 6        10      bits
 * +----------------------+ +---------+---------+
 * |       CBLOCK         | | PADDING | COFFSET |   TID for compressed row
 * +---+--------+---------+ +---------+---------+
 * | F | CBLOCK | COFFSET | | PADDING | TINDEX  |   Encoded TID
 * +---+--------+---------+ +---------+---------+
 *   1     21       10         6        10      bits
 *
 * As a consequence, we only have 21 bits for storing the block number in the
 * compressed relation, which means that we have to ensure that we do not
 * exceed this limit when adding new blocks to it.
 */

#define InvalidTupleIndex 0
#define MinTupleIndex 1
#define MaxTupleIndex UINT16_MAX

#define BLOCKID_BITS (CHAR_BIT * sizeof(BlockIdData))
#define COMPRESSED_FLAG (1UL << (BLOCKID_BITS - 1))
#define MaxCompressedBlockNumber ((BlockNumber) (COMPRESSED_FLAG - 1))
#define OFFSET_BITS (10U)
#define OFFSET_LIMIT ((uint64) 1UL << OFFSET_BITS)
#define OFFSET_MASK (OFFSET_LIMIT - 1)

static inline void
hyperstore_tid_encode(ItemPointerData *out_tid, const ItemPointerData *in_tid, uint16 tuple_index)
{
	const BlockNumber block = ItemPointerGetBlockNumber(in_tid);
	const OffsetNumber offset = ItemPointerGetOffsetNumber(in_tid);
	const uint64 encoded_tid = ((uint64) block << OFFSET_BITS) | (uint16) offset;

	Assert(offset < OFFSET_LIMIT);
	Assert(tuple_index != InvalidTupleIndex);

	/* Ensure that we are not using the compressed flag in the encoded tid */
	Ensure((COMPRESSED_FLAG | encoded_tid) != encoded_tid, "block number too large");

	ItemPointerSet(out_tid, COMPRESSED_FLAG | encoded_tid, tuple_index);
}

static inline uint16
hyperstore_tid_decode(ItemPointerData *out_tid, const ItemPointerData *in_tid)
{
	const uint64 encoded_tid = ~COMPRESSED_FLAG & ItemPointerGetBlockNumber(in_tid);
	const uint16 tuple_index = ItemPointerGetOffsetNumber(in_tid);

	BlockNumber block = (BlockNumber) (encoded_tid >> OFFSET_BITS);
	OffsetNumber offset = (OffsetNumber) (encoded_tid & OFFSET_MASK);

	ItemPointerSet(out_tid, block, offset);

	Assert(tuple_index != InvalidTupleIndex);
	return tuple_index;
}

static inline void
hyperstore_tid_set_tuple_index(ItemPointerData *tid, uint32 tuple_index)
{
	/* Assert that we do not overflow the increment: we only have 10 bits for the tuple index */
	Assert(tuple_index < 1024);
	ItemPointerSetOffsetNumber(tid, tuple_index);
}

static inline void
hyperstore_tid_increment(ItemPointerData *tid, uint16 increment)
{
	/* Assert that we do not overflow the increment: we only have 10 bits for the tuple index */
	hyperstore_tid_set_tuple_index(tid, ItemPointerGetOffsetNumber(tid) + increment);
}

static inline bool
is_compressed_tid(const ItemPointerData *itemptr)
{
	return (ItemPointerGetBlockNumber(itemptr) & COMPRESSED_FLAG) != 0;
}

extern TupleTableSlot *arrow_slot_get_compressed_slot(TupleTableSlot *slot,
													  const TupleDesc tupdesc);

static inline TupleTableSlot *
arrow_slot_get_noncompressed_slot(TupleTableSlot *slot)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;

	Assert(TTS_IS_ARROWTUPLE(slot));
	Assert(aslot->noncompressed_slot);

	return aslot->noncompressed_slot;
}

static inline uint16
arrow_slot_total_row_count(TupleTableSlot *slot)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;

	Assert(TTS_IS_ARROWTUPLE(slot));
	Assert(aslot->total_row_count > 0);

	return aslot->total_row_count;
}

static inline bool
arrow_slot_is_compressed(const TupleTableSlot *slot)
{
	const ArrowTupleTableSlot *aslot = (const ArrowTupleTableSlot *) slot;
	Assert(TTS_IS_ARROWTUPLE(slot));
	return aslot->tuple_index != InvalidTupleIndex;
}

/*
 * Get the row index into the compressed tuple.
 *
 * The index is 1-based (starts at 1).
 * InvalidTupleindex means this is not a compressed tuple.
 */
static inline uint16
arrow_slot_row_index(const TupleTableSlot *slot)
{
	const ArrowTupleTableSlot *aslot = (const ArrowTupleTableSlot *) slot;
	Assert(TTS_IS_ARROWTUPLE(slot));
	return aslot->tuple_index;
}

/*
 * Get the current offset into the arrow array.
 *
 * The offset is 0-based. Returns 0 also for a non-compressed tuple.
 */
static inline uint16
arrow_slot_arrow_offset(const TupleTableSlot *slot)
{
	const ArrowTupleTableSlot *aslot = (const ArrowTupleTableSlot *) slot;
	Assert(TTS_IS_ARROWTUPLE(slot));
	return aslot->tuple_index == InvalidTupleIndex ? 0 : aslot->tuple_index - 1;
}

static inline void
arrow_slot_mark_consumed(TupleTableSlot *slot)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;

	Assert(TTS_IS_ARROWTUPLE(slot));
	aslot->tuple_index = aslot->total_row_count + 1;
}

static inline bool
arrow_slot_is_consumed(const TupleTableSlot *slot)
{
	const ArrowTupleTableSlot *aslot = (const ArrowTupleTableSlot *) slot;

	Assert(TTS_IS_ARROWTUPLE(slot));

	return TTS_EMPTY(slot) || aslot->tuple_index > aslot->total_row_count;
}

static inline bool
arrow_slot_is_last(const TupleTableSlot *slot)
{
	const ArrowTupleTableSlot *aslot = (const ArrowTupleTableSlot *) slot;

	Assert(TTS_IS_ARROWTUPLE(slot));

	return aslot->tuple_index == InvalidTupleIndex || aslot->tuple_index == aslot->total_row_count;
}

/*
 * Increment or decrement an arrow slot to point to a subsequent row.
 *
 * If the slot points to a non-compressed tuple, the change will
 * simply clear the slot.
 *
 * If the slot points to a compressed tuple, an increment or decrement will
 * clear the slot if it reaches the end of the segment or beginning of it,
 * respectively.
 */
static inline TupleTableSlot *
ExecIncrOrDecrArrowTuple(TupleTableSlot *slot, int32 amount)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;

	Assert(slot != NULL && slot->tts_tupleDescriptor != NULL);

	if (unlikely(!TTS_IS_ARROWTUPLE(slot)))
		elog(ERROR, "trying to store an on-disk arrow tuple into wrong type of slot");

	if (aslot->tuple_index == InvalidTupleIndex)
	{
		Assert(aslot->noncompressed_slot);
		ExecClearTuple(slot);
		return slot;
	}

	int32 tuple_index = (int32) aslot->tuple_index + amount;

	if (tuple_index > aslot->total_row_count || tuple_index < 1)
	{
		Assert(aslot->compressed_slot);
		ExecClearTuple(slot);
		return slot;
	}

	Assert(tuple_index > 0 && tuple_index <= aslot->total_row_count);
	hyperstore_tid_set_tuple_index(&slot->tts_tid, tuple_index);
	aslot->tuple_index = (uint16) tuple_index;
	slot->tts_flags &= ~TTS_FLAG_EMPTY;
	slot->tts_nvalid = 0;
	memset(aslot->valid_attrs, 0, sizeof(bool) * slot->tts_tupleDescriptor->natts);

	return slot;
}

static inline TupleTableSlot *
ExecIncrArrowTuple(TupleTableSlot *slot, uint16 increment)
{
	return ExecIncrOrDecrArrowTuple(slot, increment);
}

static inline TupleTableSlot *
ExecDecrArrowTuple(TupleTableSlot *slot, uint16 decrement)
{
	return ExecIncrOrDecrArrowTuple(slot, -decrement);
}

#define ExecStoreNextArrowTuple(slot) ExecIncrArrowTuple(slot, 1)
#define ExecStorePreviousArrowTuple(slot) ExecDecrArrowTuple(slot, 1)

extern const int16 *arrow_slot_get_attribute_offset_map(TupleTableSlot *slot);
extern bool is_compressed_col(const TupleDesc tupdesc, AttrNumber attno);
extern const ArrowArray *arrow_slot_get_array(TupleTableSlot *slot, AttrNumber attno);
extern void arrow_slot_set_referenced_attrs(TupleTableSlot *slot, Bitmapset *attrs);
extern void arrow_slot_set_index_attrs(TupleTableSlot *slot, Bitmapset *attrs);

extern Datum tsl_is_compressed_tid(PG_FUNCTION_ARGS);
