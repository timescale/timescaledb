/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef PG_ARROW_TUPTABLE_H
#define PG_ARROW_TUPTABLE_H

#include <postgres.h>
#include <access/attnum.h>
#include <access/tupdesc.h>
#include <catalog/pg_attribute.h>
#include <executor/tuptable.h>
#include <nodes/bitmapset.h>
#include <utils/builtins.h>
#include <utils/hsearch.h>
#include <utils/palloc.h>

#include "arrow_c_data_interface.h"
#include "compression/arrow_cache.h"
#include "compression/create.h"
#include "nodes/decompress_chunk/detoaster.h"

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
	ArrowArray **arrow_columns;
	uint16 tuple_index; /* Index of this particular tuple in the compressed
						 * (columnar data) child tuple. Note that the first
						 * value has index 1. If the index is 0 it means the
						 * child slot points to a non-compressed tuple. */
	uint16 total_row_count;
	ArrowColumnCache arrow_cache;

	/* Decompress only these columns. If no columns are set, all columns will
	 * be decompressed. */
	Bitmapset *referenced_attrs;
	Bitmapset *segmentby_attrs;
	Bitmapset *valid_columns; /* Per-column validity replacing "nvalid" */
	int16 *attrs_offset_map;
} ArrowTupleTableSlot;

extern const TupleTableSlotOps TTSOpsArrowTuple;

static inline int16 *
build_attribute_offset_map(const TupleDesc tupdesc, const TupleDesc ctupdesc,
						   AttrNumber *count_attno)
{
	int16 *attrs_offset_map = palloc0(sizeof(int16) * tupdesc->natts);

	for (int i = 0; i < tupdesc->natts; i++)
	{
		const Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		if (attr->attisdropped)
		{
			attrs_offset_map[i] = -1;
		}
		else
		{
			bool found = false;

			for (int j = 0; j < ctupdesc->natts; j++)
			{
				const Form_pg_attribute cattr = TupleDescAttr(ctupdesc, j);

				if (!cattr->attisdropped &&
					namestrcmp(&attr->attname, NameStr(cattr->attname)) == 0)
				{
					attrs_offset_map[i] = AttrNumberGetAttrOffset(cattr->attnum);
					found = true;
					break;
				}
			}

			Ensure(found, "missing attribute in compressed relation");
		}
	}

	if (count_attno)
	{
		*count_attno = InvalidAttrNumber;

		/* Find the count column attno */
		for (int i = 0; i < ctupdesc->natts; i++)
		{
			const Form_pg_attribute cattr = TupleDescAttr(ctupdesc, i);

			if (namestrcmp(&cattr->attname, COMPRESSION_COLUMN_METADATA_COUNT_NAME) == 0)
			{
				*count_attno = cattr->attnum;
				break;
			}
		}

		Assert(*count_attno != InvalidAttrNumber);
	}

	return attrs_offset_map;
}

extern const int16 *arrow_slot_get_attribute_offset_map(TupleTableSlot *slot);
extern TupleTableSlot *ExecStoreArrowTuple(TupleTableSlot *slot, uint16 tuple_index);

#define TTS_IS_ARROWTUPLE(slot) ((slot)->tts_ops == &TTSOpsArrowTuple)

#define InvalidTupleIndex 0
#define MaxCompressedBlockNumber ((BlockNumber) 0x3FFFFF)

/*
 * The compressed TID is encoded in the following manner, which places a limit
 * on 1024 rows in a single compressed tuple. Since we are currently storing
 * 1000 rows that should work.
 *
 *         32 bits                16 bits
 * +-------------------------+-----------------+
 * |       Block Number      |  Offset Number  |
 * +------+------------------+---+-------------+
 * | Flag | Compressed Tuple TID | Tuple Index |
 * +------+----------------------+-------------+
 *  1 bit         33 bits            10 bits
 */

#define BLOCKID_BITS (CHAR_BIT * sizeof(BlockIdData))
#define COMPRESSED_FLAG (1UL << (BLOCKID_BITS - 1))
#define OFFSET_BITS (CHAR_BIT * sizeof(OffsetNumber))
#define OFFSET_MASK (((uint64) 1UL << OFFSET_BITS) - 1)
#define TUPINDEX_BITS (10U)
#define TUPINDEX_MASK (((uint64) 1UL << TUPINDEX_BITS) - 1)

/* Compute a 64-bits value from the item pointer data */
static uint64
bits_from_tid(const ItemPointerData *tid)
{
	return (ItemPointerGetBlockNumber(tid) << OFFSET_BITS) | ItemPointerGetOffsetNumber(tid);
}

/*
 * The "compressed TID" consists of the bits of the TID for the compressed row
 * shifted to insert the tuple index as the least significant bits of the TID.
 */
static inline void
tid_to_compressed_tid(ItemPointerData *out_tid, const ItemPointerData *in_tid, uint16 tuple_index)
{
	const uint64 bits = (bits_from_tid(in_tid) << TUPINDEX_BITS) | tuple_index;

	Assert(tuple_index != InvalidTupleIndex);

	/* Insert the tuple index as the least significant bits and set the most
	 * significant bit of the block id to mark it as a compressed tuple. */
	const BlockNumber blockno = COMPRESSED_FLAG | (bits >> OFFSET_BITS);
	const OffsetNumber offsetno = bits & OFFSET_MASK;
	ItemPointerSet(out_tid, blockno, offsetno);
}

static inline uint16
compressed_tid_to_tid(ItemPointerData *out_tid, const ItemPointerData *in_tid)
{
	const uint64 orig_bits = bits_from_tid(in_tid);
	const uint16 tuple_index = orig_bits & TUPINDEX_MASK;

	/* Remove the tuple index bits and clear the compressed flag from the block id. */
	const uint64 bits = orig_bits >> TUPINDEX_BITS;
	BlockNumber blockno = ~COMPRESSED_FLAG & (bits >> OFFSET_BITS);
	OffsetNumber offsetno = bits & OFFSET_MASK;

	Assert(tuple_index != InvalidTupleIndex);

	ItemPointerSet(out_tid, blockno, offsetno);

	return tuple_index;
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
	aslot->tuple_index = aslot->total_row_count;
}

static inline bool
arrow_slot_is_consumed(const TupleTableSlot *slot)
{
	const ArrowTupleTableSlot *aslot = (const ArrowTupleTableSlot *) slot;

	Assert(TTS_IS_ARROWTUPLE(slot));

	return aslot->tuple_index == aslot->total_row_count;
}

extern bool is_compressed_col(const TupleDesc tupdesc, AttrNumber attno);
extern const ArrowArray *arrow_slot_get_array(TupleTableSlot *slot, AttrNumber attno);
extern void arrow_slot_set_referenced_attrs(TupleTableSlot *slot, Bitmapset *attrs);

#endif /* PG_ARROW_TUPTABLE_H */
