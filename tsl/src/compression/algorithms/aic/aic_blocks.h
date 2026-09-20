/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include "aic_internal.h"
#include "compression/algorithms/fastlanes/fastlanes_types.h"

/*
 * the compression format starts with a 5-byte header, which is followed by a
 * sequence of blocks. the header contains the global flags, that tells the
 * format of the NULL bitmap, the element width, and the number of blocks
 * in the stream. it also contains the number of non-null
 * values, the number of null values (AICCompressed struct in aic_internal.h).
 *
 * the blocks are central concepts of the compression format. each block
 * defines a specific encoding for a set of values. each block has a header
 * that describes the encoding and a set of parameters and the encoded values
 * that follow the header.
 */

/* please read the README.md file for more details. */

typedef enum AICBlockType
{
	/* RLE: repeating the same value, or same increment from a base (delta RLE) */
	AIC_BT_RLE = 0,

	/* FOR: fastlanes with an optional (frame of reference) base */
	AIC_BT_FOR = 1,

	/* PFOR: patched frame of reference */
	AIC_BT_PFOR = 2,

	/* DFOR: strided deltas stored with FastLanes */
	AIC_BT_DFOR = 3,

	/* DICT: dictionary encoding */
	AIC_BT_DICT = 4,

	AIC_BT_COUNT_ = 5,
} AICBlockType;

/*
 * block record layout
 *
 * each block starts with a uint32 header; its low 3 bits hold the
 * AICBlockType. PFOR, DFOR, and DICT add a trailing uint8 for a count
 * that overflows the uint32. block size is derived from the header.
 *
 * shared conventions:
 *
 *  - depending on the block type, we optionally store variable length
 *    scalar values after the header, which is followed by the packed
 *    data. the packed data may have multiple parts, e.g., a body and
 *    an exception stream for PFOR.
 *
 * - the headers and packed data are stored as a contiguous stream of
 *   bytes, with no alignment or padding.
 *
 *  - scalars (base, key_base, delta_residual_base, step) are stored
 *    at 1..8 bytes of signed values in a two's complement little
 *    endian format. each scalar has a corresponding length where we
 *    tell how many bytes are used to store the scalar. the length is
 *    stored in the header.
 *
 *  - counts are always stored as count-1, so a stored c means c+1
 *    elements, because they can't be zero
 */

/*
 * AIC_BT_RLE -- a run of one repeated value (simple RLE) or an
 * arithmetic run (delta RLE). `step_len_bytes` of zero selects simple
 * RLE; nonzero selects delta RLE and appends step. The decoder
 * expands the run from count, base, and step (no packed body).
 *
 * in case of simple RLE, the `base` is repeated `count` times
 * in case of delta RLE, the `base` is repeated and incremented by
 *   `step` for `count` times
 *
 *   uint32 header: block_type (3) + base_len_bytes (4)
 *                  + step_len_bytes (4) + count (16)    [27 bits used]
 *   base: base_len_bytes bytes
 *   step: step_len_bytes bytes                          [delta RLE only]
 */
typedef struct AICBlockRLE
{
	uint32 header;
	int64 base_value;
	int64 step_value;

} AICBlockRLE;

extern void aic_block_rle_init(AICBlockRLE *block, uint32 count, int64 base, int64 step,
							   fl_elem_width_t T);

static inline uint64
aic_block_rle_value_at(const AICBlockRLE *block, uint32 pos)
{
	return (uint64) block->base_value + (uint64) pos * (uint64) block->step_value;
}

/*
 * AIC_BT_FOR -- a FastLanes body with an optional frame-of-reference
 * base. base_len_bytes of zero selects simple FastLanes; nonzero
 * subtracts base before packing.
 *
 * this is the simplest encoding type, where it is really just a
 * wrapper over the FastLanes packer.
 *
 *   uint32 header: block_type (3) + base_len_bytes (4) + W (7)
 *                  + count (8)                          [22 bits used]
 *   base: base_len_bytes bytes                          [omitted when 0]
 *   body: count values packed at W bits
 */
typedef struct AICBlockFOR
{
	uint32 header;
	int64 base_value;
	uint8 *body;
	size_t packed_body_size;

} AICBlockFOR;

extern void aic_block_for_init(AICBlockFOR *block, uint32 count, int64 base, uint8 W,
							   fl_elem_width_t T, uint8 *body);

/*
 * AIC_BT_PFOR -- PFOR with a base. The `body` packs the base-subtracted
 * values at W. the `positions` array holds the indices of the exceptions,
 * and the `exceptions` array holds the high bits of these exceptions,
 * packed at W_exc.
 *
 *   uint32 header: block_type (3) + base_len_bytes (4) + W (7)
 *                  + count_base (8) + W_exc (7)         [29 bits used]
 *   uint8:      count_exc (7)                           [7 bits used]
 *   base:       base_len_bytes bytes                    [omitted when 0]
 *   body:       count_base values packed at W bits
 *   positions:  count_exc bytes, one outlier index each
 *   exceptions: count_exc high-bit values packed at W_exc bits
 */

typedef struct AICBlockPFOR
{
	uint32 header;
	uint8 count_exc;
	int64 base_value;
	uint8 positions[AIC_PFOR_MAX_EXCEPTIONS];
	uint8 *exc_positions;
	uint8 *body;
	uint8 *exc_body;
	size_t packed_body_size;
	size_t packed_exc_size;

} AICBlockPFOR;

extern void aic_block_pfor_init(AICBlockPFOR *block, uint32 count, int64 base, uint8 W, uint8 W_exc,
								uint8 n_exc, const uint8 *positions, fl_elem_width_t T, uint8 *body,
								uint8 *exc_body);

/*
 * AIC_BT_DFOR -- strided deltas. the first count_base values are lane
 * bases packed at W_bases; the remaining count_delta_residuals values
 * are the strided deltas packed at W_delta_residuals.
 *
 * the strided delta values may be packed with zigzag encoding depending
 * on the `zigzag_deltas` flag. if zigzag_deltas is true, the deltas are
 * zigzag-encoded and then packed with a base value, the minimum of the
 * encoded deltas.
 *
 * if zigzag_deltas is false, the deltas are packed as-is, and the
 * delta_residual_base is used to store the minimum of the deltas, which
 * may be negative.
 *
 *   uint32 header: block_type (3) + base_len_bytes (4) + W_bases (7)
 *                  + count_base (6) + delta_residual_base_len_bytes (4)
 *                  + W_delta_residuals (7) + zigzag_deltas (1)
 *                                                       [32 bits used]
 *   uint8:               count_delta_residuals (8)
 *   base:                base_len_bytes bytes                 [omitted when 0]
 *   delta_residual_base: delta_residual_base_len_bytes bytes  [omitted when 0]
 *   bases:               count_base values packed at W bits
 *   delta residuals:     count_delta_residuals values packed at W_delta_residuals bits
 */
typedef struct AICBlockDFOR
{
	uint32 header;
	uint8 count_delta_residuals;
	int64 base_value;
	int64 delta_residual_base_value;
	uint8 *bases_body;
	uint8 *delta_residual_body;
	size_t packed_bases_size;
	size_t packed_delta_residual_size;

} AICBlockDFOR;

extern void aic_block_dfor_init(AICBlockDFOR *block, uint32 count, uint16 S, int64 base,
								uint8 W_bases, int64 delta_residual_base, uint8 W_delta_residuals,
								bool zigzag, fl_elem_width_t T, uint8 *bases_body,
								uint8 *delta_residual_body);

/*
 * AIC_BT_DICT -- dictionary. `count_keys` distinct values form the `dict`
 * body (keys) packed at W; count_idx indices into the dict are packed at
 * W_idx.
 *
 * the index values may be packed as a zigzag encoded delta stream or as
 * raw values depending on the `zigzag_idxs` flag.
 *
 *   uint32 header: block_type (3) + key_base_len_bytes (4) + W (7)
 *                  + count_keys (8) + W_idx (7) + zigzag_idxs (1)
 *                                                       [30 bits used]
 *   uint8:    count_idx (8)
 *   key_base: key_base_len_bytes bytes                  [omitted when 0]
 *   dict:     count_keys values packed at W bits
 *   indices:  count_idx values packed at W_idx bits, where the zigzag
 *             values use FL_ELEM_W16, and the raw values use FL_ELEM_W8
 *             for the FastLanes packing
 */
typedef struct AICBlockDICT
{
	uint32 header;
	uint8 count_idx;
	int64 key_base_value;
	uint8 *keys_body;
	uint8 *idx_body;
	size_t packed_keys_size;
	size_t packed_idx_size;

} AICBlockDICT;

extern void aic_block_dict_init(AICBlockDICT *block, uint32 count, uint16 K, int64 key_base,
								uint8 W, uint8 W_idx, bool zigzag_idxs, fl_elem_width_t T,
								uint8 *keys_body, uint8 *idx_body);

typedef union AICBlockData
{
	AICBlockRLE rle;
	AICBlockFOR for_;
	AICBlockDFOR dfor;
	AICBlockDICT dict;
	AICBlockPFOR pfor;
} AICBlockData;

/* For code simplicity AICBlock is used in both compression and decompression */
typedef struct AICBlock
{
	AICBlockType type;
	AICBlockData data;

	struct AICBlock *next;
} AICBlock;

extern void aic_free_blocks(AICBlock *block);
extern size_t aic_total_block_size(AICBlock *block);
extern bool aic_serialize_blocks(AICBlock *block, uint8 *dest, size_t max_size);

/*
 * scratch_buffer must be FL_MAX_ALIGNMENT-aligned with at least
 * AIC_SCRATCH_BYTES - FL_MAX_ALIGNMENT usable bytes. most blocks stage their
 * packed input in the first AIC_SCRATCH_STAGING_BYTES, and PFOR unpacks
 * its exception stream into the AIC_PFOR_EXC_BYTES above that.
 */
extern size_t aic_decompress_blocks(uint32 num_blocks, StringInfo si, uint8 *dest,
									uint8 *scratch_buffer, size_t max_elements, fl_elem_width_t T);

/*
 * parse and validate one block without decompressing it and advance
 * `si` to the start of the next block. after calling this function,
 * the `block` will have all the information to decompress the data.
 *
 * this function is used in the iterator based decompression
 */
extern size_t aic_parse_one_block(AICBlock *block, StringInfo si, fl_elem_width_t T);

/*
 * decompress one `block`. the scratch buffer is an aligned temp
 * buffer that can be reused between the decompression of the
 * different blocks
 */
extern size_t aic_decompress_one_block(const AICBlock *block, uint8 *dest, uint8 *scratch_buffer,
									   fl_elem_width_t T);
