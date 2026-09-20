/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <string.h>
#include "rapid_raccoon.h"
#include "compression/arrow_c_data_interface.h"
#include "compression/compression.h"
#include "fastlanes/fastlanes.h"
#include "fastlanes/fastlanes_types.h"
#include "guc.h"
#include "rapid_raccoon/rr_analyze.h"
#include "rapid_raccoon/rr_blocks.h"
#include "rapid_raccoon/rr_internal.h"
#include "rapid_raccoon/rr_nulls.h"
#include "rapid_raccoon/rr_utils.h"
#include <stdio.h>
#include <stdlib.h>
#include <utils/date.h>
#include <utils/palloc.h>
#include <utils/timestamp.h>

/*
 * Rapid Raccoon compressor. Please read the rapid_raccoon/README.md file for more details.
 *
 * The RR compressor has 3 operation modes (OpMode). The compressor starts in OP_MODE_NORMAL and
 * based on the checks at RR_DELTA_RLE_CHECKPOINT and RR_RLE_CHECKPOINT, it may move to the
 * respective modes. The modes are:
 *
 *  - OP_MODE_NORMAL: during append, the values get appended to the input buffer. when the buffer is
 * full it gets flushed, or when the stream ends, finish gets called. when these happens it analyzes
 * the content of the buffer and encodes the data using the best codec
 *
 *  - OP_MODE_RLE: during append, it checks if the current value is the same as the previous ones
 *    and if so, it increments the repetition counter, otherwise it flushes the RLE record,
 *    move to OP_MODE_NORMAL and calls its append function. During finish, it writes the RLE record.
 *
 *  - OP_MODE_DELTA_RLE: during append, it checks if the delta trend continues with the current
 *    value. If it continues, it increments the repetition counter, otherwise it flushes the
 *    Delta RLE record and move to OP_MODE_NORMAL. During finish, it writes the RLE record.
 *
 * The RR compressor must use the same integer type for decompressing the data as it was used
 * for compressing it. The FastLanes library that is used for encoding the data is _not_ type
 * agnostic, and using different types between encoding/decoding will result in incorrect
 * data being returned.
 */

#define RR_NULL_BITMAP_BYTES_MAX ((GLOBAL_MAX_ROWS_PER_COMPRESSION + 7) / 8)

typedef enum RROpMode
{
	OP_MODE_NORMAL = 0,
	OP_MODE_RLE = 1,
	OP_MODE_DELTA_RLE = 2,
	_OP_MODE_COUNT = 3
} RROpMode;

typedef struct RRCompressorImpl
{
	RROpMode op_mode;
	fl_elem_width_t elem_width;
	uint16 n_valid_elements;

	/* input buffering */
	size_t input_buffer_size;
	void *input_buffer;
	uint16 n_buffered;

	/* analysis */
	RRAnalyzeResult analyze_result;

	/* DELTA_RLE and RLE states */
	uint16 n_rle_elements;
	uint64 rle_base;
	uint64 delta_rle_step;

	/* storage for nulls, lazily allocated */
	uint16 n_nulls;
	uint64 *null_bitmap;

	/* block storage */
	uint16 n_blocks;
	RRBlock *first_block;
	RRBlock *last_block;
} RRCompressorImpl;

typedef struct RRCompressor
{
	Compressor base;
	RRCompressorImpl *impl;
	Oid oid;
} RRCompressor;

/* common helpers: */
static inline void rr_mark_null(RRCompressorImpl *impl);

static void
rr_compressor_free_impl(RRCompressorImpl **implptr)
{
	Assert(implptr != NULL);
	RRCompressorImpl *impl = *implptr;
	Assert(impl != NULL);
	if (impl->input_buffer != NULL)
	{
		pfree(impl->input_buffer);
	}
	if (impl->null_bitmap != NULL)
	{
		pfree(impl->null_bitmap);
	}
	rr_free_blocks(impl->first_block);
	pfree(impl);
	*implptr = NULL;
}

static inline void
rr_add_block(RRCompressorImpl *impl, RRBlock *block)
{
	if (impl->first_block == NULL)
	{
		impl->first_block = block;
		impl->last_block = block;
	}
	else
	{
		impl->last_block->next = block;
		impl->last_block = block;
	}
	impl->n_blocks++;
}

/* assemble the compressed data blob from the encoded blocks */
static void *
rr_compressor_assemble(RRCompressorImpl *impl)
{
	Assert(impl != NULL);
	size_t total_elements = impl->n_valid_elements + impl->n_nulls;

	/* the null size cap is the upper bound, the encoder reports the size it actually used. */
	size_t nulls_size_cap = impl->n_nulls > 0 ? rr_null_size_cap(total_elements) : 0;
	size_t block_size = rr_total_block_size(impl->first_block);
	uint8 *compressed = (uint8 *) palloc(sizeof(RRCompressed) + block_size + nulls_size_cap);
	RRCompressed *rr_compressed = (RRCompressed *) compressed;

	/* prepare the compressed header */
	rr_compressed->compression_algorithm = COMPRESSION_ALGORITHM_RAPID_RACCOON;
	uint8 width_flag = rr_elem_width_to_global_flag(impl->elem_width);
	rr_compressed->flags = RR_FLAG_NULL_RAW | width_flag;
	rr_compressed->valid_count = impl->n_valid_elements;
	rr_compressed->null_count = impl->n_nulls;
	rr_compressed->num_blocks = impl->n_blocks;

	/* serialize compressed blocks: copying the headers and the data
	 * into the final blob. this step is needed because the encoding
	 * has strict alignment requirements, but the result blob has not.
	 * the result blob has the data sequentially, without considering
	 * alignment.
	 */
	uint8 *cursor = rr_compressed->values;
	size_t nulls_size = 0;

	if (nulls_size_cap > 0)
	{
		/* we need to clear the last bits post the last element
		 * because the null bitmap was prefilled with ones
		 */
		if ((total_elements & 63) != 0)
		{
			impl->null_bitmap[(total_elements - 1) / 64] &= ((1ULL << (total_elements & 63)) - 1);
		}

		/* the result flags tell the bitmap format */
		RRGlobalFlags flags = rr_null_bitmap_encode(impl->null_bitmap,
													total_elements,
													impl->n_nulls,
													cursor,
													&nulls_size);

		Assert(nulls_size > 0);
		Assert(nulls_size < nulls_size_cap);
		rr_compressed->flags |= flags;
		cursor += nulls_size;
	}

	SET_VARSIZE(&rr_compressed->vl_len_, sizeof(RRCompressed) + nulls_size + block_size);

	if (impl->n_valid_elements > 0)
	{
		bool serialized = rr_serialize_blocks(impl->first_block, cursor, block_size);
		CheckCompressedData(serialized == true);
	}
	return compressed;
}

/* OP_MODE_NORMAL handlers: */

/* the FOR encoder is mainly FastLanes packing plus header data */
static void
rr_encode_for_block(RRCompressorImpl *impl)
{
	RRAnalyzeResult *ar = &impl->analyze_result;

	/* allocate FOR Block in a single block to reduce allocations */
	size_t body_bytes = fl_required_bytes(impl->n_buffered, ar->W, impl->elem_width);
	size_t alignment = fl_alignment(impl->n_buffered, impl->elem_width);
	size_t padded_block_size = ((sizeof(RRBlock) + alignment - 1) / alignment) * alignment;
	uint8 *block_bytes = palloc_aligned((body_bytes + padded_block_size), alignment, 0);

	RRBlock *block = (RRBlock *) block_bytes;
	memset(block, 0, sizeof(RRBlock));
	block->type = RR_BT_FOR;
	block_bytes += padded_block_size;

	rr_block_for_init(&block->data.for_,
					  impl->n_buffered,
					  ar->vmin,
					  ar->W,
					  impl->elem_width,
					  block_bytes);

	rr_add_block(impl, block);

	/* pack the values */
	if (ar->vmin == 0)
	{
		block->data.for_.packed_body_size = fl_pack(impl->input_buffer,
													block->data.for_.body,
													impl->n_buffered,
													ar->W,
													impl->elem_width);
	}
	else
	{
		block->data.for_.packed_body_size = fl_pack_ffor(impl->input_buffer,
														 block->data.for_.body,
														 impl->n_buffered,
														 ar->W,
														 impl->elem_width,
														 ar->vmin);
	}
}

/* clang-tidy doesn't like the type argument and insists on using
 * parens, but that doesn't compile */
/* NOLINTBEGIN(bugprone-macro-parentheses) */

/* the DFOR encoder needs to produce the encoded `base` values and
 * the encoded delta residuals. the `base` values only need the
 * 'bases_min' and the first 'S' values. the delta residuals are
 * staged as the non-zizgzag encoded deltas. if we need zigzag
 * encoding we wneed to do the encoding and calclualte the min
 * of the zigzagged value for FL encoding.
 *
 * most values are coming from the analysis pass, except the
 * zigzag encoded deltas
 */
#define DEFINE_RR_ENCODE_DFOR_BLOCK(TY, STY)                                                       \
	static void rr_encode_dfor_block_##TY(RRCompressorImpl *impl)                                  \
	{                                                                                              \
		RRAnalyzeResult *ar = &impl->analyze_result;                                               \
		TY *buf = (TY *) impl->input_buffer;                                                       \
		uint32 n = impl->n_buffered;                                                               \
		/* the stride-S deltas were prepared during analysis */                                    \
		TY *deltas = ar->dfor.deltas.TY##_elem;                                                    \
		uint16 S = ar->dfor.S;                                                                     \
		bool zigzag = ar->dfor.zigzag;                                                             \
		/* this is how many elements we have in the `deltas` */                                    \
		uint32 n_delta_residuals = n - S;                                                          \
		/* the base and the W_delta_residuals depend on the zigzag choice */                       \
		int64 delta_residual_base;                                                                 \
		uint8 W_delta_residuals;                                                                   \
		uint32 i;                                                                                  \
		if (zigzag)                                                                                \
		{                                                                                          \
			/* we will need to zigzag encode the deltas calculated                                 \
			 * during the analysis and the delta residual base to be                               \
			 * calculated from the zigzag encoded values */                                        \
			TY zz_lo = (TY) ~(TY) 0;                                                               \
			TY zz_hi = (TY) 0;                                                                     \
			for (i = 0; i < n_delta_residuals; ++i)                                                \
			{                                                                                      \
				TY zz = rr_zigzag_encode_##TY(deltas[i]);                                          \
				deltas[i] = zz;                                                                    \
				zz_lo = (zz < zz_lo) ? zz : zz_lo;                                                 \
				zz_hi = (zz > zz_hi) ? zz : zz_hi;                                                 \
			}                                                                                      \
			/* this will be used as the FL base value */                                           \
			delta_residual_base = (int64) zz_lo;                                                   \
			/* this is the bitwidth used for the FL encoding of the delta reisuals */              \
			W_delta_residuals = rr_bit_width_u64((uint64) (TY) (zz_hi - zz_lo));                   \
		}                                                                                          \
		else                                                                                       \
		{                                                                                          \
			delta_residual_base = ar->dfor.delta_residual_base;                                    \
			W_delta_residuals = ar->dfor.W_delta_residuals;                                        \
		}                                                                                          \
                                                                                                   \
		int64 bases_min = ar->dfor.bases_min;                                                      \
		uint8 W_bases = ar->dfor.W_bases;                                                          \
                                                                                                   \
		/* single allocation: padded RRBlock + both packed regions */                              \
		size_t bases_bytes = fl_required_bytes(S, W_bases, impl->elem_width);                      \
		size_t delta_residual_bytes =                                                              \
			fl_required_bytes(n_delta_residuals, W_delta_residuals, impl->elem_width);             \
		/* calculate the alignment based on 'n', not on 'S' or 'n_delta_residuals' */              \
		size_t alignment = fl_alignment(n, impl->elem_width);                                      \
		size_t padded_block = ((sizeof(RRBlock) + alignment - 1) / alignment) * alignment;         \
		size_t padded_bases = ((bases_bytes + alignment - 1) / alignment) * alignment;             \
		/* single aligned allocation for the whole block */                                        \
		uint8 *mem =                                                                               \
			palloc_aligned(padded_block + padded_bases + delta_residual_bytes, alignment, 0);      \
		RRBlock *block = (RRBlock *) mem;                                                          \
		memset(block, 0, sizeof(RRBlock));                                                         \
		block->type = RR_BT_DFOR;                                                                  \
		rr_block_dfor_init(&block->data.dfor,                                                      \
						   n,                                                                      \
						   S,                                                                      \
						   bases_min,                                                              \
						   W_bases,                                                                \
						   delta_residual_base,                                                    \
						   W_delta_residuals,                                                      \
						   zigzag,                                                                 \
						   impl->elem_width,                                                       \
						   mem + padded_block,                                                     \
						   mem + padded_block + padded_bases);                                     \
		rr_add_block(impl, block);                                                                 \
		/* bases are not encoded if their bitwidth is zero */                                      \
		if (W_bases > 0)                                                                           \
		{                                                                                          \
			if (bases_min == 0)                                                                    \
			{                                                                                      \
				block->data.dfor.packed_bases_size =                                               \
					fl_pack(buf, block->data.dfor.bases_body, S, W_bases, impl->elem_width);       \
			}                                                                                      \
			else                                                                                   \
			{                                                                                      \
				block->data.dfor.packed_bases_size =                                               \
					fl_pack_ffor(buf,                                                              \
								 block->data.dfor.bases_body,                                      \
								 S,                                                                \
								 W_bases,                                                          \
								 impl->elem_width,                                                 \
								 (uint64) block->data.dfor.base_value);                            \
			}                                                                                      \
		}                                                                                          \
		/* deltas are not encoded if their bitwidth is zero */                                     \
		if (W_delta_residuals > 0)                                                                 \
		{                                                                                          \
			if (delta_residual_base == 0)                                                          \
			{                                                                                      \
				block->data.dfor.packed_delta_residual_size =                                      \
					fl_pack(deltas,                                                                \
							block->data.dfor.delta_residual_body,                                  \
							n_delta_residuals,                                                     \
							W_delta_residuals,                                                     \
							impl->elem_width);                                                     \
			}                                                                                      \
			else                                                                                   \
			{                                                                                      \
				block->data.dfor.packed_delta_residual_size =                                      \
					fl_pack_ffor(deltas,                                                           \
								 block->data.dfor.delta_residual_body,                             \
								 n_delta_residuals,                                                \
								 W_delta_residuals,                                                \
								 impl->elem_width,                                                 \
								 (uint64) delta_residual_base);                                    \
			}                                                                                      \
		}                                                                                          \
	}

/* the DICT encoder needs to encode the keys and the indices of the
 * dictionary. the analyzer prepares both the keys and the indices
 * so this stage is simply packing them.
 */
#define DEFINE_RR_ENCODE_DICT_BLOCK(TY, FL_T)                                                      \
	static void rr_encode_dict_block_##TY(RRCompressorImpl *impl)                                  \
	{                                                                                              \
		RRAnalyzeResult *ar = &impl->analyze_result;                                               \
		uint32 n = impl->n_buffered;                                                               \
		uint16 K = ar->dict.K;                                                                     \
		uint8 w_idx = ar->dict.W_idx;                                                              \
		bool zigzag_idxs = ar->dict.zigzag_idxs;                                                   \
		fl_elem_width_t idx_T = zigzag_idxs ? FL_ELEM_W16 : FL_ELEM_W8;                            \
		fl_elem_width_t max_width =                                                                \
			(uint8) impl->elem_width < (uint8) idx_T ? idx_T : impl->elem_width;                   \
		TY key_vals[RR_FIXED_BLOCK_MAX_COUNT];                                                     \
		const TY *keys_src;                                                                        \
		uint32 staged_keys = fl_input_count(K, FL_T);                                              \
		uint32 i;                                                                                  \
		if (FL_T == FL_ELEM_W64)                                                                   \
		{                                                                                          \
			/* the keys need no narrowing: pack directly from the analyze                          \
			 * result. fl_pack reads fl_input_count() elements, so zero                            \
			 * the padding above K */                                                              \
			if (staged_keys > K)                                                                   \
			{                                                                                      \
				memset(&ar->dict.keys[K], 0, (size_t) (staged_keys - K) * sizeof(uint64));         \
			}                                                                                      \
			keys_src = (const TY *) ar->dict.keys;                                                 \
		}                                                                                          \
		else                                                                                       \
		{                                                                                          \
			for (i = 0; i < K; ++i)                                                                \
			{                                                                                      \
				/* narrow the keys from the analysis to TY,                                        \
				 * because the RRAnalyzeResult is not parametrized                                 \
				 * by TY, and 'ar->dict.keys' is uint64[]  */                                      \
				key_vals[i] = (TY) ar->dict.keys[i];                                               \
			}                                                                                      \
			/* fl_pack reads fl_input_count() elements, so zero                                    \
			 * the padding above K */                                                              \
			if (staged_keys > K)                                                                   \
			{                                                                                      \
				memset(key_vals + K, 0, (size_t) (staged_keys - K) * sizeof(TY));                  \
			}                                                                                      \
			keys_src = key_vals;                                                                   \
		}                                                                                          \
                                                                                                   \
		/* single allocation: padded RRBlock + both packed regions */                              \
		size_t keys_bytes = fl_required_bytes(K, ar->W, impl->elem_width);                         \
		size_t idx_bytes = fl_required_bytes(n, w_idx, idx_T);                                     \
		size_t alignment = fl_alignment(n, max_width);                                             \
		size_t padded_block = ((sizeof(RRBlock) + alignment - 1) / alignment) * alignment;         \
		size_t padded_keys = ((keys_bytes + alignment - 1) / alignment) * alignment;               \
		uint8 *mem = palloc_aligned(padded_block + padded_keys + idx_bytes, alignment, 0);         \
		RRBlock *block = (RRBlock *) mem;                                                          \
		memset(block, 0, sizeof(RRBlock));                                                         \
		block->type = RR_BT_DICT;                                                                  \
		rr_block_dict_init(&block->data.dict,                                                      \
						   n,                                                                      \
						   K,                                                                      \
						   ar->vmin,                                                               \
						   ar->W,                                                                  \
						   w_idx,                                                                  \
						   zigzag_idxs,                                                            \
						   impl->elem_width,                                                       \
						   mem + padded_block,                                                     \
						   mem + padded_block + padded_keys);                                      \
		rr_add_block(impl, block);                                                                 \
		/* the dict body always packs without a FOR base because the                               \
		 * analyzer already subtracted the 'ar->vmin' when it produced                             \
		 * the 'ar->dict.keys' array */                                                            \
		block->data.dict.packed_keys_size =                                                        \
			fl_pack(keys_src, block->data.dict.keys_body, K, ar->W, impl->elem_width);             \
		/* the idx elem width depends on the zigzag choice, in both                                \
		 * cases we pack without a base value, because zero is                                     \
		 * the min idx */                                                                          \
		block->data.dict.packed_idx_size =                                                         \
			zigzag_idxs ?                                                                          \
				fl_pack(ar->dict.idx_zz, block->data.dict.idx_body, n, w_idx, FL_ELEM_W16) :       \
				fl_pack(ar->dict.idx, block->data.dict.idx_body, n, w_idx, FL_ELEM_W8);            \
	}

/* the PFOR encoder needs to encode 1) the base/body values from which
 * we shave off the 2) exceptions. the exceptions are the high bits cut
 * off from the body values at a given 'b' threshold. we only store
 * the exceptions where the bits above 'b' are non-zero, so we also
 * need the 3) positions.
 *
 * the analysis provides the body values in the form of the 'residuals',
 * that is all input values minus the min of the values. it also
 * provides the 'b' bit-threshold. the encoder needs to calculate the
 * rest, like the exception and position arrays.
 */
#define DEFINE_RR_ENCODE_PFOR_BLOCK(TY, FL_T)                                                      \
	static void rr_encode_pfor_block_##TY(RRCompressorImpl *impl)                                  \
	{                                                                                              \
		RRAnalyzeResult *ar = &impl->analyze_result;                                               \
		uint32 n = impl->n_buffered;                                                               \
		uint8 b = ar->pfor.b;                                                                      \
		uint8 W_exc = ar->pfor.W_exc;                                                              \
		uint8 n_exc = ar->pfor.n_exc;                                                              \
		/* analyzer provides the residuals, but we will need to calculate                          \
		 * the body, exception and position values */                                              \
		TY body_vals[RR_FIXED_BLOCK_MAX_COUNT];                                                    \
		/* sized for the FL encoding */                                                            \
		TY exc_vals[RR_PFOR_EXC_STAGING_COUNT(FL_T)] = { 0 };                                      \
		uint8 positions[RR_PFOR_MAX_EXCEPTIONS];                                                   \
		uint64 mask = (b == 0) ? 0 : (~(uint64) 0 >> (64 - b));                                    \
		uint32 e = 0;                                                                              \
		uint32 i;                                                                                  \
		for (i = 0; i < n; ++i)                                                                    \
		{                                                                                          \
			uint64 res = ar->residuals[i];                                                         \
			uint64 high = res >> b;                                                                \
			body_vals[i] = (TY) (res & mask);                                                      \
			if (high != 0)                                                                         \
			{                                                                                      \
				if (e < RR_PFOR_MAX_EXCEPTIONS)                                                    \
				{                                                                                  \
					positions[e] = (uint8) i;                                                      \
					exc_vals[e] = (TY) high;                                                       \
				}                                                                                  \
				e++;                                                                               \
			}                                                                                      \
		}                                                                                          \
		CheckCompressedData(e == (uint32) n_exc);                                                  \
                                                                                                   \
		uint32 staged_body = fl_input_count(n, FL_T);                                              \
		/* fl_pack reads fl_input_count() elements, so zero the tier                               \
		 * padding above n */                                                                      \
		if (staged_body > n)                                                                       \
		{                                                                                          \
			memset(body_vals + n, 0, (size_t) (staged_body - n) * sizeof(TY));                     \
		}                                                                                          \
		/* single allocation: padded RRBlock + body + exception stream;                            \
		 * the positions live in the block struct */                                               \
		size_t body_bytes = fl_required_bytes(n, b, impl->elem_width);                             \
		size_t exc_bytes = fl_required_bytes(n_exc, W_exc, impl->elem_width);                      \
		size_t alignment = fl_alignment(n, impl->elem_width);                                      \
		size_t padded_block = ((sizeof(RRBlock) + alignment - 1) / alignment) * alignment;         \
		size_t padded_body = ((body_bytes + alignment - 1) / alignment) * alignment;               \
		uint8 *mem = palloc_aligned(padded_block + padded_body + exc_bytes, alignment, 0);         \
		RRBlock *block = (RRBlock *) mem;                                                          \
		memset(block, 0, sizeof(RRBlock));                                                         \
		block->type = ar->type;                                                                    \
		rr_block_pfor_init(&block->data.pfor,                                                      \
						   n,                                                                      \
						   ar->vmin,                                                               \
						   b,                                                                      \
						   W_exc,                                                                  \
						   n_exc,                                                                  \
						   positions,                                                              \
						   impl->elem_width,                                                       \
						   mem + padded_block,                                                     \
						   mem + padded_block + padded_body);                                      \
		rr_add_block(impl, block);                                                                 \
		if (b > 0)                                                                                 \
		{                                                                                          \
			/* only pack the body if b > 0 */                                                      \
			block->data.pfor.packed_body_size =                                                    \
				fl_pack(body_vals, block->data.pfor.body, n, b, impl->elem_width);                 \
		}                                                                                          \
		block->data.pfor.packed_exc_size =                                                         \
			fl_pack(exc_vals, block->data.pfor.exc_body, n_exc, W_exc, impl->elem_width);          \
	}

#define DEFINE_RR_ENCODE_BLOCK(TY, STY)                                                            \
	static void rr_encode_block_##TY(RRCompressorImpl *impl)                                       \
	{                                                                                              \
		if (impl->n_buffered == 0)                                                                 \
		{                                                                                          \
			/* when finish was called with a multiple of 256 elements */                           \
			return;                                                                                \
		}                                                                                          \
		/* analysis of the buffered data */                                                        \
		STY *buf = (STY *) impl->input_buffer;                                                     \
		RRAnalyzeResult *ar = &impl->analyze_result;                                               \
		/* launch the analysis */                                                                  \
		rr_analyze_##TY(buf, impl->n_buffered, impl->elem_width, ar);                              \
		switch (ar->type)                                                                          \
		{                                                                                          \
			case RR_BT_RLE:                                                                        \
				/* the RLE decision can only happen in normal mode if the                          \
				 * buffered element count is less than the checkpoint. it                          \
				 * can only happen if we finish buffering early */                                 \
				Assert(impl->n_buffered <= RR_RLE_CHECKPOINT);                                     \
				impl->rle_base = (uint64) ar->vmin;                                                \
				impl->n_rle_elements = impl->n_buffered;                                           \
				impl->delta_rle_step = 0;                                                          \
				rr_encode_rle_block_##TY(impl);                                                    \
				return;                                                                            \
			case RR_BT_FOR:                                                                        \
				rr_encode_for_block(impl);                                                         \
				return;                                                                            \
			case RR_BT_DFOR:                                                                       \
				rr_encode_dfor_block_##TY(impl);                                                   \
				break;                                                                             \
			case RR_BT_DICT:                                                                       \
				rr_encode_dict_block_##TY(impl);                                                   \
				break;                                                                             \
			case RR_BT_PFOR:                                                                       \
				rr_encode_pfor_block_##TY(impl);                                                   \
				break;                                                                             \
			default:                                                                               \
				Assert(false);                                                                     \
				break;                                                                             \
		};                                                                                         \
	}

/* this is the NORMAL mode append handler, where we do the RLE checks
 * at the checkpoints so we may transit to the other RLE modes
 */
#define DEFINE_RR_APPEND_VAL(TY)                                                                   \
	static RROpMode rr_append_val_##TY(RRCompressorImpl *impl, TY v)                               \
	{                                                                                              \
		TY *buf = (TY *) impl->input_buffer;                                                       \
		buf[impl->n_buffered] = v;                                                                 \
		++(impl->n_buffered);                                                                      \
                                                                                                   \
		/* at the DELTA RLE checkpoint, we check if this is a constant non-zero                    \
		 * slope of the values. if so, move to OP_MODE_DELTA_RLE.  */                              \
		if (impl->n_buffered == RR_DELTA_RLE_CHECKPOINT)                                           \
		{                                                                                          \
			TY d = (TY) (buf[1] - buf[0]);                                                         \
			if (d != 0)                                                                            \
			{                                                                                      \
				bool is_delta_rle = true;                                                          \
				for (int i = 2; is_delta_rle && i < RR_DELTA_RLE_CHECKPOINT; ++i)                  \
				{                                                                                  \
					is_delta_rle = (TY) (buf[i] - buf[i - 1]) == d;                                \
				}                                                                                  \
				if (is_delta_rle)                                                                  \
				{                                                                                  \
					impl->rle_base = (uint64) buf[0];                                              \
					impl->n_rle_elements = RR_DELTA_RLE_CHECKPOINT;                                \
					impl->delta_rle_step = (uint64) d;                                             \
					impl->op_mode = OP_MODE_DELTA_RLE;                                             \
					impl->n_buffered = 0;                                                          \
				}                                                                                  \
			}                                                                                      \
		}                                                                                          \
		/* at the RLE checkpoint check if this is a constant RLE run */                            \
		else if (impl->n_buffered == RR_RLE_CHECKPOINT)                                            \
		{                                                                                          \
			bool is_rle = true;                                                                    \
			for (int i = 1; is_rle && i < RR_RLE_CHECKPOINT; ++i)                                  \
			{                                                                                      \
				is_rle = buf[i] == buf[0];                                                         \
			}                                                                                      \
			if (is_rle)                                                                            \
			{                                                                                      \
				impl->rle_base = (uint64) buf[0];                                                  \
				impl->n_rle_elements = RR_RLE_CHECKPOINT;                                          \
				impl->delta_rle_step = 0;                                                          \
				impl->op_mode = OP_MODE_RLE;                                                       \
				impl->n_buffered = 0;                                                              \
			}                                                                                      \
		}                                                                                          \
		/* when the buffer is full, we need to encode the current block */                         \
		else if (impl->n_buffered == RR_FIXED_BLOCK_MAX_COUNT)                                     \
		{                                                                                          \
			rr_encode_block_##TY(impl);                                                            \
			impl->n_buffered = 0;                                                                  \
		}                                                                                          \
		return impl->op_mode;                                                                      \
	}

/* OP_MODE_RLE and OP_MODE_DELTA_RLE handlers: */
#define DEFINE_RR_ENCODE_RLE_BLOCK(TY)                                                             \
	static void rr_encode_rle_block_##TY(RRCompressorImpl *impl)                                   \
	{                                                                                              \
		RRBlock *block = palloc0(sizeof(RRBlock));                                                 \
		block->type = RR_BT_RLE;                                                                   \
		rr_block_rle_init(&block->data.rle,                                                        \
						  impl->n_rle_elements,                                                    \
						  (int64) impl->rle_base,                                                  \
						  (int64) impl->delta_rle_step,                                            \
						  impl->elem_width);                                                       \
		rr_add_block(impl, block);                                                                 \
		impl->rle_base = 0;                                                                        \
		impl->n_rle_elements = 0;                                                                  \
		impl->delta_rle_step = 0;                                                                  \
	}

#define DEFINE_RR_APPEND_RLE_VAL(TY)                                                               \
	static RROpMode rr_append_rle_val_##TY(RRCompressorImpl *impl, TY v)                           \
	{                                                                                              \
		uint64 current_value = (uint64) v;                                                         \
		if (current_value == impl->rle_base)                                                       \
		{                                                                                          \
			++(impl->n_rle_elements);                                                              \
		}                                                                                          \
		else                                                                                       \
		{                                                                                          \
			/* flush RLE block */                                                                  \
			rr_encode_rle_block_##TY(impl);                                                        \
			impl->op_mode = OP_MODE_NORMAL;                                                        \
			/* append the current value */                                                         \
			TY *buf = (TY *) impl->input_buffer;                                                   \
			buf[0] = v;                                                                            \
			impl->n_buffered = 1;                                                                  \
		}                                                                                          \
		return impl->op_mode;                                                                      \
	}

#define DEFINE_RR_APPEND_DELTA_RLE_VAL(TY)                                                         \
	static RROpMode rr_append_delta_rle_val_##TY(RRCompressorImpl *impl, TY v)                     \
	{                                                                                              \
		TY expected = (TY) (impl->rle_base + impl->n_rle_elements * impl->delta_rle_step);         \
		if (v == expected)                                                                         \
		{                                                                                          \
			++(impl->n_rle_elements);                                                              \
		}                                                                                          \
		else                                                                                       \
		{                                                                                          \
			/* flush DELTA RLE block */                                                            \
			rr_encode_rle_block_##TY(impl);                                                        \
			impl->op_mode = OP_MODE_NORMAL;                                                        \
			/* append the current value */                                                         \
			TY *buf = (TY *) impl->input_buffer;                                                   \
			buf[0] = v;                                                                            \
			impl->n_buffered = 1;                                                                  \
		}                                                                                          \
		return impl->op_mode;                                                                      \
	}

#define DEFINE_RR_COMPRESSOR_IMPL_ALLOC(TY, FL_T)                                                  \
	static RRCompressorImpl *rr_compressor_impl_alloc_##TY()                                       \
	{                                                                                              \
		size_t input_alignment = fl_alignment(RR_FIXED_BLOCK_MAX_COUNT, FL_T);                     \
		RRCompressorImpl *ret = palloc0(sizeof(*ret));                                             \
		ret->op_mode = OP_MODE_NORMAL;                                                             \
		ret->elem_width = FL_T;                                                                    \
		ret->input_buffer_size = fl_input_bytes(RR_FIXED_BLOCK_MAX_COUNT, FL_T);                   \
		ret->input_buffer = palloc_aligned(ret->input_buffer_size, input_alignment, 0);            \
		return ret;                                                                                \
	}

#define DEFINE_RR_OPMODE_HANDLERS_MAIN(TY, STY, FL_T)                                              \
	DEFINE_RR_ENCODE_RLE_BLOCK(TY)                                                                 \
	DEFINE_RR_ENCODE_DFOR_BLOCK(TY, STY)                                                           \
	DEFINE_RR_ENCODE_DICT_BLOCK(TY, FL_T)                                                          \
	DEFINE_RR_ENCODE_PFOR_BLOCK(TY, FL_T)                                                          \
	DEFINE_RR_ENCODE_BLOCK(TY, STY)                                                                \
	DEFINE_RR_APPEND_VAL(TY)                                                                       \
	DEFINE_RR_APPEND_RLE_VAL(TY)                                                                   \
	DEFINE_RR_APPEND_DELTA_RLE_VAL(TY)                                                             \
	DEFINE_RR_COMPRESSOR_IMPL_ALLOC(TY, FL_T)

DEFINE_RR_OPMODE_HANDLERS_MAIN(uint16, int16, FL_ELEM_W16)
DEFINE_RR_OPMODE_HANDLERS_MAIN(uint32, int32, FL_ELEM_W32)
DEFINE_RR_OPMODE_HANDLERS_MAIN(uint64, int64, FL_ELEM_W64)

#undef DEFINE_RR_APPEND_VAL
#undef DEFINE_RR_ENCODE_BLOCK
#undef DEFINE_RR_APPEND_RLE_VAL
#undef DEFINE_RR_ENCODE_RLE_BLOCK
#undef DEFINE_RR_APPEND_DELTA_RLE_VAL
#undef DEFINE_RR_ENCODE_DELTA_RLE_BLOCK
#undef DEFINE_RR_COMPRESSOR_IMPL_ALLOC
#undef DEFINE_RR_OPMODE_HANDLERS_MAIN
#undef DEFINE_RR_ENCODE_DFOR_BLOCK
#undef DEFINE_RR_ENCODE_DICT_BLOCK
#undef DEFINE_RR_ENCODE_PFOR_BLOCK

#define RR_COMPRESSOR_APPEND_FN(SUFFIX, TY, DATUM_GET)                                             \
	static void rr_append_##SUFFIX(Compressor *compressor, Datum val)                              \
	{                                                                                              \
		TY v = DATUM_GET(val);                                                                     \
		RRCompressor *ci = (RRCompressor *) compressor;                                            \
		if (ci->impl == NULL)                                                                      \
		{                                                                                          \
			ci->impl = rr_compressor_impl_alloc_##TY();                                            \
			Assert(rr_oid_to_elem_width_t(ci->oid) == ci->impl->elem_width);                       \
		}                                                                                          \
		++(ci->impl->n_valid_elements);                                                            \
		/* call the right handler for the current opmode */                                        \
		switch (ci->impl->op_mode)                                                                 \
		{                                                                                          \
			case OP_MODE_NORMAL:                                                                   \
				rr_append_val_##TY(ci->impl, v);                                                   \
				break;                                                                             \
			case OP_MODE_RLE:                                                                      \
				rr_append_rle_val_##TY(ci->impl, v);                                               \
				break;                                                                             \
			default:                                                                               \
				rr_append_delta_rle_val_##TY(ci->impl, v);                                         \
				break;                                                                             \
		}                                                                                          \
	}

#define RR_COMPRESSOR_APPEND_NULL_FN(SUFFIX, TY)                                                   \
	static void rr_append_null_##SUFFIX(Compressor *compressor)                                    \
	{                                                                                              \
		RRCompressor *ci = (RRCompressor *) compressor;                                            \
		if (ci->impl == NULL)                                                                      \
		{                                                                                          \
			ci->impl = rr_compressor_impl_alloc_##TY();                                            \
			fl_elem_width_t expected_width PG_USED_FOR_ASSERTS_ONLY =                              \
				rr_oid_to_elem_width_t(ci->oid);                                                   \
			Assert(expected_width == ci->impl->elem_width);                                        \
		}                                                                                          \
		/* common null handler */                                                                  \
		rr_mark_null(ci->impl);                                                                    \
	}

#define RR_COMPRESSOR_FINISH_RESET_FN(SUFFIX, TY)                                                  \
	static void *rr_finish_and_reset_##SUFFIX(Compressor *compressor)                              \
	{                                                                                              \
		RRCompressor *ci = (RRCompressor *) compressor;                                            \
		if (ci == NULL || ci->impl == NULL)                                                        \
		{                                                                                          \
			return NULL;                                                                           \
		}                                                                                          \
		if (ci->impl->n_valid_elements == 0)                                                       \
		{                                                                                          \
			rr_compressor_free_impl(&ci->impl);                                                    \
			return NULL;                                                                           \
		}                                                                                          \
		/* make sure any buffered data gets flushed */                                             \
		if (ci->impl->op_mode == OP_MODE_NORMAL)                                                   \
		{                                                                                          \
			rr_encode_block_##TY(ci->impl);                                                        \
		}                                                                                          \
		else                                                                                       \
		{                                                                                          \
			rr_encode_rle_block_##TY(ci->impl);                                                    \
		}                                                                                          \
		/* assemble the final results from the encoded blocks  */                                  \
		void *result = rr_compressor_assemble(ci->impl);                                           \
		rr_compressor_free_impl(&ci->impl);                                                        \
		return result;                                                                             \
	}

#define RR_COMPRESSOR_STRUCT_DEF(SUFFIX)                                                           \
	const Compressor rr_compressor_##SUFFIX = {                                                    \
		.append_val = rr_append_##SUFFIX,                                                          \
		.append_null = rr_append_null_##SUFFIX,                                                    \
		.is_full = NULL,                                                                           \
		.finish = rr_finish_and_reset_##SUFFIX,                                                    \
	};

#define RR_COMPRESSOR_STRUCT(SUFFIX, TY, DATUM_GET)                                                \
	RR_COMPRESSOR_APPEND_FN(SUFFIX, TY, DATUM_GET)                                                 \
	RR_COMPRESSOR_APPEND_NULL_FN(SUFFIX, TY)                                                       \
	RR_COMPRESSOR_FINISH_RESET_FN(SUFFIX, TY)                                                      \
	RR_COMPRESSOR_STRUCT_DEF(SUFFIX)

RR_COMPRESSOR_STRUCT(uint16, uint16, DatumGetInt16)
RR_COMPRESSOR_STRUCT(uint32, uint32, DatumGetInt32)
RR_COMPRESSOR_STRUCT(uint64, uint64, DatumGetInt64)
RR_COMPRESSOR_STRUCT(date, uint32, DatumGetDateADT)
RR_COMPRESSOR_STRUCT(timestamp, uint64, DatumGetTimestamp)
RR_COMPRESSOR_STRUCT(timestamptz, uint64, DatumGetTimestampTz)
#undef RR_COMPRESSOR_STRUCT
#undef RR_COMPRESSOR_APPEND_FN
#undef RR_COMPRESSOR_FINISH_RESET_FN
#undef RR_COMPRESSOR_STRUCT_DEF

/* NOLINTEND(bugprone-macro-parentheses) */

extern Compressor *
rapid_raccoon_compressor_for_type(Oid element_type)
{
	RRCompressor *compressor = palloc(sizeof(*compressor));
	switch (element_type)
	{
		case INT2OID:
			*compressor = (RRCompressor){ .base = rr_compressor_uint16 };
			break;
		case INT4OID:
			*compressor = (RRCompressor){ .base = rr_compressor_uint32 };
			break;
		case INT8OID:
			*compressor = (RRCompressor){ .base = rr_compressor_uint64 };
			break;
		case DATEOID:
			*compressor = (RRCompressor){ .base = rr_compressor_date };
			break;
		case TIMESTAMPOID:
			*compressor = (RRCompressor){ .base = rr_compressor_timestamp };
			break;
		case TIMESTAMPTZOID:
			*compressor = (RRCompressor){ .base = rr_compressor_timestamptz };
			break;
		default:
			elog(ERROR,
				 "invalid type for rapid-raccoon compressor \"%s\"",
				 format_type_be(element_type));
	};

	compressor->oid = element_type;
	return &compressor->base;
}

static inline void
rr_mark_null(RRCompressorImpl *impl)
{
	/* The bitmap is allocated at full row-cap capacity and filled
	 * all-valid, so valid entries never need a write -- only NULLs
	 * land here. */
	uint32 pos = (uint32) impl->n_valid_elements + impl->n_nulls;
	Ensure(pos < GLOBAL_MAX_ROWS_PER_COMPRESSION, "batch exceeds the compression row cap");

	if (impl->null_bitmap == NULL)
	{
		impl->null_bitmap = palloc(RR_NULL_BITMAP_BYTES_MAX);
		memset(impl->null_bitmap, 0xFF, RR_NULL_BITMAP_BYTES_MAX);
	}

	arrow_set_row_validity(impl->null_bitmap, pos, false);
	++impl->n_nulls;
}

bool
rapid_raccoon_compressed_has_nulls(const CompressedDataHeader *header)
{
	const RRCompressed *c = (const RRCompressed *) header;
	return c->null_count > 0;
}

Datum
tsl_rapid_raccoon_compressor_append(PG_FUNCTION_ARGS)
{
	MemoryContext old_context;
	MemoryContext agg_context;
	RRCompressor *compressor = (RRCompressor *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));

	if (!AggCheckCallContext(fcinfo, &agg_context))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "tsl_rapid_raccoon_compressor_append called in non-aggregate context");
	}

	old_context = MemoryContextSwitchTo(agg_context);

	if (compressor == NULL)
	{
		compressor = (RRCompressor *) rapid_raccoon_compressor_for_type(
			get_fn_expr_argtype(fcinfo->flinfo, 1));
	}

	if (PG_ARGISNULL(1))
	{
		compressor->base.append_null(&compressor->base);
	}
	else
	{
		compressor->base.append_val(&compressor->base, PG_GETARG_DATUM(1));
	}

	MemoryContextSwitchTo(old_context);
	PG_RETURN_POINTER(compressor);
}

Datum
tsl_rapid_raccoon_compressor_finish(PG_FUNCTION_ARGS)
{
	RRCompressor *compressor = PG_ARGISNULL(0) ? NULL : (RRCompressor *) PG_GETARG_POINTER(0);
	void *compressed;
	if (compressor == NULL)
	{
		PG_RETURN_NULL();
	}

	compressed = compressor->base.finish(&compressor->base);
	if (compressed == NULL)
	{
		PG_RETURN_NULL();
	}
	PG_RETURN_POINTER(compressed);
}
