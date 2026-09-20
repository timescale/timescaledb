/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * aic_impl.h -- per-type compressor opmode handler template.
 *
 * Usage (only from aic.c, after the shared helpers):
 *     #define AIC_TY   uint16               // uint16/uint32/uint64
 *     #define AIC_STY  int16                // matching signed type
 *     #define AIC_FL_T FL_ELEM_W16          // matching fl_elem_width_t
 *     #include "aic_impl.h"
 *
 * Emits these functions per parameter set (all static):
 *
 *   aic_encode_rle_block_{AIC_TY}
 *   aic_encode_dfor_block_{AIC_TY}
 *   aic_encode_dict_block_{AIC_TY}
 *   aic_encode_pfor_block_{AIC_TY}
 *   aic_encode_block_{AIC_TY}
 *   aic_append_val_{AIC_TY}
 *   aic_append_rle_val_{AIC_TY}
 *   aic_append_delta_rle_val_{AIC_TY}
 *   aic_compressor_impl_alloc_{AIC_TY}
 *
 * This file is private to aic.c: it relies on the file-local helpers
 * (aic_add_block, aic_encode_for_block), types (AICCompressorImpl,
 * AICOpMode) and the constants from aic/aic_internal.h. It has no
 * include guard by design, it is re-included once per type.
 */

#if !defined(AIC_TY) || !defined(AIC_STY) || !defined(AIC_FL_T)
#error                                                                                             \
	"aic_impl.h requires AIC_TY (uint16/uint32/uint64), AIC_STY (matching signed type) and AIC_FL_T (matching fl_elem_width_t)"
#endif

/* Symbol-name plumbing: two-level paste so AIC_TY expands before ## */
#define AIC_PASTE2_(a, b) a##b
#define AIC_PASTE2(a, b) AIC_PASTE2_(a, b)
#define AIC_TFN(prefix) AIC_PASTE2(prefix, AIC_TY)

/* OP_MODE_RLE and OP_MODE_DELTA_RLE handlers: */
static void
AIC_TFN(aic_encode_rle_block_)(AICCompressorImpl *impl)
{
	AICBlock *block = palloc0(sizeof(AICBlock));
	block->type = AIC_BT_RLE;
	aic_block_rle_init(&block->data.rle,
					   impl->n_rle_elements,
					   (int64) impl->rle_base,
					   (int64) impl->delta_rle_step,
					   impl->elem_width);
	aic_add_block(impl, block);
	impl->rle_base = 0;
	impl->n_rle_elements = 0;
	impl->delta_rle_step = 0;
}

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
static void
AIC_TFN(aic_encode_dfor_block_)(AICCompressorImpl *impl)
{
	AICAnalyzeResult *ar = &impl->analyze_result;
	AIC_TY *buf = (AIC_TY *) impl->input_buffer;
	uint32 n = impl->n_buffered;
	/* the stride-S deltas were prepared during analysis */
	AIC_TY *deltas = ar->dfor.deltas.AIC_PASTE2(AIC_TY, _elem);
	uint16 S = ar->dfor.S;
	bool zigzag = ar->dfor.zigzag;
	/* this is how many elements we have in the `deltas` */
	uint32 n_delta_residuals = n - S;
	/* the base and the W_delta_residuals depend on the zigzag choice */
	int64 delta_residual_base;
	uint8 W_delta_residuals;
	uint32 i;
	if (zigzag)
	{
		/* we will need to zigzag encode the deltas calculated
		 * during the analysis and the delta residual base to be
		 * calculated from the zigzag encoded values */
		AIC_TY zz_lo = (AIC_TY) ~(AIC_TY) 0;
		AIC_TY zz_hi = (AIC_TY) 0;
		for (i = 0; i < n_delta_residuals; ++i)
		{
			AIC_TY zz = AIC_TFN(aic_zigzag_encode_)(deltas[i]);
			deltas[i] = zz;
			zz_lo = (zz < zz_lo) ? zz : zz_lo;
			zz_hi = (zz > zz_hi) ? zz : zz_hi;
		}
		/* this will be used as the FL base value */
		delta_residual_base = (int64) zz_lo;
		/* this is the bitwidth used for the FL encoding of the delta reisuals */
		W_delta_residuals = aic_bit_width_u64((uint64) (AIC_TY) (zz_hi - zz_lo));
	}
	else
	{
		delta_residual_base = ar->dfor.delta_residual_base;
		W_delta_residuals = ar->dfor.W_delta_residuals;
	}

	int64 bases_min = ar->dfor.bases_min;
	uint8 W_bases = ar->dfor.W_bases;

	/* single allocation: padded AICBlock + both packed regions */
	size_t bases_bytes = fl_required_bytes(S, W_bases, impl->elem_width);
	size_t delta_residual_bytes =
		fl_required_bytes(n_delta_residuals, W_delta_residuals, impl->elem_width);
	/* calculate the alignment based on 'n', not on 'S' or 'n_delta_residuals' */
	size_t alignment = fl_alignment(n, impl->elem_width);
	size_t padded_block = ((sizeof(AICBlock) + alignment - 1) / alignment) * alignment;
	size_t padded_bases = ((bases_bytes + alignment - 1) / alignment) * alignment;
	/* single aligned allocation for the whole block */
	uint8 *mem = palloc_aligned(padded_block + padded_bases + delta_residual_bytes, alignment, 0);
	AICBlock *block = (AICBlock *) mem;
	memset(block, 0, sizeof(AICBlock));
	block->type = AIC_BT_DFOR;
	aic_block_dfor_init(&block->data.dfor,
						n,
						S,
						bases_min,
						W_bases,
						delta_residual_base,
						W_delta_residuals,
						zigzag,
						impl->elem_width,
						mem + padded_block,
						mem + padded_block + padded_bases);
	aic_add_block(impl, block);
	/* bases are not encoded if their bitwidth is zero */
	if (W_bases > 0)
	{
		if (bases_min == 0)
		{
			block->data.dfor.packed_bases_size =
				fl_pack(buf, block->data.dfor.bases_body, S, W_bases, impl->elem_width);
		}
		else
		{
			block->data.dfor.packed_bases_size = fl_pack_ffor(buf,
															  block->data.dfor.bases_body,
															  S,
															  W_bases,
															  impl->elem_width,
															  (uint64) block->data.dfor.base_value);
		}
	}
	/* deltas are not encoded if their bitwidth is zero */
	if (W_delta_residuals > 0)
	{
		if (delta_residual_base == 0)
		{
			block->data.dfor.packed_delta_residual_size =
				fl_pack(deltas,
						block->data.dfor.delta_residual_body,
						n_delta_residuals,
						W_delta_residuals,
						impl->elem_width);
		}
		else
		{
			block->data.dfor.packed_delta_residual_size =
				fl_pack_ffor(deltas,
							 block->data.dfor.delta_residual_body,
							 n_delta_residuals,
							 W_delta_residuals,
							 impl->elem_width,
							 (uint64) delta_residual_base);
		}
	}
}

/* the DICT encoder needs to encode the keys and the indices of the
 * dictionary. the analyzer prepares both the keys and the indices
 * so this stage is simply packing them.
 */
static void
AIC_TFN(aic_encode_dict_block_)(AICCompressorImpl *impl)
{
	AICAnalyzeResult *ar = &impl->analyze_result;
	uint32 n = impl->n_buffered;
	uint16 K = ar->dict.K;
	uint8 w_idx = ar->dict.W_idx;
	bool zigzag_idxs = ar->dict.zigzag_idxs;
	fl_elem_width_t idx_T = zigzag_idxs ? FL_ELEM_W16 : FL_ELEM_W8;
	fl_elem_width_t max_width = (uint8) impl->elem_width < (uint8) idx_T ? idx_T : impl->elem_width;
	AIC_TY key_vals[AIC_FIXED_BLOCK_MAX_COUNT];
	const AIC_TY *keys_src;
	uint32 staged_keys = fl_input_count(K, AIC_FL_T);
	uint32 i;
	if (AIC_FL_T == FL_ELEM_W64)
	{
		/* the keys need no narrowing: pack directly from the analyze
		 * result. fl_pack reads fl_input_count() elements, so zero
		 * the padding above K */
		if (staged_keys > K)
		{
			memset(&ar->dict.keys[K], 0, (size_t) (staged_keys - K) * sizeof(uint64));
		}
		keys_src = (const AIC_TY *) ar->dict.keys;
	}
	else
	{
		for (i = 0; i < K; ++i)
		{
			/* narrow the keys from the analysis to AIC_TY,
			 * because the AICAnalyzeResult is not parametrized
			 * by AIC_TY, and 'ar->dict.keys' is uint64[]  */
			key_vals[i] = (AIC_TY) ar->dict.keys[i];
		}
		/* fl_pack reads fl_input_count() elements, so zero
		 * the padding above K */
		if (staged_keys > K)
		{
			memset(key_vals + K, 0, (size_t) (staged_keys - K) * sizeof(AIC_TY));
		}
		keys_src = key_vals;
	}

	/* single allocation: padded AICBlock + both packed regions */
	size_t keys_bytes = fl_required_bytes(K, ar->W, impl->elem_width);
	size_t idx_bytes = fl_required_bytes(n, w_idx, idx_T);
	size_t alignment = fl_alignment(n, max_width);
	size_t padded_block = ((sizeof(AICBlock) + alignment - 1) / alignment) * alignment;
	size_t padded_keys = ((keys_bytes + alignment - 1) / alignment) * alignment;
	uint8 *mem = palloc_aligned(padded_block + padded_keys + idx_bytes, alignment, 0);
	AICBlock *block = (AICBlock *) mem;
	memset(block, 0, sizeof(AICBlock));
	block->type = AIC_BT_DICT;
	aic_block_dict_init(&block->data.dict,
						n,
						K,
						ar->vmin,
						ar->W,
						w_idx,
						zigzag_idxs,
						impl->elem_width,
						mem + padded_block,
						mem + padded_block + padded_keys);
	aic_add_block(impl, block);
	/* the dict body always packs without a FOR base because the
	 * analyzer already subtracted the 'ar->vmin' when it produced
	 * the 'ar->dict.keys' array */
	block->data.dict.packed_keys_size =
		fl_pack(keys_src, block->data.dict.keys_body, K, ar->W, impl->elem_width);
	/* the idx elem width depends on the zigzag choice, in both
	 * cases we pack without a base value, because zero is
	 * the min idx */
	block->data.dict.packed_idx_size =
		zigzag_idxs ? fl_pack(ar->dict.idx_zz, block->data.dict.idx_body, n, w_idx, FL_ELEM_W16) :
					  fl_pack(ar->dict.idx, block->data.dict.idx_body, n, w_idx, FL_ELEM_W8);
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
static void
AIC_TFN(aic_encode_pfor_block_)(AICCompressorImpl *impl)
{
	AICAnalyzeResult *ar = &impl->analyze_result;
	uint32 n = impl->n_buffered;
	uint8 b = ar->pfor.b;
	uint8 W_exc = ar->pfor.W_exc;
	uint8 n_exc = ar->pfor.n_exc;
	/* analyzer provides the residuals, but we will need to calculate
	 * the body, exception and position values */
	AIC_TY body_vals[AIC_FIXED_BLOCK_MAX_COUNT];
	/* sized for the FL encoding */
	AIC_TY exc_vals[AIC_PFOR_EXC_STAGING_COUNT(AIC_FL_T)] = { 0 };
	uint8 positions[AIC_PFOR_MAX_EXCEPTIONS];
	uint64 mask = (b == 0) ? 0 : (~(uint64) 0 >> (64 - b));
	uint32 e = 0;
	uint32 i;
	for (i = 0; i < n; ++i)
	{
		uint64 res = ar->residuals[i];
		uint64 high = res >> b;
		body_vals[i] = (AIC_TY) (res & mask);
		if (high != 0)
		{
			if (e < AIC_PFOR_MAX_EXCEPTIONS)
			{
				positions[e] = (uint8) i;
				exc_vals[e] = (AIC_TY) high;
			}
			e++;
		}
	}
	CheckCompressedData(e == (uint32) n_exc);

	uint32 staged_body = fl_input_count(n, AIC_FL_T);
	/* fl_pack reads fl_input_count() elements, so zero the tier
	 * padding above n */
	if (staged_body > n)
	{
		memset(body_vals + n, 0, (size_t) (staged_body - n) * sizeof(AIC_TY));
	}
	/* single allocation: padded AICBlock + body + exception stream;
	 * the positions live in the block struct */
	size_t body_bytes = fl_required_bytes(n, b, impl->elem_width);
	size_t exc_bytes = fl_required_bytes(n_exc, W_exc, impl->elem_width);
	size_t alignment = fl_alignment(n, impl->elem_width);
	size_t padded_block = ((sizeof(AICBlock) + alignment - 1) / alignment) * alignment;
	size_t padded_body = ((body_bytes + alignment - 1) / alignment) * alignment;
	uint8 *mem = palloc_aligned(padded_block + padded_body + exc_bytes, alignment, 0);
	AICBlock *block = (AICBlock *) mem;
	memset(block, 0, sizeof(AICBlock));
	block->type = ar->type;
	aic_block_pfor_init(&block->data.pfor,
						n,
						ar->vmin,
						b,
						W_exc,
						n_exc,
						positions,
						impl->elem_width,
						mem + padded_block,
						mem + padded_block + padded_body);
	aic_add_block(impl, block);
	if (b > 0)
	{
		/* only pack the body if b > 0 */
		block->data.pfor.packed_body_size =
			fl_pack(body_vals, block->data.pfor.body, n, b, impl->elem_width);
	}
	block->data.pfor.packed_exc_size =
		fl_pack(exc_vals, block->data.pfor.exc_body, n_exc, W_exc, impl->elem_width);
}

static void
AIC_TFN(aic_encode_block_)(AICCompressorImpl *impl)
{
	if (impl->n_buffered == 0)
	{
		/* when finish was called with a multiple of 256 elements */
		return;
	}
	/* analysis of the buffered data */
	AIC_STY *buf = (AIC_STY *) impl->input_buffer;
	AICAnalyzeResult *ar = &impl->analyze_result;
	/* launch the analysis */
	AIC_TFN(aic_analyze_)(buf, impl->n_buffered, impl->elem_width, ar);
	switch (ar->type)
	{
		case AIC_BT_RLE:
			/* the RLE decision can only happen in normal mode if the
			 * buffered element count is less than the checkpoint. it
			 * can only happen if we finish buffering early */
			Assert(impl->n_buffered <= AIC_RLE_CHECKPOINT);
			impl->rle_base = (uint64) ar->vmin;
			impl->n_rle_elements = impl->n_buffered;
			impl->delta_rle_step = 0;
			AIC_TFN(aic_encode_rle_block_)(impl);
			return;
		case AIC_BT_FOR:
			aic_encode_for_block(impl);
			return;
		case AIC_BT_DFOR:
			AIC_TFN(aic_encode_dfor_block_)(impl);
			break;
		case AIC_BT_DICT:
			AIC_TFN(aic_encode_dict_block_)(impl);
			break;
		case AIC_BT_PFOR:
			AIC_TFN(aic_encode_pfor_block_)(impl);
			break;
		default:
			Assert(false);
			break;
	};
}

/* this is the NORMAL mode append handler, where we do the RLE checks
 * at the checkpoints so we may transit to the other RLE modes
 */
static AICOpMode
AIC_TFN(aic_append_val_)(AICCompressorImpl *impl, AIC_TY v)
{
	AIC_TY *buf = (AIC_TY *) impl->input_buffer;
	buf[impl->n_buffered] = v;
	++(impl->n_buffered);

	/* at the DELTA RLE checkpoint, we check if this is a constant non-zero
	 * slope of the values. if so, move to OP_MODE_DELTA_RLE.  */
	if (impl->n_buffered == AIC_DELTA_RLE_CHECKPOINT)
	{
		AIC_TY d = (AIC_TY) (buf[1] - buf[0]);
		if (d != 0)
		{
			bool is_delta_rle = true;
			for (int i = 2; is_delta_rle && i < AIC_DELTA_RLE_CHECKPOINT; ++i)
			{
				is_delta_rle = (AIC_TY) (buf[i] - buf[i - 1]) == d;
			}
			if (is_delta_rle)
			{
				impl->rle_base = (uint64) buf[0];
				impl->n_rle_elements = AIC_DELTA_RLE_CHECKPOINT;
				impl->delta_rle_step = (uint64) d;
				impl->op_mode = OP_MODE_DELTA_RLE;
				impl->n_buffered = 0;
			}
		}
	}
	/* at the RLE checkpoint check if this is a constant RLE run */
	else if (impl->n_buffered == AIC_RLE_CHECKPOINT)
	{
		bool is_rle = true;
		for (int i = 1; is_rle && i < AIC_RLE_CHECKPOINT; ++i)
		{
			is_rle = buf[i] == buf[0];
		}
		if (is_rle)
		{
			impl->rle_base = (uint64) buf[0];
			impl->n_rle_elements = AIC_RLE_CHECKPOINT;
			impl->delta_rle_step = 0;
			impl->op_mode = OP_MODE_RLE;
			impl->n_buffered = 0;
		}
	}
	/* when the buffer is full, we need to encode the current block */
	else if (impl->n_buffered == AIC_FIXED_BLOCK_MAX_COUNT)
	{
		AIC_TFN(aic_encode_block_)(impl);
		impl->n_buffered = 0;
	}
	return impl->op_mode;
}

static AICOpMode
AIC_TFN(aic_append_rle_val_)(AICCompressorImpl *impl, AIC_TY v)
{
	uint64 current_value = (uint64) v;
	if (current_value == impl->rle_base)
	{
		++(impl->n_rle_elements);
	}
	else
	{
		/* flush RLE block */
		AIC_TFN(aic_encode_rle_block_)(impl);
		impl->op_mode = OP_MODE_NORMAL;
		/* append the current value */
		AIC_TY *buf = (AIC_TY *) impl->input_buffer;
		buf[0] = v;
		impl->n_buffered = 1;
	}
	return impl->op_mode;
}

static AICOpMode
AIC_TFN(aic_append_delta_rle_val_)(AICCompressorImpl *impl, AIC_TY v)
{
	AIC_TY expected = (AIC_TY) (impl->rle_base + impl->n_rle_elements * impl->delta_rle_step);
	if (v == expected)
	{
		++(impl->n_rle_elements);
	}
	else
	{
		/* flush DELTA RLE block */
		AIC_TFN(aic_encode_rle_block_)(impl);
		impl->op_mode = OP_MODE_NORMAL;
		/* append the current value */
		AIC_TY *buf = (AIC_TY *) impl->input_buffer;
		buf[0] = v;
		impl->n_buffered = 1;
	}
	return impl->op_mode;
}

static AICCompressorImpl *
AIC_TFN(aic_compressor_impl_alloc_)()
{
	size_t input_alignment = fl_alignment(AIC_FIXED_BLOCK_MAX_COUNT, AIC_FL_T);
	AICCompressorImpl *ret = palloc0(sizeof(*ret));
	ret->op_mode = OP_MODE_NORMAL;
	ret->elem_width = AIC_FL_T;
	ret->input_buffer_size = fl_input_bytes(AIC_FIXED_BLOCK_MAX_COUNT, AIC_FL_T);
	ret->input_buffer = palloc_aligned(ret->input_buffer_size, input_alignment, 0);
	return ret;
}

/* Teardown */
#undef AIC_TFN
#undef AIC_PASTE2
#undef AIC_PASTE2_
#undef AIC_TY
#undef AIC_STY
#undef AIC_FL_T
