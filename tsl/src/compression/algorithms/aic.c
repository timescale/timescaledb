/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <string.h>
#include "aic.h"
#include "aic/aic_analyze.h"
#include "aic/aic_blocks.h"
#include "aic/aic_internal.h"
#include "aic/aic_nulls.h"
#include "aic/aic_utils.h"
#include "compression/arrow_c_data_interface.h"
#include "compression/compression.h"
#include "fastlanes/fastlanes.h"
#include "fastlanes/fastlanes_types.h"
#include "guc.h"
#include <stdio.h>
#include <stdlib.h>
#include <utils/date.h>
#include <utils/palloc.h>
#include <utils/timestamp.h>

/*
 * Adaptive Integer Compressor (AIC). Please read the aic/README.md file for more details.
 *
 * The AIC compressor has 3 operation modes (OpMode). The compressor starts in OP_MODE_NORMAL and
 * based on the checks at AIC_DELTA_RLE_CHECKPOINT and AIC_RLE_CHECKPOINT, it may move to the
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
 * The AIC compressor must use the same integer type for decompressing the data as it was used
 * for compressing it. The FastLanes library that is used for encoding the data is _not_ type
 * agnostic, and using different types between encoding/decoding will result in incorrect
 * data being returned.
 *
 * The per-type opmode handlers (append/encode functions for uint16/32/64) are emitted from
 * the aic_impl.h template.
 */

#define AIC_NULL_BITMAP_BYTES_MAX ((GLOBAL_MAX_ROWS_PER_COMPRESSION + 7) / 8)

typedef enum AICOpMode
{
	OP_MODE_NORMAL = 0,
	OP_MODE_RLE = 1,
	OP_MODE_DELTA_RLE = 2,
	_OP_MODE_COUNT = 3
} AICOpMode;

typedef struct AICCompressorImpl
{
	AICOpMode op_mode;
	fl_elem_width_t elem_width;
	uint16 n_valid_elements;

	/* input buffering */
	size_t input_buffer_size;
	void *input_buffer;
	uint16 n_buffered;

	/* analysis */
	AICAnalyzeResult analyze_result;

	/* DELTA_RLE and RLE states */
	uint16 n_rle_elements;
	uint64 rle_base;
	uint64 delta_rle_step;

	/* storage for nulls, lazily allocated */
	uint16 n_nulls;
	uint64 *null_bitmap;

	/* block storage */
	uint16 n_blocks;
	AICBlock *first_block;
	AICBlock *last_block;
} AICCompressorImpl;

typedef struct AICCompressor
{
	Compressor base;
	AICCompressorImpl *impl;
	Oid oid;
} AICCompressor;

/* common helpers: */
static inline void aic_mark_null(AICCompressorImpl *impl);

static void
aic_compressor_free_impl(AICCompressorImpl **implptr)
{
	Assert(implptr != NULL);
	AICCompressorImpl *impl = *implptr;
	Assert(impl != NULL);
	if (impl->input_buffer != NULL)
	{
		pfree(impl->input_buffer);
	}
	if (impl->null_bitmap != NULL)
	{
		pfree(impl->null_bitmap);
	}
	aic_free_blocks(impl->first_block);
	pfree(impl);
	*implptr = NULL;
}

static inline void
aic_add_block(AICCompressorImpl *impl, AICBlock *block)
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
aic_compressor_assemble(AICCompressorImpl *impl)
{
	Assert(impl != NULL);
	size_t total_elements = impl->n_valid_elements + impl->n_nulls;

	/* the null size cap is the upper bound, the encoder reports the size it actually used. */
	size_t nulls_size_cap = impl->n_nulls > 0 ? aic_null_size_cap(total_elements) : 0;
	size_t block_size = aic_total_block_size(impl->first_block);
	uint8 *compressed = (uint8 *) palloc(sizeof(AICCompressed) + block_size + nulls_size_cap);
	AICCompressed *aic_compressed = (AICCompressed *) compressed;

	/* prepare the compressed header */
	aic_compressed->compression_algorithm = COMPRESSION_ALGORITHM_AIC;
	uint8 width_flag = aic_elem_width_to_global_flag(impl->elem_width);
	aic_compressed->flags = AIC_FLAG_NULL_RAW | width_flag;
	aic_compressed->valid_count = impl->n_valid_elements;
	aic_compressed->null_count = impl->n_nulls;
	aic_compressed->num_blocks = impl->n_blocks;

	/* serialize compressed blocks: copying the headers and the data
	 * into the final blob. this step is needed because the encoding
	 * has strict alignment requirements, but the result blob has not.
	 * the result blob has the data sequentially, without considering
	 * alignment.
	 */
	uint8 *cursor = aic_compressed->values;
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
		AICGlobalFlags flags = aic_null_bitmap_encode(impl->null_bitmap,
													  total_elements,
													  impl->n_nulls,
													  cursor,
													  &nulls_size);

		Assert(nulls_size > 0);
		Assert(nulls_size < nulls_size_cap);
		aic_compressed->flags |= flags;
		cursor += nulls_size;
	}

	SET_VARSIZE(&aic_compressed->vl_len_, sizeof(AICCompressed) + nulls_size + block_size);

	if (impl->n_valid_elements > 0)
	{
		bool serialized = aic_serialize_blocks(impl->first_block, cursor, block_size);
		CheckCompressedData(serialized == true);
	}
	return compressed;
}

/* OP_MODE_NORMAL handlers: */

/* the FOR encoder is mainly FastLanes packing plus header data */
static void
aic_encode_for_block(AICCompressorImpl *impl)
{
	AICAnalyzeResult *ar = &impl->analyze_result;

	/* allocate FOR Block in a single block to reduce allocations */
	size_t body_bytes = fl_required_bytes(impl->n_buffered, ar->W, impl->elem_width);
	size_t alignment = fl_alignment(impl->n_buffered, impl->elem_width);
	size_t padded_block_size = ((sizeof(AICBlock) + alignment - 1) / alignment) * alignment;
	uint8 *block_bytes = palloc_aligned((body_bytes + padded_block_size), alignment, 0);

	AICBlock *block = (AICBlock *) block_bytes;
	memset(block, 0, sizeof(AICBlock));
	block->type = AIC_BT_FOR;
	block_bytes += padded_block_size;

	aic_block_for_init(&block->data.for_,
					   impl->n_buffered,
					   ar->vmin,
					   ar->W,
					   impl->elem_width,
					   block_bytes);

	aic_add_block(impl, block);

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

/* per-type opmode handlers, emitted from the aic_impl.h template */

/* clang format would reorder the headers which is not desired here */
/* clang-format off */

#define AIC_TY uint16
#define AIC_STY int16
#define AIC_FL_T FL_ELEM_W16
#include "aic_impl.h"

#define AIC_TY uint32
#define AIC_STY int32
#define AIC_FL_T FL_ELEM_W32
#include "aic_impl.h"

#define AIC_TY uint64
#define AIC_STY int64
#define AIC_FL_T FL_ELEM_W64
#include "aic_impl.h"

/* clang-format on */

/* clang-tidy doesn't like the type argument and insists on using
 * parens, but that doesn't compile */
/* NOLINTBEGIN(bugprone-macro-parentheses) */

#define AIC_COMPRESSOR_APPEND_FN(SUFFIX, TY, DATUM_GET)                                            \
	static void aic_append_##SUFFIX(Compressor *compressor, Datum val)                             \
	{                                                                                              \
		TY v = DATUM_GET(val);                                                                     \
		AICCompressor *ci = (AICCompressor *) compressor;                                          \
		if (ci->impl == NULL)                                                                      \
		{                                                                                          \
			ci->impl = aic_compressor_impl_alloc_##TY();                                           \
			Assert(aic_oid_to_elem_width_t(ci->oid) == ci->impl->elem_width);                      \
		}                                                                                          \
		++(ci->impl->n_valid_elements);                                                            \
		/* call the right handler for the current opmode */                                        \
		switch (ci->impl->op_mode)                                                                 \
		{                                                                                          \
			case OP_MODE_NORMAL:                                                                   \
				aic_append_val_##TY(ci->impl, v);                                                  \
				break;                                                                             \
			case OP_MODE_RLE:                                                                      \
				aic_append_rle_val_##TY(ci->impl, v);                                              \
				break;                                                                             \
			default:                                                                               \
				aic_append_delta_rle_val_##TY(ci->impl, v);                                        \
				break;                                                                             \
		}                                                                                          \
	}

#define AIC_COMPRESSOR_APPEND_NULL_FN(SUFFIX, TY)                                                  \
	static void aic_append_null_##SUFFIX(Compressor *compressor)                                   \
	{                                                                                              \
		AICCompressor *ci = (AICCompressor *) compressor;                                          \
		if (ci->impl == NULL)                                                                      \
		{                                                                                          \
			ci->impl = aic_compressor_impl_alloc_##TY();                                           \
			fl_elem_width_t expected_width PG_USED_FOR_ASSERTS_ONLY =                              \
				aic_oid_to_elem_width_t(ci->oid);                                                  \
			Assert(expected_width == ci->impl->elem_width);                                        \
		}                                                                                          \
		/* common null handler */                                                                  \
		aic_mark_null(ci->impl);                                                                   \
	}

#define AIC_COMPRESSOR_FINISH_RESET_FN(SUFFIX, TY)                                                 \
	static void *aic_finish_and_reset_##SUFFIX(Compressor *compressor)                             \
	{                                                                                              \
		AICCompressor *ci = (AICCompressor *) compressor;                                          \
		if (ci == NULL || ci->impl == NULL)                                                        \
		{                                                                                          \
			return NULL;                                                                           \
		}                                                                                          \
		if (ci->impl->n_valid_elements == 0)                                                       \
		{                                                                                          \
			aic_compressor_free_impl(&ci->impl);                                                   \
			return NULL;                                                                           \
		}                                                                                          \
		/* make sure any buffered data gets flushed */                                             \
		if (ci->impl->op_mode == OP_MODE_NORMAL)                                                   \
		{                                                                                          \
			aic_encode_block_##TY(ci->impl);                                                       \
		}                                                                                          \
		else                                                                                       \
		{                                                                                          \
			aic_encode_rle_block_##TY(ci->impl);                                                   \
		}                                                                                          \
		/* assemble the final results from the encoded blocks  */                                  \
		void *result = aic_compressor_assemble(ci->impl);                                          \
		aic_compressor_free_impl(&ci->impl);                                                       \
		return result;                                                                             \
	}

#define AIC_COMPRESSOR_STRUCT_DEF(SUFFIX)                                                          \
	const Compressor aic_compressor_##SUFFIX = {                                                   \
		.append_val = aic_append_##SUFFIX,                                                         \
		.append_null = aic_append_null_##SUFFIX,                                                   \
		.is_full = NULL,                                                                           \
		.finish = aic_finish_and_reset_##SUFFIX,                                                   \
	};

#define AIC_COMPRESSOR_STRUCT(SUFFIX, TY, DATUM_GET)                                               \
	AIC_COMPRESSOR_APPEND_FN(SUFFIX, TY, DATUM_GET)                                                \
	AIC_COMPRESSOR_APPEND_NULL_FN(SUFFIX, TY)                                                      \
	AIC_COMPRESSOR_FINISH_RESET_FN(SUFFIX, TY)                                                     \
	AIC_COMPRESSOR_STRUCT_DEF(SUFFIX)

AIC_COMPRESSOR_STRUCT(uint16, uint16, DatumGetInt16)
AIC_COMPRESSOR_STRUCT(uint32, uint32, DatumGetInt32)
AIC_COMPRESSOR_STRUCT(uint64, uint64, DatumGetInt64)
AIC_COMPRESSOR_STRUCT(date, uint32, DatumGetDateADT)
AIC_COMPRESSOR_STRUCT(timestamp, uint64, DatumGetTimestamp)
AIC_COMPRESSOR_STRUCT(timestamptz, uint64, DatumGetTimestampTz)
#undef AIC_COMPRESSOR_STRUCT
#undef AIC_COMPRESSOR_APPEND_FN
#undef AIC_COMPRESSOR_FINISH_RESET_FN
#undef AIC_COMPRESSOR_STRUCT_DEF

/* NOLINTEND(bugprone-macro-parentheses) */

extern Compressor *
aic_compressor_for_type(Oid element_type)
{
	AICCompressor *compressor = palloc(sizeof(*compressor));
	switch (element_type)
	{
		case INT2OID:
			*compressor = (AICCompressor){ .base = aic_compressor_uint16 };
			break;
		case INT4OID:
			*compressor = (AICCompressor){ .base = aic_compressor_uint32 };
			break;
		case INT8OID:
			*compressor = (AICCompressor){ .base = aic_compressor_uint64 };
			break;
		case DATEOID:
			*compressor = (AICCompressor){ .base = aic_compressor_date };
			break;
		case TIMESTAMPOID:
			*compressor = (AICCompressor){ .base = aic_compressor_timestamp };
			break;
		case TIMESTAMPTZOID:
			*compressor = (AICCompressor){ .base = aic_compressor_timestamptz };
			break;
		default:
			elog(ERROR, "invalid type for aic compressor \"%s\"", format_type_be(element_type));
	};

	compressor->oid = element_type;
	return &compressor->base;
}

static inline void
aic_mark_null(AICCompressorImpl *impl)
{
	/* The bitmap is allocated at full row-cap capacity and filled
	 * all-valid, so valid entries never need a write -- only NULLs
	 * land here. */
	uint32 pos = (uint32) impl->n_valid_elements + impl->n_nulls;
	Ensure(pos < GLOBAL_MAX_ROWS_PER_COMPRESSION, "batch exceeds the compression row cap");

	if (impl->null_bitmap == NULL)
	{
		impl->null_bitmap = palloc(AIC_NULL_BITMAP_BYTES_MAX);
		memset(impl->null_bitmap, 0xFF, AIC_NULL_BITMAP_BYTES_MAX);
	}

	arrow_set_row_validity(impl->null_bitmap, pos, false);
	++impl->n_nulls;
}

bool
aic_compressed_has_nulls(const CompressedDataHeader *header)
{
	const AICCompressed *c = (const AICCompressed *) header;
	return c->null_count > 0;
}

Datum
tsl_aic_compressor_append(PG_FUNCTION_ARGS)
{
	MemoryContext old_context;
	MemoryContext agg_context;
	AICCompressor *compressor = (AICCompressor *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));

	if (!AggCheckCallContext(fcinfo, &agg_context))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "tsl_aic_compressor_append called in non-aggregate context");
	}

	old_context = MemoryContextSwitchTo(agg_context);

	if (compressor == NULL)
	{
		compressor =
			(AICCompressor *) aic_compressor_for_type(get_fn_expr_argtype(fcinfo->flinfo, 1));
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
tsl_aic_compressor_finish(PG_FUNCTION_ARGS)
{
	AICCompressor *compressor = PG_ARGISNULL(0) ? NULL : (AICCompressor *) PG_GETARG_POINTER(0);
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
