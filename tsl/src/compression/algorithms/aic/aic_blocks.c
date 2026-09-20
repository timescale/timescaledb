/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <string.h>
#include "aic_blocks.h"
#include "aic_internal.h"
#include "aic_utils.h"
#include "compression/algorithms/fastlanes/fastlanes.h"
#include "compression/algorithms/fastlanes/fastlanes_types.h"
#include "compression/compression.h"
#include <guc.h>
#include <stdlib.h>

/* please read the README.md file for more details. */

/* Internal block helpers */

/* RLE: */
static inline size_t aic_block_rle_size(const AICBlockRLE *block);
static inline uint8 *aic_block_rle_serialize(const AICBlockRLE *block, uint8 *dest,
											 size_t *max_size);
static inline size_t aic_block_rle_prepare(AICBlockRLE *block, uint32 header, StringInfo si);
static inline size_t aic_block_rle_decompress(const AICBlockRLE *block, uint8 *dest,
											  fl_elem_width_t T);

/* FOR: */
static inline size_t aic_block_for_size(const AICBlockFOR *block);
static inline uint8 *aic_block_for_serialize(const AICBlockFOR *block, uint8 *dest,
											 size_t *max_size);
static inline size_t aic_block_for_prepare(AICBlockFOR *block, uint32 header, StringInfo si,
										   fl_elem_width_t T);
static inline size_t aic_block_for_decompress(const AICBlockFOR *block, uint8 *dest,
											  uint8 *scratch_buffer, fl_elem_width_t T);

/* DFOR: */
static inline size_t aic_block_dfor_size(const AICBlockDFOR *block);
static inline uint8 *aic_block_dfor_serialize(const AICBlockDFOR *block, uint8 *dest,
											  size_t *max_size);
static inline size_t aic_block_dfor_prepare(AICBlockDFOR *block, uint32 header, StringInfo si,
											fl_elem_width_t T);
static inline size_t aic_block_dfor_decompress(const AICBlockDFOR *block, uint8 *dest,
											   uint8 *scratch_buffer, fl_elem_width_t T);

/* DICT */
static inline size_t aic_block_dict_size(const AICBlockDICT *block);
static inline uint8 *aic_block_dict_serialize(const AICBlockDICT *block, uint8 *dest,
											  size_t *max_size);
static inline size_t aic_block_dict_prepare(AICBlockDICT *block, uint32 header, StringInfo si,
											fl_elem_width_t T);
static inline size_t aic_block_dict_decompress(const AICBlockDICT *block, uint8 *dest,
											   uint8 *scratch_buffer, fl_elem_width_t T);

/* PFOR */
static inline size_t aic_block_pfor_size(const AICBlockPFOR *block);
static inline uint8 *aic_block_pfor_serialize(const AICBlockPFOR *block, uint8 *dest,
											  size_t *max_size);
static inline size_t aic_block_pfor_prepare(AICBlockPFOR *block, uint32 header, StringInfo si,
											fl_elem_width_t T);
static inline size_t aic_block_pfor_decompress(const AICBlockPFOR *block, uint8 *dest,
											   uint8 *scratch_buffer, fl_elem_width_t T);

/* free */
void
aic_free_blocks(AICBlock *block)
{
	while (block != NULL)
	{
		AICBlock *next = block->next;
		pfree(block);
		block = next;
	};
}

/* init */
void
aic_block_rle_init(AICBlockRLE *block, uint32 count, int64 base, int64 step, fl_elem_width_t T)
{
	/* the base and step values need to be sign-extended first */
	base = aic_sign_extend_u64((uint64) base, (uint8) T);
	step = aic_sign_extend_u64((uint64) step, (uint8) T);

	block->header = 0;
	block->header |= (AIC_BT_RLE & 0x7);
	block->header |= ((count - 1) & 0xFFFF) << 11;
	block->base_value = base;
	block->step_value = step;

	if (base != 0)
	{
		uint8 bytes_needed = aic_signed_bytes_needed(base);
		block->header |= (bytes_needed & 0xF) << 3;
	}

	if (step != 0)
	{
		uint8 bytes_needed = aic_signed_bytes_needed(step);
		block->header |= (bytes_needed & 0xF) << 7;
	}
}

void
aic_block_for_init(AICBlockFOR *block, uint32 count, int64 base, uint8 W, fl_elem_width_t T,
				   uint8 *body)
{
	/* sign extend base first */
	base = aic_sign_extend_u64((uint64) base, (uint8) T);

	block->header = 0;
	block->header |= (AIC_BT_FOR & 0x7);
	block->header |= ((count - 1) & 0xFF) << 14;
	block->header |= (W & 0x7F) << 7;
	block->base_value = base;

	if (base != 0)
	{
		uint8 bytes_needed = aic_signed_bytes_needed(base);
		block->header |= (bytes_needed & 0xF) << 3;
	}

	if (W > 0)
	{
		block->body = body;
		block->packed_body_size = fl_result_bytes(count, W, T);
	}
	else
	{
		block->body = NULL;
		block->packed_body_size = 0;
	}
}

void
aic_block_dfor_init(AICBlockDFOR *block, uint32 count, uint16 S, int64 base, uint8 W_bases,
					int64 delta_residual_base, uint8 W_delta_residuals, bool zigzag,
					fl_elem_width_t T, uint8 *bases_body, uint8 *delta_residual_body)
{
	Assert(count > S);
	Assert(S >= 1 && S <= 64);

	uint32 n_delta_residuals = count - S;

	base = aic_sign_extend_u64((uint64) base, (uint8) T);
	delta_residual_base = aic_sign_extend_u64((uint64) delta_residual_base, (uint8) T);

	block->header = 0;
	block->header |= (AIC_BT_DFOR & 0x7);
	block->header |= (W_bases & 0x7F) << 7;
	block->header |= ((S - 1) & 0x3F) << 14;
	block->header |= (W_delta_residuals & 0x7F) << 24;
	if (zigzag)
	{
		block->header |= ((uint32) 1) << 31;
	}
	block->count_delta_residuals = (uint8) (n_delta_residuals - 1);
	block->base_value = base;
	block->delta_residual_base_value = delta_residual_base;

	if (base != 0)
	{
		uint8 bytes_needed = aic_signed_bytes_needed(base);
		block->header |= (bytes_needed & 0xF) << 3;
	}

	if (delta_residual_base != 0)
	{
		uint8 bytes_needed = aic_signed_bytes_needed(delta_residual_base);
		block->header |= (bytes_needed & 0xF) << 20;
	}

	if (W_bases > 0)
	{
		block->bases_body = bases_body;
		block->packed_bases_size = fl_result_bytes(S, W_bases, T);
	}
	else
	{
		block->bases_body = NULL;
		block->packed_bases_size = 0;
	}

	if (W_delta_residuals > 0)
	{
		block->delta_residual_body = delta_residual_body;
		block->packed_delta_residual_size =
			fl_result_bytes(n_delta_residuals, W_delta_residuals, T);
	}
	else
	{
		block->delta_residual_body = NULL;
		block->packed_delta_residual_size = 0;
	}
}

void
aic_block_dict_init(AICBlockDICT *block, uint32 count, uint16 K, int64 key_base, uint8 W,
					uint8 W_idx, bool zigzag_idxs, fl_elem_width_t T, uint8 *keys_body,
					uint8 *idx_body)
{
	fl_elem_width_t idx_T = zigzag_idxs ? FL_ELEM_W16 : FL_ELEM_W8;

	Assert(count >= 1);
	Assert(K >= 2 && K <= 256);
	Assert(W >= 2);
	Assert(W_idx >= 1);

	key_base = aic_sign_extend_u64((uint64) key_base, (uint8) T);

	block->header = 0;
	block->header |= (AIC_BT_DICT & 0x7);
	block->header |= (W & 0x7F) << 7;
	block->header |= ((uint32) (K - 1) & 0xFF) << 14;
	block->header |= ((uint32) W_idx & 0x7F) << 22;

	if (zigzag_idxs)
	{
		block->header |= ((uint32) 1) << 29;
	}
	block->count_idx = (uint8) (count - 1);
	block->key_base_value = key_base;

	if (key_base != 0)
	{
		uint8 bytes_needed = aic_signed_bytes_needed(key_base);
		block->header |= (bytes_needed & 0xF) << 3;
	}

	/* fastlanes init: the keys pack at the block type width T, the
	 * indices at the variant's index lane */
	block->keys_body = keys_body;
	block->packed_keys_size = fl_result_bytes(K, W, T);
	block->idx_body = idx_body;
	block->packed_idx_size = fl_result_bytes(count, W_idx, idx_T);
}

void
aic_block_pfor_init(AICBlockPFOR *block, uint32 count, int64 base, uint8 W, uint8 W_exc,
					uint8 n_exc, const uint8 *positions, fl_elem_width_t T, uint8 *body,
					uint8 *exc_body)
{
	Assert(count >= 1);
	Assert(n_exc >= 1 && n_exc <= AIC_PFOR_MAX_EXCEPTIONS);
	Assert(W_exc >= 1);

	base = aic_sign_extend_u64((uint64) base, (uint8) T);

	block->header = 0;

	uint8 bytes_needed = aic_block_header_scalar_len(base);

	block->header |= (AIC_BT_PFOR & 0x7);
	block->header |= (bytes_needed & 0xF) << 3;
	block->header |= ((uint32) W & 0x7F) << 7;
	block->header |= ((uint32) (count - 1) & 0xFF) << 14;
	block->header |= ((uint32) W_exc & 0x7F) << 22;

	block->count_exc = (uint8) (n_exc - 1);
	block->base_value = base;
	memcpy(block->positions, positions, n_exc);
	block->exc_positions = block->positions;

	/* the body can be zero-width (b == 0), the exception stream must have
	 * (n_exc >= 1 and W_exc >= 1) */
	if (W > 0)
	{
		block->body = body;
		block->packed_body_size = fl_result_bytes(count, W, T);
	}
	else
	{
		block->body = NULL;
		block->packed_body_size = 0;
	}
	block->exc_body = exc_body;
	block->packed_exc_size = fl_result_bytes(n_exc, W_exc, T);
}

/* block size helpers */
size_t
aic_block_rle_size(const AICBlockRLE *block)
{
	uint32 header = block->header;
	uint8 base_len_bytes = (header >> 3) & 0xF;
	uint8 step_len_bytes = (header >> 7) & 0xF;
	return sizeof(uint32) + base_len_bytes + step_len_bytes;
}

size_t
aic_block_for_size(const AICBlockFOR *block)
{
	uint32 header = block->header;
	uint8 base_len_bytes = (header >> 3) & 0xF;
	return sizeof(uint32) + base_len_bytes + block->packed_body_size;
}

size_t
aic_block_dfor_size(const AICBlockDFOR *block)
{
	uint32 header = block->header;
	uint8 base_len_bytes = (header >> 3) & 0xF;
	uint8 delta_residual_base_len_bytes = (header >> 20) & 0xF;
	return sizeof(uint32) + sizeof(uint8) + base_len_bytes + delta_residual_base_len_bytes +
		   block->packed_bases_size + block->packed_delta_residual_size;
}

size_t
aic_block_dict_size(const AICBlockDICT *block)
{
	uint32 header = block->header;
	uint8 key_base_len_bytes = (header >> 3) & 0xF;
	return sizeof(uint32) + sizeof(uint8) + key_base_len_bytes + block->packed_keys_size +
		   block->packed_idx_size;
}

size_t
aic_block_pfor_size(const AICBlockPFOR *block)
{
	uint32 header = block->header;
	/* count_exc is staged by both init and prepare */
	uint32 n_exc = (uint32) block->count_exc + 1;
	size_t scalar_bytes = sizeof(uint8) + ((header >> 3) & 0xF);

	return sizeof(uint32) + scalar_bytes + block->packed_body_size + n_exc + block->packed_exc_size;
}

size_t
aic_total_block_size(AICBlock *block)
{
	size_t total_size = 0;
	while (block != NULL)
	{
		switch (block->type)
		{
			case AIC_BT_RLE:
				total_size += aic_block_rle_size(&block->data.rle);
				break;
			case AIC_BT_FOR:
				total_size += aic_block_for_size(&block->data.for_);
				break;
			case AIC_BT_DFOR:
				total_size += aic_block_dfor_size(&block->data.dfor);
				break;
			case AIC_BT_DICT:
				total_size += aic_block_dict_size(&block->data.dict);
				break;
			case AIC_BT_PFOR:
				total_size += aic_block_pfor_size(&block->data.pfor);
				break;
			default:
				Assert(false);
		}
		block = block->next;
	}
	return total_size;
}

/* serialization: the *_serialize functions take a destination buffer and
 * `max_size` that tells how many bytes are available. the functions return a
 * pointer to the next byte after the serialized block, and decrease `max_size`
 * to reflect the consumed bytes.
 */
uint8 *
aic_block_rle_serialize(const AICBlockRLE *block, uint8 *dest, size_t *max_size)
{
	CheckCompressedData(block != NULL);
	CheckCompressedData(dest != NULL);
	CheckCompressedData(max_size != NULL);

	size_t required_size = aic_block_rle_size(block);
	CheckCompressedData(required_size <= *max_size);
	uint8 *start_dest PG_USED_FOR_ASSERTS_ONLY = dest;

	uint32 header = block->header;
	uint8 base_len_bytes = (header >> 3) & 0xF;
	uint8 step_len_bytes = (header >> 7) & 0xF;

	aic_store_le32(dest, block->header);
	dest += sizeof(uint32);

	if (base_len_bytes > 0)
	{
		aic_store_le64(dest, (uint64) block->base_value, base_len_bytes);
		dest += base_len_bytes;
	}

	if (step_len_bytes > 0)
	{
		aic_store_le64(dest, (uint64) block->step_value, step_len_bytes);
		dest += step_len_bytes;
	}

	*max_size -= required_size;
	Assert((size_t) (dest - start_dest) == required_size);
	return dest;
}

uint8 *
aic_block_for_serialize(const AICBlockFOR *block, uint8 *dest, size_t *max_size)
{
	CheckCompressedData(block != NULL);
	CheckCompressedData(dest != NULL);
	CheckCompressedData(max_size != NULL);

	size_t required_size = aic_block_for_size(block);
	CheckCompressedData(required_size <= *max_size);
	uint8 *start_dest PG_USED_FOR_ASSERTS_ONLY = dest;

	uint32 header = block->header;
	uint8 base_len_bytes = (header >> 3) & 0xF;

	aic_store_le32(dest, block->header);
	dest += sizeof(uint32);

	if (base_len_bytes > 0)
	{
		aic_store_le64(dest, (uint64) block->base_value, base_len_bytes);
		dest += base_len_bytes;
	}

	if (block->packed_body_size > 0)
	{
		memcpy(dest, block->body, block->packed_body_size);
		dest += block->packed_body_size;
	}

	*max_size -= required_size;
	Assert((size_t) (dest - start_dest) == required_size);
	return dest;
}

uint8 *
aic_block_dfor_serialize(const AICBlockDFOR *block, uint8 *dest, size_t *max_size)
{
	CheckCompressedData(block != NULL);
	CheckCompressedData(dest != NULL);
	CheckCompressedData(max_size != NULL);

	size_t required_size = aic_block_dfor_size(block);
	CheckCompressedData(required_size <= *max_size);
	uint8 *start_dest PG_USED_FOR_ASSERTS_ONLY = dest;

	uint32 header = block->header;
	uint8 base_len_bytes = (header >> 3) & 0xF;
	uint8 delta_residual_base_len_bytes = (header >> 20) & 0xF;

	aic_store_le32(dest, block->header);
	dest += sizeof(uint32);

	*dest = block->count_delta_residuals;
	dest++;

	if (base_len_bytes > 0)
	{
		aic_store_le64(dest, (uint64) block->base_value, base_len_bytes);
		dest += base_len_bytes;
	}

	if (delta_residual_base_len_bytes > 0)
	{
		aic_store_le64(dest,
					   (uint64) block->delta_residual_base_value,
					   delta_residual_base_len_bytes);
		dest += delta_residual_base_len_bytes;
	}

	if (block->packed_bases_size > 0)
	{
		memcpy(dest, block->bases_body, block->packed_bases_size);
		dest += block->packed_bases_size;
	}

	if (block->packed_delta_residual_size > 0)
	{
		memcpy(dest, block->delta_residual_body, block->packed_delta_residual_size);
		dest += block->packed_delta_residual_size;
	}

	*max_size -= required_size;
	Assert((size_t) (dest - start_dest) == required_size);
	return dest;
}

uint8 *
aic_block_dict_serialize(const AICBlockDICT *block, uint8 *dest, size_t *max_size)
{
	CheckCompressedData(block != NULL);
	CheckCompressedData(dest != NULL);
	CheckCompressedData(max_size != NULL);

	size_t required_size = aic_block_dict_size(block);
	CheckCompressedData(required_size <= *max_size);
	uint8 *start_dest PG_USED_FOR_ASSERTS_ONLY = dest;

	uint32 header = block->header;
	uint8 key_base_len_bytes = (header >> 3) & 0xF;

	aic_store_le32(dest, block->header);
	dest += sizeof(uint32);

	*dest = block->count_idx;
	dest++;

	if (key_base_len_bytes > 0)
	{
		aic_store_le64(dest, (uint64) block->key_base_value, key_base_len_bytes);
		dest += key_base_len_bytes;
	}

	/* both packed streams always have bytes: W > 1 and W_idx > 0 */
	memcpy(dest, block->keys_body, block->packed_keys_size);
	dest += block->packed_keys_size;

	memcpy(dest, block->idx_body, block->packed_idx_size);
	dest += block->packed_idx_size;

	*max_size -= required_size;
	Assert((size_t) (dest - start_dest) == required_size);
	return dest;
}

uint8 *
aic_block_pfor_serialize(const AICBlockPFOR *block, uint8 *dest, size_t *max_size)
{
	CheckCompressedData(block != NULL);
	CheckCompressedData(dest != NULL);
	CheckCompressedData(max_size != NULL);

	size_t required_size = aic_block_pfor_size(block);
	CheckCompressedData(required_size <= *max_size);
	uint8 *start_dest PG_USED_FOR_ASSERTS_ONLY = dest;

	uint32 header = block->header;
	uint32 n_exc = (uint32) block->count_exc + 1;
	uint8 base_len_bytes = (header >> 3) & 0xF;

	aic_store_le32(dest, block->header);
	dest += sizeof(uint32);

	*dest = block->count_exc;
	dest++;
	aic_store_le64(dest, (uint64) block->base_value, base_len_bytes);
	dest += base_len_bytes;

	if (block->packed_body_size > 0)
	{
		memcpy(dest, block->body, block->packed_body_size);
		dest += block->packed_body_size;
	}

	memcpy(dest, block->positions, n_exc);
	dest += n_exc;

	/* the exception stream always has bytes: n_exc >= 1, W_exc >= 1 */
	memcpy(dest, block->exc_body, block->packed_exc_size);
	dest += block->packed_exc_size;

	*max_size -= required_size;
	Assert((size_t) (dest - start_dest) == required_size);
	return dest;
}

bool
aic_serialize_blocks(AICBlock *block, uint8 *dest, size_t max_size)
{
	Assert(block != NULL);
	Assert(dest != NULL);
	Assert(max_size != 0);

	while (block != NULL)
	{
		AICBlock *next = block->next;
		switch (block->type)
		{
			case AIC_BT_RLE:
				dest = aic_block_rle_serialize(&block->data.rle, dest, &max_size);
				break;
			case AIC_BT_FOR:
				dest = aic_block_for_serialize(&block->data.for_, dest, &max_size);
				break;
			case AIC_BT_DFOR:
				dest = aic_block_dfor_serialize(&block->data.dfor, dest, &max_size);
				break;
			case AIC_BT_DICT:
				dest = aic_block_dict_serialize(&block->data.dict, dest, &max_size);
				break;
			case AIC_BT_PFOR:
				dest = aic_block_pfor_serialize(&block->data.pfor, dest, &max_size);
				break;
			default:
				CheckCompressedData(false);
		}
		CheckCompressedData(dest != NULL);
		block = next;
	};

	return (max_size == 0);
}

/* preparation for decompression: the *_prepare functions set the AICBlock*
 * values and body pointers from the serialized data from the StringInfo.
 * they return the number of elements in the block, which is needed for
 * decompression.
 */
size_t
aic_block_rle_prepare(AICBlockRLE *block, uint32 header, StringInfo si)
{
	Assert(block != NULL);
	Assert(si != NULL);

	block->header = header;

	uint8 base_len_bytes = (header >> 3) & 0xF;
	uint8 step_len_bytes = (header >> 7) & 0xF;
	size_t num_elements = ((header >> 11) & 0xFFFF) + 1;

	CheckCompressedData(base_len_bytes <= 8);
	CheckCompressedData(step_len_bytes <= 8);
	CheckCompressedData(num_elements <= GLOBAL_MAX_ROWS_PER_COMPRESSION);

	block->base_value = 0;
	block->step_value = 0;

	if (base_len_bytes > 0)
	{
		uint8 *base_ptr = (uint8 *) consumeCompressedData(si, base_len_bytes);
		block->base_value = aic_read_signed(base_ptr, base_len_bytes);
	}

	if (step_len_bytes > 0)
	{
		uint8 *step_ptr = (uint8 *) consumeCompressedData(si, step_len_bytes);
		block->step_value = aic_read_signed(step_ptr, step_len_bytes);
	}

	return num_elements;
}

size_t
aic_block_for_prepare(AICBlockFOR *block, uint32 header, StringInfo si, fl_elem_width_t T)
{
	Assert(block != NULL);
	Assert(si != NULL);

	block->header = header;

	uint8 base_len_bytes = (header >> 3) & 0xF;
	uint8 W = (header >> 7) & 0x7F;
	size_t num_elements = ((header >> 14) & 0xFF) + 1;

	CheckCompressedData(W <= (uint8) T);
	CheckCompressedData(base_len_bytes <= 8);
	CheckCompressedData(num_elements <= GLOBAL_MAX_ROWS_PER_COMPRESSION);

	block->base_value = 0;

	if (base_len_bytes > 0)
	{
		uint8 *base_ptr = (uint8 *) consumeCompressedData(si, base_len_bytes);
		block->base_value = aic_read_signed(base_ptr, base_len_bytes);
	}

	if (W > 0)
	{
		block->packed_body_size = fl_result_bytes(num_elements, W, T);

		/* we don't own this ptr, will need to zero in the caller */
		uint8 *body_ptr = (uint8 *) consumeCompressedData(si, block->packed_body_size);
		block->body = body_ptr;
	}
	else
	{
		block->body = NULL;
		block->packed_body_size = 0;
	}

	return num_elements;
}

size_t
aic_block_dfor_prepare(AICBlockDFOR *block, uint32 header, StringInfo si, fl_elem_width_t T)
{
	Assert(block != NULL);
	Assert(si != NULL);

	block->header = header;

	uint8 base_len_bytes = (header >> 3) & 0xF;
	uint8 W_bases = (header >> 7) & 0x7F;
	uint16 S = (uint16) (((header >> 14) & 0x3F) + 1);
	uint8 delta_residual_base_len_bytes = (header >> 20) & 0xF;
	uint8 W_delta_residuals = (header >> 24) & 0x7F;

	CheckCompressedData(W_bases <= (uint8) T);
	CheckCompressedData(W_delta_residuals <= (uint8) T);
	CheckCompressedData(base_len_bytes <= 8);
	CheckCompressedData(delta_residual_base_len_bytes <= 8);

	/* the trailing byte sits directly after the header */
	uint8 *count_ptr = (uint8 *) consumeCompressedData(si, sizeof(uint8));
	block->count_delta_residuals = *count_ptr;
	uint32 n_delta_residuals = (uint32) block->count_delta_residuals + 1;
	size_t num_elements = (size_t) S + n_delta_residuals;

	CheckCompressedData(num_elements <= GLOBAL_MAX_ROWS_PER_COMPRESSION);

	block->base_value = 0;
	block->delta_residual_base_value = 0;

	/* scalars sit directly after the count byte */
	if (base_len_bytes > 0)
	{
		uint8 *base_ptr = (uint8 *) consumeCompressedData(si, base_len_bytes);
		block->base_value = aic_read_signed(base_ptr, base_len_bytes);
	}

	if (delta_residual_base_len_bytes > 0)
	{
		uint8 *res_ptr = (uint8 *) consumeCompressedData(si, delta_residual_base_len_bytes);
		block->delta_residual_base_value = aic_read_signed(res_ptr, delta_residual_base_len_bytes);
	}

	block->packed_bases_size = fl_result_bytes(S, W_bases, T);
	block->packed_delta_residual_size = fl_result_bytes(n_delta_residuals, W_delta_residuals, T);

	if (block->packed_bases_size + block->packed_delta_residual_size > 0)
	{
		uint8 *body_ptr = (uint8 *) consumeCompressedData(si,
														  block->packed_bases_size +
															  block->packed_delta_residual_size);
		block->bases_body = (block->packed_bases_size > 0) ? body_ptr : NULL;
		block->delta_residual_body =
			(block->packed_delta_residual_size > 0) ? body_ptr + block->packed_bases_size : NULL;
	}
	else
	{
		block->bases_body = NULL;
		block->delta_residual_body = NULL;
	}

	return num_elements;
}

size_t
aic_block_dict_prepare(AICBlockDICT *block, uint32 header, StringInfo si, fl_elem_width_t T)
{
	Assert(block != NULL);
	Assert(si != NULL);

	block->header = header;

	uint8 key_base_len_bytes = (header >> 3) & 0xF;
	uint8 W = (header >> 7) & 0x7F;
	uint16 K = (uint16) (((header >> 14) & 0xFF) + 1);
	uint8 W_idx = (header >> 22) & 0x7F;
	bool zigzag_idxs = (bool) ((header >> 29) & 0x1);
	fl_elem_width_t idx_T = zigzag_idxs ? FL_ELEM_W16 : FL_ELEM_W8;

	CheckCompressedData(W >= 2);
	CheckCompressedData(W <= (uint8) T);
	CheckCompressedData(W_idx >= 1);
	CheckCompressedData(W_idx <= (uint8) idx_T);
	CheckCompressedData(W_idx <= 8);
	CheckCompressedData(key_base_len_bytes <= 8);
	CheckCompressedData(K >= 2);

	/* the trailing byte sits directly after the header */
	uint8 *count_ptr = (uint8 *) consumeCompressedData(si, sizeof(uint8));
	block->count_idx = *count_ptr;
	size_t num_elements = (size_t) block->count_idx + 1;

	CheckCompressedData(num_elements <= GLOBAL_MAX_ROWS_PER_COMPRESSION);

	block->key_base_value = 0;

	/* the scalar sits directly after the count byte */
	if (key_base_len_bytes > 0)
	{
		uint8 *base_ptr = (uint8 *) consumeCompressedData(si, key_base_len_bytes);
		block->key_base_value = aic_read_signed(base_ptr, key_base_len_bytes);
	}

	block->packed_keys_size = fl_result_bytes(K, W, T);
	block->packed_idx_size = fl_result_bytes((uint32) num_elements, W_idx, idx_T);

	/* both streams always have bytes (W > 1, W_idx > 0), so one
	 * contiguous consume covers them */
	uint8 *body_ptr =
		(uint8 *) consumeCompressedData(si, block->packed_keys_size + block->packed_idx_size);
	block->keys_body = body_ptr;
	block->idx_body = body_ptr + block->packed_keys_size;

	return num_elements;
}

size_t
aic_block_pfor_prepare(AICBlockPFOR *block, uint32 header, StringInfo si, fl_elem_width_t T)
{
	Assert(block != NULL);
	Assert(si != NULL);

	block->header = header;

	AICBlockType type PG_USED_FOR_ASSERTS_ONLY = (AICBlockType) (header & 0x7);
	uint8 W;
	uint8 W_exc;
	size_t num_elements;
	uint32 n_exc;
	uint32 i;

	Assert(type == AIC_BT_PFOR);
	uint8 base_len_bytes = (header >> 3) & 0xF;

	W = (header >> 7) & 0x7F;
	num_elements = (size_t) ((header >> 14) & 0xFF) + 1;
	W_exc = (header >> 22) & 0x7F;

	CheckCompressedData(base_len_bytes <= 8);

	/* the trailing count byte sits directly after the header */
	uint8 *count_ptr = (uint8 *) consumeCompressedData(si, sizeof(uint8));
	block->count_exc = *count_ptr;
	n_exc = (uint32) block->count_exc + 1;

	/* the scalar sits directly after the count byte */
	block->base_value = 0;
	if (base_len_bytes > 0)
	{
		uint8 *base_ptr = (uint8 *) consumeCompressedData(si, base_len_bytes);
		block->base_value = aic_read_signed(base_ptr, base_len_bytes);
	}

	CheckCompressedData(W <= (uint8) T);
	CheckCompressedData(W_exc >= 1);
	CheckCompressedData(W_exc <= (uint8) T);
	CheckCompressedData((uint32) W + (uint32) W_exc <= (uint32) (uint8) T);
	CheckCompressedData(n_exc <= AIC_PFOR_MAX_EXCEPTIONS);
	CheckCompressedData(num_elements <= GLOBAL_MAX_ROWS_PER_COMPRESSION);

	block->packed_body_size = fl_result_bytes((uint32) num_elements, W, T);
	block->packed_exc_size = fl_result_bytes(n_exc, W_exc, T);

	/* one contiguous consume: body, positions, exception stream */
	uint8 *body_ptr =
		(uint8 *) consumeCompressedData(si,
										block->packed_body_size + n_exc + block->packed_exc_size);
	block->body = body_ptr;

	uint32 bad = 0;
	block->exc_positions = (uint8 *) &body_ptr[block->packed_body_size];
	/* positions are uint8 on the wire; at num_elements == 256 every
	 * possible value is in range */
	if (num_elements < 256)
	{
		uint32 ne = (uint32) num_elements;
		for (i = 0; i < n_exc; ++i)
		{
			uint8 p = body_ptr[block->packed_body_size + i];
			bad |= (uint32) (p >= ne);
		}
		CheckCompressedData(bad == 0);
	}

	return num_elements;
}

/* decompression: the per-type reconstruct/broadcast helpers are emitted
 * from the aic_blocks_impl.h template */

/* clang format would reorder the headers which is not desired here */
/* clang-format off */

#define AIC_TY uint16
#define AIC_STY int16
#include "aic_blocks_impl.h"

#define AIC_TY uint32
#define AIC_STY int32
#include "aic_blocks_impl.h"

#define AIC_TY uint64
#define AIC_STY int64
#include "aic_blocks_impl.h"

/* clang-format on */

size_t
aic_block_rle_decompress(const AICBlockRLE *block, uint8 *dest, fl_elem_width_t T)
{
	Assert(block != NULL);
	Assert(T > 0);

	size_t num_elements = ((block->header >> 11) & 0xFFFF) + 1;
	CheckCompressedData(num_elements <= GLOBAL_MAX_ROWS_PER_COMPRESSION);

	if (block->step_value == 0)
	{
		/* simple RLE */
		switch (T)
		{
			case FL_ELEM_W16:
				aic_rle_broadcast_uint16(dest, block->base_value, num_elements);
				break;
			case FL_ELEM_W32:
				aic_rle_broadcast_uint32(dest, block->base_value, num_elements);
				break;
			case FL_ELEM_W64:
				aic_rle_broadcast_uint64(dest, block->base_value, num_elements);
				break;
			default:
				CheckCompressedData(false);
				break;
		};
	}
	else
	{
		/* delta RLE */
		switch (T)
		{
			case FL_ELEM_W16:
				aic_delta_rle_broadcast_uint16(dest,
											   block->base_value,
											   block->step_value,
											   num_elements);
				break;
			case FL_ELEM_W32:
				aic_delta_rle_broadcast_uint32(dest,
											   block->base_value,
											   block->step_value,
											   num_elements);
				break;
			case FL_ELEM_W64:
				aic_delta_rle_broadcast_uint64(dest,
											   block->base_value,
											   block->step_value,
											   num_elements);
				break;
			default:
				CheckCompressedData(false);
				break;
		};
	}

	return num_elements;
}

size_t
aic_block_for_decompress(const AICBlockFOR *block, uint8 *dest, uint8 *scratch_buffer,
						 fl_elem_width_t T)
{
	Assert(block != NULL);
	Assert(dest != NULL);

	uint32 header = block->header;
	uint8 base_len_bytes = (header >> 3) & 0xF;
	uint8 W = (header >> 7) & 0x7F;
	size_t num_elements = ((header >> 14) & 0xFF) + 1;
	const uint8 *body = block->body;

	/* copy the packed data to the scratch buffer */
	if (W > 0 && block->packed_body_size > 0)
	{
		memcpy(scratch_buffer, body, block->packed_body_size);
	}

	if (base_len_bytes > 0)
	{
		fl_unpack_ffor(scratch_buffer, dest, num_elements, W, T, block->base_value);
	}
	else
	{
		fl_unpack(scratch_buffer, dest, num_elements, W, T);
	}
	return num_elements;
}

size_t
aic_block_dfor_decompress(const AICBlockDFOR *block, uint8 *dest, uint8 *scratch_buffer,
						  fl_elem_width_t T)
{
	Assert(block != NULL);
	Assert(dest != NULL);

	uint32 header = block->header;
	uint8 base_len_bytes = (header >> 3) & 0xF;
	uint8 W_bases = (header >> 7) & 0x7F;
	uint16 S = (uint16) (((header >> 14) & 0x3F) + 1);
	uint8 W_delta_residuals = (header >> 24) & 0x7F;
	bool zigzag = (bool) ((header >> 31) & 0x1);
	uint32 n_delta_residuals = (uint32) block->count_delta_residuals + 1;
	size_t num_elements = (size_t) S + n_delta_residuals;
	size_t elem_bytes = (size_t) ((uint8) T / 8);

	/* bases into dest[0..S) */
	if (block->packed_bases_size > 0 && block->bases_body != NULL)
	{
		memcpy(scratch_buffer, block->bases_body, block->packed_bases_size);
	}
	if (base_len_bytes > 0)
	{
		fl_unpack_ffor(scratch_buffer, dest, S, W_bases, T, block->base_value);
	}
	else
	{
		fl_unpack(scratch_buffer, dest, S, W_bases, T);
	}

	/* delta residuals into dest[S..n) */
	if (block->packed_delta_residual_size > 0 && block->delta_residual_body != NULL)
	{
		memcpy(scratch_buffer, block->delta_residual_body, block->packed_delta_residual_size);
	}

	if (block->delta_residual_base_value == 0)
	{
		fl_unpack(scratch_buffer,
				  dest + (size_t) S * elem_bytes,
				  n_delta_residuals,
				  W_delta_residuals,
				  T);
	}
	else
	{
		fl_unpack_ffor(scratch_buffer,
					   dest + (size_t) S * elem_bytes,
					   n_delta_residuals,
					   W_delta_residuals,
					   T,
					   block->delta_residual_base_value);
	}

	/* reconstruct in place */
	switch (T)
	{
		case FL_ELEM_W16:
			aic_dfor_reconstruct_uint16(dest, S, (uint32) num_elements, zigzag);
			break;
		case FL_ELEM_W32:
			aic_dfor_reconstruct_uint32(dest, S, (uint32) num_elements, zigzag);
			break;
		case FL_ELEM_W64:
			aic_dfor_reconstruct_uint64(dest, S, (uint32) num_elements, zigzag);
			break;
		default:
			CheckCompressedData(false);
	}

	return num_elements;
}

size_t
aic_block_dict_decompress(const AICBlockDICT *block, uint8 *dest, uint8 *scratch_buffer,
						  fl_elem_width_t T)
{
	Assert(block != NULL);
	Assert(dest != NULL);

	AICIntBuffer keys;
	uint16 idx[AIC_FIXED_BLOCK_MAX_COUNT];

	uint32 header = block->header;
	uint8 W = (header >> 7) & 0x7F;
	uint16 K = (uint16) (((header >> 14) & 0xFF) + 1);
	uint8 W_idx = (header >> 22) & 0x7F;
	bool zigzag_idxs = (bool) ((header >> 29) & 0x1);
	fl_elem_width_t idx_T = zigzag_idxs ? FL_ELEM_W16 : FL_ELEM_W8;
	uint32 num_elements = (size_t) block->count_idx + 1;
	uint32 idx_mask = ((uint32) 1 << aic_bit_width_u64((uint64) (K - 1))) - 1;
	const uint8 *body = block->keys_body;

	/* copy the 'body' holding the keys into the aligned scratch buffer */
	memcpy(scratch_buffer, body, block->packed_keys_size);

	/* unpack the dict keys into 'keys' union */
	if (block->key_base_value == 0)
	{
		fl_unpack(scratch_buffer, keys.uint64_elem, K, W, T);
	}
	else
	{
		fl_unpack_ffor(scratch_buffer, keys.uint64_elem, K, W, T, block->key_base_value);
	}

	/* the idx_mask bounds the maximum key index to the next power
	 * of two minus one, which keeps the keys[] access in
	 * aic_dict_reconstruct_* branch-less instead of a per-element
	 * bounds check. out-of-range key indexes only occur on corrupt
	 * input: the non-zigzag case cannot read past the block's maximum
	 * capacity anyway, but the zigzag case's delta/prefix sum stream
	 * can easily go over it. so the idx_mask is more important there.
	 *
	 * the below memset clears the keys between the first invalid slot
	 * (K) and the idx_mask clamp, so corrupt blocks decode these slots
	 * to zero instead of leaking stale buffer contents.
	 */
	if (K <= idx_mask)
	{
		size_t elem_bytes = (size_t) ((uint8) T / 8);
		uint32 undefined_slots = idx_mask - K + 1;

		memset((uint8 *) keys.uint64_elem + (size_t) K * elem_bytes,
			   0,
			   (size_t) undefined_slots * elem_bytes);
	}

	/* the indices into the index array, scratch reused */
	const uint8 *idx_comp = body + block->packed_keys_size;
	memcpy(scratch_buffer, idx_comp, block->packed_idx_size);
	fl_unpack(scratch_buffer, idx, num_elements, W_idx, idx_T);

	/* gather with separately generated functions */
	switch (T)
	{
		case FL_ELEM_W16:
			aic_dict_reconstruct_uint16(dest,
										keys.uint16_elem,
										idx,
										num_elements,
										idx_mask,
										zigzag_idxs);
			break;
		case FL_ELEM_W32:
			aic_dict_reconstruct_uint32(dest,
										keys.uint32_elem,
										idx,
										num_elements,
										idx_mask,
										zigzag_idxs);
			break;
		case FL_ELEM_W64:
			aic_dict_reconstruct_uint64(dest,
										keys.uint64_elem,
										idx,
										num_elements,
										idx_mask,
										zigzag_idxs);
			break;
		default:
			CheckCompressedData(false);
	}

	return (size_t) num_elements;
}

size_t
aic_block_pfor_decompress(const AICBlockPFOR *block, uint8 *dest, uint8 *scratch_buffer,
						  fl_elem_width_t T)
{
	Assert(block != NULL);
	Assert(dest != NULL);

	uint32 header = block->header;
	AICBlockType type PG_USED_FOR_ASSERTS_ONLY = (AICBlockType) (header & 0x7);
	uint8 W;
	uint8 W_exc;
	uint32 num_elements;
	uint32 n_exc = (uint32) block->count_exc + 1;
	const uint8 *body = block->body;

	Assert(type == AIC_BT_PFOR);
	W = (header >> 7) & 0x7F;
	num_elements = ((header >> 14) & 0xFF) + 1;
	W_exc = (header >> 22) & 0x7F;

	if (block->packed_body_size > 0)
	{
		memcpy(scratch_buffer, body, block->packed_body_size);
	}
	if (block->base_value == 0)
	{
		fl_unpack(scratch_buffer, dest, num_elements, W, T);
	}
	else
	{
		fl_unpack_ffor(scratch_buffer, dest, num_elements, W, T, block->base_value);
	}

	/* the scratch buffer is used both as the temporary aligned
	 * storage of the packed values and also the higher area
	 * (at AIC_SCRATCH_STAGING_BYTES offset) is used as the destination
	 * of the unpacked exception data. from the decompression
	 * perspective, these two are both temporary data that will
	 * be merged into the main data stream.
	 */
	uint8 *exc_vals = scratch_buffer + AIC_SCRATCH_STAGING_BYTES;
	const uint8 *exc_comp = body + block->packed_body_size + n_exc;
	Assert(fl_input_count(n_exc, T) * (size_t) ((uint8) T / 8) <= AIC_PFOR_EXC_BYTES);
	memcpy(scratch_buffer, exc_comp, block->packed_exc_size);
	fl_unpack(scratch_buffer, exc_vals, n_exc, W_exc, T);

	/* patch up the results at 'dest' */
	switch (T)
	{
		case FL_ELEM_W16:
			aic_pfor_reconstruct_uint16(dest, exc_vals, block->exc_positions, n_exc, W);
			break;
		case FL_ELEM_W32:
			aic_pfor_reconstruct_uint32(dest, exc_vals, block->exc_positions, n_exc, W);
			break;
		case FL_ELEM_W64:
			aic_pfor_reconstruct_uint64(dest, exc_vals, block->exc_positions, n_exc, W);
			break;
		default:
			CheckCompressedData(false);
	}

	return num_elements;
}

size_t
aic_decompress_blocks(uint32 num_blocks, StringInfo si, uint8 *dest, uint8 *scratch_buffer,
					  size_t max_elements, fl_elem_width_t T)
{
	size_t elem_bytes = (uint8) T / 8;
	size_t num_elements = 0;
	uint32 header;
	uint32 i;

	for (i = 0; i < num_blocks; ++i)
	{
		AICBlock block;
		uint32 count = 0;
		uint8 *comp_ptr = (uint8 *) consumeCompressedData(si, sizeof(uint32));

		header = aic_load_le32(comp_ptr);
		block.type = (AICBlockType) (header & 0x7);
		switch (block.type)
		{
			case AIC_BT_RLE:
				count = aic_block_rle_prepare(&block.data.rle, header, si);
				CheckCompressedData(num_elements + count <= max_elements);
				aic_block_rle_decompress(&block.data.rle, dest, T);
				break;
			case AIC_BT_FOR:
				count = aic_block_for_prepare(&block.data.for_, header, si, T);
				CheckCompressedData(num_elements + count <= max_elements);
				aic_block_for_decompress(&block.data.for_, dest, scratch_buffer, T);
				break;
			case AIC_BT_DFOR:
				count = aic_block_dfor_prepare(&block.data.dfor, header, si, T);
				CheckCompressedData(num_elements + count <= max_elements);
				aic_block_dfor_decompress(&block.data.dfor, dest, scratch_buffer, T);
				break;
			case AIC_BT_DICT:
				count = aic_block_dict_prepare(&block.data.dict, header, si, T);
				CheckCompressedData(num_elements + count <= max_elements);
				aic_block_dict_decompress(&block.data.dict, dest, scratch_buffer, T);
				break;
			case AIC_BT_PFOR:
				count = aic_block_pfor_prepare(&block.data.pfor, header, si, T);
				CheckCompressedData(num_elements + count <= max_elements);
				aic_block_pfor_decompress(&block.data.pfor, dest, scratch_buffer, T);
				break;
			default:
				CheckCompressedData(false);
		}
		num_elements += count;
		dest += count * elem_bytes;
	}
	return num_elements;
}

size_t
aic_parse_one_block(AICBlock *block, StringInfo si, fl_elem_width_t T)
{
	uint32 header;
	uint8 *comp_ptr = (uint8 *) consumeCompressedData(si, sizeof(uint32));

	header = aic_load_le32(comp_ptr);
	block->type = (AICBlockType) (header & 0x7);
	switch (block->type)
	{
		case AIC_BT_RLE:
			return aic_block_rle_prepare(&block->data.rle, header, si);
		case AIC_BT_FOR:
			return aic_block_for_prepare(&block->data.for_, header, si, T);
		case AIC_BT_DFOR:
			return aic_block_dfor_prepare(&block->data.dfor, header, si, T);
		case AIC_BT_DICT:
			return aic_block_dict_prepare(&block->data.dict, header, si, T);
		case AIC_BT_PFOR:
			return aic_block_pfor_prepare(&block->data.pfor, header, si, T);
		default:
			CheckCompressedData(false);
	}
	return 0;
}

size_t
aic_decompress_one_block(const AICBlock *block, uint8 *dest, uint8 *scratch_buffer,
						 fl_elem_width_t T)
{
	switch (block->type)
	{
		case AIC_BT_RLE:
			return aic_block_rle_decompress(&block->data.rle, dest, T);
		case AIC_BT_FOR:
			return aic_block_for_decompress(&block->data.for_, dest, scratch_buffer, T);
		case AIC_BT_DFOR:
			return aic_block_dfor_decompress(&block->data.dfor, dest, scratch_buffer, T);
		case AIC_BT_DICT:
			return aic_block_dict_decompress(&block->data.dict, dest, scratch_buffer, T);
		case AIC_BT_PFOR:
			return aic_block_pfor_decompress(&block->data.pfor, dest, scratch_buffer, T);
		default:
			CheckCompressedData(false);
	}
	return 0;
}
