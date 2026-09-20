/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <string.h>
#include "rr_blocks.h"
#include "compression/algorithms/fastlanes/fastlanes.h"
#include "compression/algorithms/fastlanes/fastlanes_types.h"
#include "compression/compression.h"
#include "rr_internal.h"
#include "rr_utils.h"
#include <guc.h>
#include <stdlib.h>

/* please read the README.md file for more details. */

/* Internal block helpers */

/* RLE: */
static inline size_t rr_block_rle_size(const RRBlockRLE *block);
static inline uint8 *rr_block_rle_serialize(const RRBlockRLE *block, uint8 *dest, size_t *max_size);
static inline size_t rr_block_rle_prepare(RRBlockRLE *block, uint32 header, StringInfo si);
static inline size_t rr_block_rle_decompress(const RRBlockRLE *block, uint8 *dest,
											 fl_elem_width_t T);

/* FOR: */
static inline size_t rr_block_for_size(const RRBlockFOR *block);
static inline uint8 *rr_block_for_serialize(const RRBlockFOR *block, uint8 *dest, size_t *max_size);
static inline size_t rr_block_for_prepare(RRBlockFOR *block, uint32 header, StringInfo si,
										  fl_elem_width_t T);
static inline size_t rr_block_for_decompress(const RRBlockFOR *block, uint8 *dest,
											 uint8 *scratch_buffer, fl_elem_width_t T);

/* DFOR: */
static inline size_t rr_block_dfor_size(const RRBlockDFOR *block);
static inline uint8 *rr_block_dfor_serialize(const RRBlockDFOR *block, uint8 *dest,
											 size_t *max_size);
static inline size_t rr_block_dfor_prepare(RRBlockDFOR *block, uint32 header, StringInfo si,
										   fl_elem_width_t T);
static inline size_t rr_block_dfor_decompress(const RRBlockDFOR *block, uint8 *dest,
											  uint8 *scratch_buffer, fl_elem_width_t T);

/* DICT */
static inline size_t rr_block_dict_size(const RRBlockDICT *block);
static inline uint8 *rr_block_dict_serialize(const RRBlockDICT *block, uint8 *dest,
											 size_t *max_size);
static inline size_t rr_block_dict_prepare(RRBlockDICT *block, uint32 header, StringInfo si,
										   fl_elem_width_t T);
static inline size_t rr_block_dict_decompress(const RRBlockDICT *block, uint8 *dest,
											  uint8 *scratch_buffer, fl_elem_width_t T);

/* PFOR */
static inline size_t rr_block_pfor_size(const RRBlockPFOR *block);
static inline uint8 *rr_block_pfor_serialize(const RRBlockPFOR *block, uint8 *dest,
											 size_t *max_size);
static inline size_t rr_block_pfor_prepare(RRBlockPFOR *block, uint32 header, StringInfo si,
										   fl_elem_width_t T);
static inline size_t rr_block_pfor_decompress(const RRBlockPFOR *block, uint8 *dest,
											  uint8 *scratch_buffer, fl_elem_width_t T);

/* free */
void
rr_free_blocks(RRBlock *block)
{
	while (block != NULL)
	{
		RRBlock *next = block->next;
		pfree(block);
		block = next;
	};
}

/* init */
void
rr_block_rle_init(RRBlockRLE *block, uint32 count, int64 base, int64 step, fl_elem_width_t T)
{
	/* the base and step values need to be sign-extended first */
	base = rr_sign_extend_u64((uint64) base, (uint8) T);
	step = rr_sign_extend_u64((uint64) step, (uint8) T);

	block->header = 0;
	block->header |= (RR_BT_RLE & 0x7);
	block->header |= ((count - 1) & 0xFFFF) << 11;
	block->base_value = base;
	block->step_value = step;

	if (base != 0)
	{
		uint8 bytes_needed = rr_signed_bytes_needed(base);
		block->header |= (bytes_needed & 0xF) << 3;
	}

	if (step != 0)
	{
		uint8 bytes_needed = rr_signed_bytes_needed(step);
		block->header |= (bytes_needed & 0xF) << 7;
	}
}

void
rr_block_for_init(RRBlockFOR *block, uint32 count, int64 base, uint8 W, fl_elem_width_t T,
				  uint8 *body)
{
	/* sign extend base first */
	base = rr_sign_extend_u64((uint64) base, (uint8) T);

	block->header = 0;
	block->header |= (RR_BT_FOR & 0x7);
	block->header |= ((count - 1) & 0xFF) << 14;
	block->header |= (W & 0x7F) << 7;
	block->base_value = base;

	if (base != 0)
	{
		uint8 bytes_needed = rr_signed_bytes_needed(base);
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
rr_block_dfor_init(RRBlockDFOR *block, uint32 count, uint16 S, int64 base, uint8 W_bases,
				   int64 delta_residual_base, uint8 W_delta_residuals, bool zigzag,
				   fl_elem_width_t T, uint8 *bases_body, uint8 *delta_residual_body)
{
	Assert(count > S);
	Assert(S >= 1 && S <= 64);

	uint32 n_delta_residuals = count - S;

	base = rr_sign_extend_u64((uint64) base, (uint8) T);
	delta_residual_base = rr_sign_extend_u64((uint64) delta_residual_base, (uint8) T);

	block->header = 0;
	block->header |= (RR_BT_DFOR & 0x7);
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
		uint8 bytes_needed = rr_signed_bytes_needed(base);
		block->header |= (bytes_needed & 0xF) << 3;
	}

	if (delta_residual_base != 0)
	{
		uint8 bytes_needed = rr_signed_bytes_needed(delta_residual_base);
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
rr_block_dict_init(RRBlockDICT *block, uint32 count, uint16 K, int64 key_base, uint8 W, uint8 W_idx,
				   bool zigzag_idxs, fl_elem_width_t T, uint8 *keys_body, uint8 *idx_body)
{
	fl_elem_width_t idx_T = zigzag_idxs ? FL_ELEM_W16 : FL_ELEM_W8;

	Assert(count >= 1);
	Assert(K >= 2 && K <= 256);
	Assert(W >= 2);
	Assert(W_idx >= 1);

	key_base = rr_sign_extend_u64((uint64) key_base, (uint8) T);

	block->header = 0;
	block->header |= (RR_BT_DICT & 0x7);
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
		uint8 bytes_needed = rr_signed_bytes_needed(key_base);
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
rr_block_pfor_init(RRBlockPFOR *block, uint32 count, int64 base, uint8 W, uint8 W_exc, uint8 n_exc,
				   const uint8 *positions, fl_elem_width_t T, uint8 *body, uint8 *exc_body)
{
	Assert(count >= 1);
	Assert(n_exc >= 1 && n_exc <= RR_PFOR_MAX_EXCEPTIONS);
	Assert(W_exc >= 1);

	base = rr_sign_extend_u64((uint64) base, (uint8) T);

	block->header = 0;

	uint8 bytes_needed = rr_block_header_scalar_len(base);

	block->header |= (RR_BT_PFOR & 0x7);
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
rr_block_rle_size(const RRBlockRLE *block)
{
	uint32 header = block->header;
	uint8 base_len_bytes = (header >> 3) & 0xF;
	uint8 step_len_bytes = (header >> 7) & 0xF;
	return sizeof(uint32) + base_len_bytes + step_len_bytes;
}

size_t
rr_block_for_size(const RRBlockFOR *block)
{
	uint32 header = block->header;
	uint8 base_len_bytes = (header >> 3) & 0xF;
	return sizeof(uint32) + base_len_bytes + block->packed_body_size;
}

size_t
rr_block_dfor_size(const RRBlockDFOR *block)
{
	uint32 header = block->header;
	uint8 base_len_bytes = (header >> 3) & 0xF;
	uint8 delta_residual_base_len_bytes = (header >> 20) & 0xF;
	return sizeof(uint32) + sizeof(uint8) + base_len_bytes + delta_residual_base_len_bytes +
		   block->packed_bases_size + block->packed_delta_residual_size;
}

size_t
rr_block_dict_size(const RRBlockDICT *block)
{
	uint32 header = block->header;
	uint8 key_base_len_bytes = (header >> 3) & 0xF;
	return sizeof(uint32) + sizeof(uint8) + key_base_len_bytes + block->packed_keys_size +
		   block->packed_idx_size;
}

size_t
rr_block_pfor_size(const RRBlockPFOR *block)
{
	uint32 header = block->header;
	/* count_exc is staged by both init and prepare */
	uint32 n_exc = (uint32) block->count_exc + 1;
	size_t scalar_bytes = sizeof(uint8) + ((header >> 3) & 0xF);

	return sizeof(uint32) + scalar_bytes + block->packed_body_size + n_exc + block->packed_exc_size;
}

size_t
rr_total_block_size(RRBlock *block)
{
	size_t total_size = 0;
	while (block != NULL)
	{
		switch (block->type)
		{
			case RR_BT_RLE:
				total_size += rr_block_rle_size(&block->data.rle);
				break;
			case RR_BT_FOR:
				total_size += rr_block_for_size(&block->data.for_);
				break;
			case RR_BT_DFOR:
				total_size += rr_block_dfor_size(&block->data.dfor);
				break;
			case RR_BT_DICT:
				total_size += rr_block_dict_size(&block->data.dict);
				break;
			case RR_BT_PFOR:
				total_size += rr_block_pfor_size(&block->data.pfor);
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
rr_block_rle_serialize(const RRBlockRLE *block, uint8 *dest, size_t *max_size)
{
	CheckCompressedData(block != NULL);
	CheckCompressedData(dest != NULL);
	CheckCompressedData(max_size != NULL);

	size_t required_size = rr_block_rle_size(block);
	CheckCompressedData(required_size <= *max_size);
	uint8 *start_dest PG_USED_FOR_ASSERTS_ONLY = dest;

	uint32 header = block->header;
	uint8 base_len_bytes = (header >> 3) & 0xF;
	uint8 step_len_bytes = (header >> 7) & 0xF;

	rr_store_le32(dest, block->header);
	dest += sizeof(uint32);

	if (base_len_bytes > 0)
	{
		rr_store_le64(dest, (uint64) block->base_value, base_len_bytes);
		dest += base_len_bytes;
	}

	if (step_len_bytes > 0)
	{
		rr_store_le64(dest, (uint64) block->step_value, step_len_bytes);
		dest += step_len_bytes;
	}

	*max_size -= required_size;
	Assert((size_t) (dest - start_dest) == required_size);
	return dest;
}

uint8 *
rr_block_for_serialize(const RRBlockFOR *block, uint8 *dest, size_t *max_size)
{
	CheckCompressedData(block != NULL);
	CheckCompressedData(dest != NULL);
	CheckCompressedData(max_size != NULL);

	size_t required_size = rr_block_for_size(block);
	CheckCompressedData(required_size <= *max_size);
	uint8 *start_dest PG_USED_FOR_ASSERTS_ONLY = dest;

	uint32 header = block->header;
	uint8 base_len_bytes = (header >> 3) & 0xF;

	rr_store_le32(dest, block->header);
	dest += sizeof(uint32);

	if (base_len_bytes > 0)
	{
		rr_store_le64(dest, (uint64) block->base_value, base_len_bytes);
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
rr_block_dfor_serialize(const RRBlockDFOR *block, uint8 *dest, size_t *max_size)
{
	CheckCompressedData(block != NULL);
	CheckCompressedData(dest != NULL);
	CheckCompressedData(max_size != NULL);

	size_t required_size = rr_block_dfor_size(block);
	CheckCompressedData(required_size <= *max_size);
	uint8 *start_dest PG_USED_FOR_ASSERTS_ONLY = dest;

	uint32 header = block->header;
	uint8 base_len_bytes = (header >> 3) & 0xF;
	uint8 delta_residual_base_len_bytes = (header >> 20) & 0xF;

	rr_store_le32(dest, block->header);
	dest += sizeof(uint32);

	*dest = block->count_delta_residuals;
	dest++;

	if (base_len_bytes > 0)
	{
		rr_store_le64(dest, (uint64) block->base_value, base_len_bytes);
		dest += base_len_bytes;
	}

	if (delta_residual_base_len_bytes > 0)
	{
		rr_store_le64(dest,
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
rr_block_dict_serialize(const RRBlockDICT *block, uint8 *dest, size_t *max_size)
{
	CheckCompressedData(block != NULL);
	CheckCompressedData(dest != NULL);
	CheckCompressedData(max_size != NULL);

	size_t required_size = rr_block_dict_size(block);
	CheckCompressedData(required_size <= *max_size);
	uint8 *start_dest PG_USED_FOR_ASSERTS_ONLY = dest;

	uint32 header = block->header;
	uint8 key_base_len_bytes = (header >> 3) & 0xF;

	rr_store_le32(dest, block->header);
	dest += sizeof(uint32);

	*dest = block->count_idx;
	dest++;

	if (key_base_len_bytes > 0)
	{
		rr_store_le64(dest, (uint64) block->key_base_value, key_base_len_bytes);
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
rr_block_pfor_serialize(const RRBlockPFOR *block, uint8 *dest, size_t *max_size)
{
	CheckCompressedData(block != NULL);
	CheckCompressedData(dest != NULL);
	CheckCompressedData(max_size != NULL);

	size_t required_size = rr_block_pfor_size(block);
	CheckCompressedData(required_size <= *max_size);
	uint8 *start_dest PG_USED_FOR_ASSERTS_ONLY = dest;

	uint32 header = block->header;
	uint32 n_exc = (uint32) block->count_exc + 1;
	uint8 base_len_bytes = (header >> 3) & 0xF;

	rr_store_le32(dest, block->header);
	dest += sizeof(uint32);

	*dest = block->count_exc;
	dest++;
	rr_store_le64(dest, (uint64) block->base_value, base_len_bytes);
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
rr_serialize_blocks(RRBlock *block, uint8 *dest, size_t max_size)
{
	Assert(block != NULL);
	Assert(dest != NULL);
	Assert(max_size != 0);

	while (block != NULL)
	{
		RRBlock *next = block->next;
		switch (block->type)
		{
			case RR_BT_RLE:
				dest = rr_block_rle_serialize(&block->data.rle, dest, &max_size);
				break;
			case RR_BT_FOR:
				dest = rr_block_for_serialize(&block->data.for_, dest, &max_size);
				break;
			case RR_BT_DFOR:
				dest = rr_block_dfor_serialize(&block->data.dfor, dest, &max_size);
				break;
			case RR_BT_DICT:
				dest = rr_block_dict_serialize(&block->data.dict, dest, &max_size);
				break;
			case RR_BT_PFOR:
				dest = rr_block_pfor_serialize(&block->data.pfor, dest, &max_size);
				break;
			default:
				CheckCompressedData(false);
		}
		CheckCompressedData(dest != NULL);
		block = next;
	};

	return (max_size == 0);
}

/* preparation for decompression: the *_prepare functions set the RRBlock*
 * values and body pointers from the serialized data from the StringInfo.
 * they return the number of elements in the block, which is needed for
 * decompression.
 */
size_t
rr_block_rle_prepare(RRBlockRLE *block, uint32 header, StringInfo si)
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
		block->base_value = rr_read_signed(base_ptr, base_len_bytes);
	}

	if (step_len_bytes > 0)
	{
		uint8 *step_ptr = (uint8 *) consumeCompressedData(si, step_len_bytes);
		block->step_value = rr_read_signed(step_ptr, step_len_bytes);
	}

	return num_elements;
}

size_t
rr_block_for_prepare(RRBlockFOR *block, uint32 header, StringInfo si, fl_elem_width_t T)
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
		block->base_value = rr_read_signed(base_ptr, base_len_bytes);
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
rr_block_dfor_prepare(RRBlockDFOR *block, uint32 header, StringInfo si, fl_elem_width_t T)
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
		block->base_value = rr_read_signed(base_ptr, base_len_bytes);
	}

	if (delta_residual_base_len_bytes > 0)
	{
		uint8 *res_ptr = (uint8 *) consumeCompressedData(si, delta_residual_base_len_bytes);
		block->delta_residual_base_value = rr_read_signed(res_ptr, delta_residual_base_len_bytes);
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
rr_block_dict_prepare(RRBlockDICT *block, uint32 header, StringInfo si, fl_elem_width_t T)
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
		block->key_base_value = rr_read_signed(base_ptr, key_base_len_bytes);
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
rr_block_pfor_prepare(RRBlockPFOR *block, uint32 header, StringInfo si, fl_elem_width_t T)
{
	Assert(block != NULL);
	Assert(si != NULL);

	block->header = header;

	RRBlockType type PG_USED_FOR_ASSERTS_ONLY = (RRBlockType) (header & 0x7);
	uint8 W;
	uint8 W_exc;
	size_t num_elements;
	uint32 n_exc;
	uint32 i;

	Assert(type == RR_BT_PFOR);
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
		block->base_value = rr_read_signed(base_ptr, base_len_bytes);
	}

	CheckCompressedData(W <= (uint8) T);
	CheckCompressedData(W_exc >= 1);
	CheckCompressedData(W_exc <= (uint8) T);
	CheckCompressedData((uint32) W + (uint32) W_exc <= (uint32) (uint8) T);
	CheckCompressedData(n_exc <= RR_PFOR_MAX_EXCEPTIONS);
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

/* clang-tidy doesn't like the type argument and insists on using
 * parens, but that doesn't compile */
/* NOLINTBEGIN(bugprone-macro-parentheses) */

/* decompression */
#define RR_RLE_BROADCAST(TY, STY)                                                                  \
	static inline void rr_rle_broadcast_##TY(uint8 *dest, STY base, uint16 count)                  \
	{                                                                                              \
		STY *ptr = (STY *) dest;                                                                   \
		for (uint16 i = 0; i < count; ++i)                                                         \
		{                                                                                          \
			ptr[i] = base;                                                                         \
		}                                                                                          \
	}

RR_RLE_BROADCAST(uint16, int16)
RR_RLE_BROADCAST(uint32, int32)
RR_RLE_BROADCAST(uint64, int64)
#undef RR_RLE_BROADCAST

#define RR_DELTA_RLE_BROADCAST(TY, STY)                                                            \
	static inline void rr_delta_rle_broadcast_##TY(uint8 *dest, STY base, STY step, uint16 count)  \
	{                                                                                              \
		TY *ptr = (TY *) dest;                                                                     \
		TY b = (TY) base;                                                                          \
		TY s = (TY) step;                                                                          \
		for (uint16 i = 0; i < count; ++i)                                                         \
		{                                                                                          \
			ptr[i] = (TY) ((uint64) b + (uint64) i * (uint64) s);                                  \
		}                                                                                          \
	}

RR_DELTA_RLE_BROADCAST(uint16, int16)
RR_DELTA_RLE_BROADCAST(uint32, int32)
RR_DELTA_RLE_BROADCAST(uint64, int64)
#undef RR_DELTA_RLE_BROADCAST

size_t
rr_block_rle_decompress(const RRBlockRLE *block, uint8 *dest, fl_elem_width_t T)
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
				rr_rle_broadcast_uint16(dest, block->base_value, num_elements);
				break;
			case FL_ELEM_W32:
				rr_rle_broadcast_uint32(dest, block->base_value, num_elements);
				break;
			case FL_ELEM_W64:
				rr_rle_broadcast_uint64(dest, block->base_value, num_elements);
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
				rr_delta_rle_broadcast_uint16(dest,
											  block->base_value,
											  block->step_value,
											  num_elements);
				break;
			case FL_ELEM_W32:
				rr_delta_rle_broadcast_uint32(dest,
											  block->base_value,
											  block->step_value,
											  num_elements);
				break;
			case FL_ELEM_W64:
				rr_delta_rle_broadcast_uint64(dest,
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
rr_block_for_decompress(const RRBlockFOR *block, uint8 *dest, uint8 *scratch_buffer,
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
	if (W > 0)
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

/* rr_dfor_reconstruct_.. : for in-place reconstruction of the original values
 * from stride-S delta residual values. The zigzag flag indicates whether the
 * elements were zigzag encoded or not.
 */
#define RR_DFOR_RECONSTRUCT(TY)                                                                    \
	static inline void rr_dfor_reconstruct_##TY(uint8 *dest, uint16 S, uint32 n, bool zigzag)      \
	{                                                                                              \
		TY *v = (TY *) dest;                                                                       \
		if (zigzag)                                                                                \
		{                                                                                          \
			for (uint32 i = S; i < n; ++i)                                                         \
			{                                                                                      \
				TY zz = v[i];                                                                      \
				v[i] = (TY) (v[i - S] + rr_zigzag_decode_##TY(zz));                                \
			}                                                                                      \
		}                                                                                          \
		else                                                                                       \
		{                                                                                          \
			for (uint32 i = S; i < n; ++i)                                                         \
			{                                                                                      \
				v[i] = v[i - S] + v[i];                                                            \
			}                                                                                      \
		}                                                                                          \
	}

RR_DFOR_RECONSTRUCT(uint16)
RR_DFOR_RECONSTRUCT(uint32)
RR_DFOR_RECONSTRUCT(uint64)
#undef RR_DFOR_RECONSTRUCT

size_t
rr_block_dfor_decompress(const RRBlockDFOR *block, uint8 *dest, uint8 *scratch_buffer,
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

	const uint8 *body =
		(block->bases_body != NULL) ? block->bases_body : block->delta_residual_body;

	/* bases into dest[0..S) */
	if (W_bases > 0)
	{
		memcpy(scratch_buffer, body, block->packed_bases_size);
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
	if (W_delta_residuals > 0)
	{
		const uint8 *delta_residual_comp = body + block->packed_bases_size;
		memcpy(scratch_buffer, delta_residual_comp, block->packed_delta_residual_size);
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
			rr_dfor_reconstruct_uint16(dest, S, (uint32) num_elements, zigzag);
			break;
		case FL_ELEM_W32:
			rr_dfor_reconstruct_uint32(dest, S, (uint32) num_elements, zigzag);
			break;
		case FL_ELEM_W64:
			rr_dfor_reconstruct_uint64(dest, S, (uint32) num_elements, zigzag);
			break;
		default:
			CheckCompressedData(false);
	}

	return num_elements;
}

/* rr_dict_reconstruct_.. : write each element's dictionary value,
 * selected by its unpacked index, into dest.
 *
 * depending on the zigzag flag we either use the indexes as-is
 * or decode them from the delta-coded index form. the zigzag
 * flag also determines the index array's element type: zigzag
 * indexes are uint16, non-zigzag indexes are uint8.
 *
 * delta endoced indices arrive as zigzag deltas and are
 * prefix-summed on the fly.
 *
 * the idx_mask is a cheap way to ensure that the index is in
 * close range of the dictionary keys. this offers a cheap
 * branch-less optimisation for the non-zigzag case, and
 * more importantly, it prevents the zigzag case from going
 * wildly out of range (in case of corrupt data)
 */
#define RR_DICT_RECONSTRUCT(TY)                                                                    \
	static inline void rr_dict_reconstruct_##TY(uint8 *dest,                                       \
												const TY *keys,                                    \
												const uint16 *idx_area,                            \
												uint32 n,                                          \
												uint32 idx_mask,                                   \
												bool zigzag_idxs)                                  \
	{                                                                                              \
		TY *v = (TY *) dest;                                                                       \
		if (zigzag_idxs)                                                                           \
		{                                                                                          \
			const uint16 *idx = idx_area;                                                          \
			uint32 prev = 0;                                                                       \
			for (uint32 i = 0; i < n; ++i)                                                         \
			{                                                                                      \
				uint32 zz = (uint32) idx[i];                                                       \
				prev += rr_zigzag_decode_uint32(zz);                                               \
				v[i] = keys[prev & idx_mask];                                                      \
			}                                                                                      \
		}                                                                                          \
		else                                                                                       \
		{                                                                                          \
			const uint8 *idx = (const uint8 *) idx_area;                                           \
			for (uint32 i = 0; i < n; ++i)                                                         \
			{                                                                                      \
				v[i] = keys[(uint32) idx[i] & idx_mask];                                           \
			}                                                                                      \
		}                                                                                          \
	}

RR_DICT_RECONSTRUCT(uint16)
RR_DICT_RECONSTRUCT(uint32)
RR_DICT_RECONSTRUCT(uint64)
#undef RR_DICT_RECONSTRUCT

size_t
rr_block_dict_decompress(const RRBlockDICT *block, uint8 *dest, uint8 *scratch_buffer,
						 fl_elem_width_t T)
{
	Assert(block != NULL);
	Assert(dest != NULL);

	RRIntBuffer keys;
	uint16 idx[RR_FIXED_BLOCK_MAX_COUNT];

	uint32 header = block->header;
	uint8 W = (header >> 7) & 0x7F;
	uint16 K = (uint16) (((header >> 14) & 0xFF) + 1);
	uint8 W_idx = (header >> 22) & 0x7F;
	bool zigzag_idxs = (bool) ((header >> 29) & 0x1);
	fl_elem_width_t idx_T = zigzag_idxs ? FL_ELEM_W16 : FL_ELEM_W8;
	uint32 num_elements = (size_t) block->count_idx + 1;
	uint32 idx_mask = ((uint32) 1 << rr_bit_width_u64((uint64) (K - 1))) - 1;
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
	 * rr_dict_reconstruct_* branch-less instead of a per-element
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
			rr_dict_reconstruct_uint16(dest,
									   keys.uint16_elem,
									   idx,
									   num_elements,
									   idx_mask,
									   zigzag_idxs);
			break;
		case FL_ELEM_W32:
			rr_dict_reconstruct_uint32(dest,
									   keys.uint32_elem,
									   idx,
									   num_elements,
									   idx_mask,
									   zigzag_idxs);
			break;
		case FL_ELEM_W64:
			rr_dict_reconstruct_uint64(dest,
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

/* rr_pfor_reconstruct_.. : add each exception's high bits back into
 * its body value. positions were validated against the element
 * count at prepare time, so every write lands inside dest. */
#define RR_PFOR_RECONSTRUCT(TY)                                                                    \
	static inline void rr_pfor_reconstruct_##TY(uint8 *dest,                                       \
												const uint8 *exc_area,                             \
												const uint8 *positions,                            \
												uint32 n_exc,                                      \
												uint8 b)                                           \
	{                                                                                              \
		TY *v = (TY *) dest;                                                                       \
		const TY *exc = (const TY *) exc_area;                                                     \
		for (uint32 e = 0; e < n_exc; ++e)                                                         \
		{                                                                                          \
			v[positions[e]] += (TY) (exc[e] << b);                                                 \
		}                                                                                          \
	}

RR_PFOR_RECONSTRUCT(uint16)
RR_PFOR_RECONSTRUCT(uint32)
RR_PFOR_RECONSTRUCT(uint64)
#undef RR_PFOR_RECONSTRUCT

/* NOLINTEND(bugprone-macro-parentheses) */

size_t
rr_block_pfor_decompress(const RRBlockPFOR *block, uint8 *dest, uint8 *scratch_buffer,
						 fl_elem_width_t T)
{
	Assert(block != NULL);
	Assert(dest != NULL);

	uint32 header = block->header;
	RRBlockType type PG_USED_FOR_ASSERTS_ONLY = (RRBlockType) (header & 0x7);
	uint8 W;
	uint8 W_exc;
	uint32 num_elements;
	uint32 n_exc = (uint32) block->count_exc + 1;
	const uint8 *body = block->body;

	Assert(type == RR_BT_PFOR);
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
	 * (at RR_SCRATCH_STAGING_BYTES offset) is used as the destination
	 * of the unpacked exception data. from the decompression
	 * perspective, these two are both temporary data that will
	 * be merged into the main data stream.
	 */
	uint8 *exc_vals = scratch_buffer + RR_SCRATCH_STAGING_BYTES;
	const uint8 *exc_comp = body + block->packed_body_size + n_exc;
	Assert(fl_input_count(n_exc, T) * (size_t) ((uint8) T / 8) <= RR_PFOR_EXC_BYTES);
	memcpy(scratch_buffer, exc_comp, block->packed_exc_size);
	fl_unpack(scratch_buffer, exc_vals, n_exc, W_exc, T);

	/* patch up the results at 'dest' */
	switch (T)
	{
		case FL_ELEM_W16:
			rr_pfor_reconstruct_uint16(dest, exc_vals, block->exc_positions, n_exc, W);
			break;
		case FL_ELEM_W32:
			rr_pfor_reconstruct_uint32(dest, exc_vals, block->exc_positions, n_exc, W);
			break;
		case FL_ELEM_W64:
			rr_pfor_reconstruct_uint64(dest, exc_vals, block->exc_positions, n_exc, W);
			break;
		default:
			CheckCompressedData(false);
	}

	return num_elements;
}

size_t
rr_decompress_blocks(uint32 num_blocks, StringInfo si, uint8 *dest, uint8 *scratch_buffer,
					 size_t max_elements, fl_elem_width_t T)
{
	size_t elem_bytes = (uint8) T / 8;
	size_t num_elements = 0;
	uint32 header;
	uint32 i;

	for (i = 0; i < num_blocks; ++i)
	{
		RRBlock block;
		uint32 count = 0;
		uint8 *comp_ptr = (uint8 *) consumeCompressedData(si, sizeof(uint32));

		header = rr_load_le32(comp_ptr);
		block.type = (RRBlockType) (header & 0x7);
		switch (block.type)
		{
			case RR_BT_RLE:
				count = rr_block_rle_prepare(&block.data.rle, header, si);
				CheckCompressedData(num_elements + count <= max_elements);
				rr_block_rle_decompress(&block.data.rle, dest, T);
				break;
			case RR_BT_FOR:
				count = rr_block_for_prepare(&block.data.for_, header, si, T);
				CheckCompressedData(num_elements + count <= max_elements);
				rr_block_for_decompress(&block.data.for_, dest, scratch_buffer, T);
				break;
			case RR_BT_DFOR:
				count = rr_block_dfor_prepare(&block.data.dfor, header, si, T);
				CheckCompressedData(num_elements + count <= max_elements);
				rr_block_dfor_decompress(&block.data.dfor, dest, scratch_buffer, T);
				break;
			case RR_BT_DICT:
				count = rr_block_dict_prepare(&block.data.dict, header, si, T);
				CheckCompressedData(num_elements + count <= max_elements);
				rr_block_dict_decompress(&block.data.dict, dest, scratch_buffer, T);
				break;
			case RR_BT_PFOR:
				count = rr_block_pfor_prepare(&block.data.pfor, header, si, T);
				CheckCompressedData(num_elements + count <= max_elements);
				rr_block_pfor_decompress(&block.data.pfor, dest, scratch_buffer, T);
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
rr_parse_one_block(RRBlock *block, StringInfo si, fl_elem_width_t T)
{
	uint32 header;
	uint8 *comp_ptr = (uint8 *) consumeCompressedData(si, sizeof(uint32));

	header = rr_load_le32(comp_ptr);
	block->type = (RRBlockType) (header & 0x7);
	switch (block->type)
	{
		case RR_BT_RLE:
			return rr_block_rle_prepare(&block->data.rle, header, si);
		case RR_BT_FOR:
			return rr_block_for_prepare(&block->data.for_, header, si, T);
		case RR_BT_DFOR:
			return rr_block_dfor_prepare(&block->data.dfor, header, si, T);
		case RR_BT_DICT:
			return rr_block_dict_prepare(&block->data.dict, header, si, T);
		case RR_BT_PFOR:
			return rr_block_pfor_prepare(&block->data.pfor, header, si, T);
		default:
			CheckCompressedData(false);
	}
	return 0;
}

size_t
rr_decompress_one_block(const RRBlock *block, uint8 *dest, uint8 *scratch_buffer, fl_elem_width_t T)
{
	switch (block->type)
	{
		case RR_BT_RLE:
			return rr_block_rle_decompress(&block->data.rle, dest, T);
		case RR_BT_FOR:
			return rr_block_for_decompress(&block->data.for_, dest, scratch_buffer, T);
		case RR_BT_DFOR:
			return rr_block_dfor_decompress(&block->data.dfor, dest, scratch_buffer, T);
		case RR_BT_DICT:
			return rr_block_dict_decompress(&block->data.dict, dest, scratch_buffer, T);
		case RR_BT_PFOR:
			return rr_block_pfor_decompress(&block->data.pfor, dest, scratch_buffer, T);
		default:
			CheckCompressedData(false);
	}
	return 0;
}
