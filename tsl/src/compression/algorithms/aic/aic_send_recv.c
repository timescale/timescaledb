/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "aic_send_recv.h"
#include "aic_blocks.h"
#include "aic_internal.h"
#include "aic_nulls.h"
#include <guc.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>

void
aic_compressed_send(CompressedDataHeader *header, StringInfo buffer)
{
	const AICCompressed *data = (const AICCompressed *) header;
	Assert(header->compression_algorithm == COMPRESSION_ALGORITHM_AIC);

	uint32 stream_size = VARSIZE(data) - sizeof(AICCompressed);

	pq_sendbyte(buffer, data->flags);
	pq_sendint16(buffer, data->valid_count);
	pq_sendint16(buffer, data->null_count);
	pq_sendint16(buffer, data->num_blocks);
	pq_sendbytes(buffer, (const char *) data->values, stream_size);
}

static void
aic_recv_validate(const AICCompressed *compressed)
{
	StringInfoData si = { .data = (char *) compressed, .len = VARSIZE(compressed) };
	(void) consumeCompressedData(&si, sizeof(AICCompressed));

	fl_elem_width_t T = aic_global_flag_to_elem_width(compressed->flags);
	size_t total_count = (size_t) compressed->valid_count + compressed->null_count;

	if (compressed->null_count > 0)
	{
		size_t bitmap_words = (total_count + 63) / 64;
		size_t validity_size = bitmap_words * sizeof(uint64);
		uint64 *bitmap = palloc(validity_size);
		size_t n_valid_runs;
		size_t decoded = aic_null_bitmap_decode(&si,
												total_count,
												compressed->null_count,
												bitmap,
												(AICGlobalFlags) compressed->flags,
												&n_valid_runs);
		CheckCompressedData(
			(decoded == validity_size && (compressed->flags & AIC_FLAG_NULL_SUMMARY) == 0) ||
			(decoded < validity_size && (compressed->flags & AIC_FLAG_NULL_SUMMARY) != 0));
		pfree(bitmap);
	}

	CheckCompressedData(compressed->num_blocks <=
						((GLOBAL_MAX_ROWS_PER_COMPRESSION / AIC_DELTA_RLE_CHECKPOINT) + 1));

	size_t num_elements = 0;
	for (uint16 i = 0; i < compressed->num_blocks; i++)
	{
		AICBlock blk;
		num_elements += aic_parse_one_block(&blk, &si, T);
		CheckCompressedData(num_elements <= compressed->valid_count);
	}
	CheckCompressedData(num_elements == compressed->valid_count);
	CheckCompressedData(si.cursor == si.len);
}

Datum
aic_compressed_recv(StringInfo buffer)
{
	uint8 flags = pq_getmsgbyte(buffer);
	uint16 valid_count = pq_getmsgint(buffer, 2);
	uint16 null_count = pq_getmsgint(buffer, 2);
	uint16 num_blocks = pq_getmsgint(buffer, 2);

	CheckCompressedData((flags & ~0x07) == 0);
	CheckCompressedData(valid_count > 0);
	CheckCompressedData(num_blocks > 0);
	CheckCompressedData((uint32) valid_count + null_count <= GLOBAL_MAX_ROWS_PER_COMPRESSION);

	int stream_size = buffer->len - buffer->cursor;
	size_t total_size = sizeof(AICCompressed) + (size_t) stream_size;
	CheckCompressedData(total_size <= MaxAllocSize);

	AICCompressed *compressed = palloc(total_size);
	SET_VARSIZE(&compressed->vl_len_, total_size);
	compressed->compression_algorithm = COMPRESSION_ALGORITHM_AIC;
	compressed->flags = flags;
	compressed->valid_count = valid_count;
	compressed->null_count = null_count;
	compressed->num_blocks = num_blocks;
	memcpy(compressed->values, pq_getmsgbytes(buffer, stream_size), (size_t) stream_size);

	aic_recv_validate(compressed);

	PG_RETURN_POINTER(compressed);
}
