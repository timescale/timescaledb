/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

/*
 * Adaptive Integer Compressor (AIC) - general purpose integer compression.
 *
 * AIC is designed to handle a variety of data distributions including
 * low cardinality data, repeated values, data with small deltas and mixed regimes
 * within the same block with potential outliers.
 *
 * The algorithm is based on ideas from the FastLanes and PFOR compressors, and also
 * heavily adapted for the use cases and constraints of TimescaleDB. It is
 * designed to utilize SIMD instructions on modern CPUs, and as a consequence it
 * operates on fixed-size blocks of 256 values, which allows for efficient vectorized
 * processing.
 *
 * At the start of each block, we perform an incremental analysis of the data to determine
 * the characteristics of the block, and then we use this information to decide which
 * compression method to use for the block.
 *
 * The actual compression of the block is then performed based on the analysis results.
 * The compressed block uses one of these techniques:
 *
 *  - Run-length encoding (RLE) for blocks with low cardinality and many repeated values.
 *  - Frame of reference (FOR) for blocks with small deltas between values.
 *  - Patched FOR (PFOR), where we store the majority of the values using FOR, and then we
 *    store the outliers separately.
 *  - Dictionary encoding for blocks for low cardinality data.
 *  - Delta FOR, where we first compute the strided deltas of the values and then we
 *    apply FOR on the deltas and the anchors.
 */

#include <postgres.h>
#include <lib/stringinfo.h>

#include "aic/aic_bulk_decompress.h"
#include "aic/aic_iter_decompress.h"
#include "aic/aic_send_recv.h"
#include "compression/compression.h"

extern Compressor *aic_compressor_for_type(Oid element_type);
extern bool aic_compressed_has_nulls(const CompressedDataHeader *header);
extern Datum tsl_aic_compressor_append(PG_FUNCTION_ARGS);
extern Datum tsl_aic_compressor_finish(PG_FUNCTION_ARGS);

#define AIC_ALGORITHM_DEFINITION                                                                   \
	{                                                                                              \
		.iterator_init_forward = aic_decompression_iterator_from_datum_forward,                    \
		.iterator_init_reverse = aic_decompression_iterator_from_datum_reverse,                    \
		.decompress_all = aic_decompress_all, .compressed_data_send = aic_compressed_send,         \
		.compressed_data_recv = aic_compressed_recv,                                               \
		.compressor_for_type = aic_compressor_for_type,                                            \
		.compressed_data_storage = TOAST_STORAGE_EXTERNAL,                                         \
	}
