/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

typedef struct RowCompressor RowCompressor;

typedef struct BatchMetadataBuilder
{
	void (*update_val)(void *builder, Datum val);
	void (*update_null)(void *builder);

	void (*insert_to_compressed_row)(void *builder, RowCompressor *compressor);

	void (*reset)(void *builder, RowCompressor *compressor);
} BatchMetadataBuilder;

BatchMetadataBuilder *batch_metadata_builder_minmax_create(Oid type, Oid collation,
														   int min_attr_offset,
														   int max_attr_offset);

BatchMetadataBuilder *batch_metadata_builder_bloom1_create(Oid type, int bloom_attr_offset);
