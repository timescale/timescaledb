/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include "funcapi.h" /* for PGFunction, FmgrInfo */

typedef struct RowCompressor RowCompressor;

enum BatchMetadataBuilderType
{
	METADATA_BUILDER_MINMAX,
	METADATA_BUILDER_BLOOM1,
	METADATA_BUILDER_BLOOM1_COMPOSITE,
};

typedef struct BatchMetadataBuilder
{
	void (*update_val)(void *builder, Datum val);
	void (*update_null)(void *builder);

	void (*insert_to_compressed_row)(void *builder, RowCompressor *compressor);

	void (*reset)(void *builder, RowCompressor *compressor);

	enum BatchMetadataBuilderType builder_type;
} BatchMetadataBuilder;

BatchMetadataBuilder *batch_metadata_builder_minmax_create(Oid type, Oid collation,
														   int min_attr_offset,
														   int max_attr_offset);

BatchMetadataBuilder *batch_metadata_builder_bloom1_create(Oid type, int bloom_attr_offset);

BatchMetadataBuilder *batch_metadata_builder_bloom1_composite_create(const Oid *type_oids,
																	 int num_columns,
																	 int bloom_attr_offset);

/* Shared utilities between metadata builders */
int batch_metadata_builder_bloom1_varlena_size(void);
uint64 batch_metadata_builder_bloom1_calculate_hash(PGFunction hash_function, FmgrInfo *finfo,
													Datum needle);
void batch_metadata_builder_bloom1_update_bloom_filter_with_hash(void *bloom_varlena, uint64 hash);
void batch_metadata_builder_bloom1_insert_bloom_filter_to_compressed_row(void *bloom_varlena,
																		 int16 bloom_attr_offset,
																		 RowCompressor *compressor);
uint64 batch_metadata_builder_bloom1_composite_get_last_hash(void *builder);