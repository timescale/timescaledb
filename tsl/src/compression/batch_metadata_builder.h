/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include "funcapi.h" /* for PGFunction, FmgrInfo */

enum BatchMetadataBuilderType
{
	METADATA_BUILDER_MINMAX,
	METADATA_BUILDER_BLOOM1,
	METADATA_BUILDER_FIRSTLAST,
};

typedef struct BatchMetadataBuilder
{
	void (*update_row)(void *builder, TupleTableSlot *slot);
	void (*insert_to_compressed_row)(void *builder, Datum *compressed_values,
									 bool *compressed_is_null);
	void (*reset)(void *builder, Datum *compressed_values, bool *compressed_is_null);
	enum BatchMetadataBuilderType builder_type;
} BatchMetadataBuilder;

BatchMetadataBuilder *batch_metadata_builder_minmax_create(Oid type, Oid collation,
														   AttrNumber attnum, int min_attr_offset,
														   int max_attr_offset);

BatchMetadataBuilder *batch_metadata_builder_bloom1_create(int num_columns, const Oid *type_oids,
														   const AttrNumber *attnums,
														   int bloom_attr_offset);

BatchMetadataBuilder *batch_metadata_builder_firstlast_create(Oid type_oid, AttrNumber attnum,
															  int first_attr_offset,
															  int last_attr_offset);

/* Hasher interface common to bloom filters, used to compute the hash without updating the bloom
 * filter */
typedef struct Bloom1Hasher
{
	uint64 (*hash_values)(void *hasher, const NullableDatum *values);
	int num_columns;
} Bloom1Hasher;

Bloom1Hasher *bloom1_hasher_create(const Oid *type_oids, int num_columns);

/* Shared utilities between metadata builders */
int batch_metadata_builder_bloom1_varlena_size(void);
uint64 batch_metadata_builder_bloom1_calculate_hash(PGFunction hash_function, FmgrInfo *finfo,
													Datum needle);
void batch_metadata_builder_bloom1_update_bloom_filter_with_hash(void *varlena_ptr, uint64 hash);
void batch_metadata_builder_bloom1_insert_bloom_filter_to_compressed_row(void *bloom_varlena,
																		 int16 bloom_attr_offset,
																		 Datum *compressed_values,
																		 bool *compressed_is_null);

/* Returns true if any of the hashes is maybe present in a bloom filter. The bloom
 * filter must not be NULL; callers treat a NULL filter as "maybe present". */
extern bool bloom1_contains_any_hash_values(Datum bloom_datum, const uint64 *hashes, int nhashes);
