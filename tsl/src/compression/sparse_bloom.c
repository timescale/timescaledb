/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include "postgres.h"

#include <utils/builtins.h>
#include <utils/typcache.h>

#include "compression.h"

#include "batch_metadata_builder.h"

typedef struct BloomMetadataBuilder
{
	BatchMetadataBuilder functions;

	Oid type_oid;
	bool empty;
	bool has_null;

	bool type_by_val;
	int16 type_len;
	Oid hash_proc_oid;
	void *bloom;

	int16 bloom_attr_offset;
} BloomMetadataBuilder;

static void bloom_update_val(void *builder_, Datum val);
static void bloom_update_null(void *builder_);
static void bloom_insert_to_compressed_row(void *builder_, RowCompressor *compressor);
static void bloom_reset(void *builder_, RowCompressor *compressor);

BatchMetadataBuilder *
batch_metadata_builder_bloom_create(Oid type_oid, int bloom_attr_offset)
{
	BloomMetadataBuilder *builder = palloc(sizeof(*builder));
	TypeCacheEntry *type = lookup_type_cache(type_oid, TYPECACHE_HASH_PROC);

	if (!OidIsValid(type->hash_proc))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("could not identify a hashing function for type %s",
						format_type_be(type_oid))));
	}

	*builder = (BloomMetadataBuilder){
		.functions =
			(BatchMetadataBuilder){
				.update_val = bloom_update_val,
				.update_null = bloom_update_null,
				.insert_to_compressed_row = bloom_insert_to_compressed_row,
				.reset = bloom_reset,
			},
		.type_oid = type_oid,
		.empty = true,
		.has_null = false,
		.type_by_val = type->typbyval,
		.type_len = type->typlen,
		.bloom_attr_offset = bloom_attr_offset,
		.hash_proc_oid = type->hash_proc,
	};

	return &builder->functions;
}

void
bloom_update_val(void *builder_, Datum val)
{
	// BloomMetadataBuilder *builder = (BloomMetadataBuilder *) builder_;
}

void
bloom_update_null(void *builder_)
{
	BloomMetadataBuilder *builder = (BloomMetadataBuilder *) builder_;
	builder->has_null = true;
}

static void
bloom_insert_to_compressed_row(void *builder_, RowCompressor *compressor)
{
	BloomMetadataBuilder *builder = (BloomMetadataBuilder *) builder_;

	compressor->compressed_is_null[builder->bloom_attr_offset] = true;
	compressor->compressed_values[builder->bloom_attr_offset] = 0;
}

static void
bloom_reset(void *builder_, RowCompressor *compressor)
{
	BloomMetadataBuilder *builder = (BloomMetadataBuilder *) builder_;

	builder->empty = true;
	builder->has_null = false;

	compressor->compressed_is_null[builder->bloom_attr_offset] = true;
	compressor->compressed_values[builder->bloom_attr_offset] = 0;
}
