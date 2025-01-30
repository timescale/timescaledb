/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include "postgres.h"

#include <catalog/pg_collation_d.h>
#include <common/hashfn.h>
#include <utils/builtins.h>
#include <utils/typcache.h>

#include "compression.h"

#include "batch_metadata_builder.h"

#include "utils/bloom1_sparse_index_params.h"

typedef struct Bloom1MetadataBuilder
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

	int nbits;
	bytea *bloom_bytea;
	int nbits_set;
} Bloom1MetadataBuilder;

static void bloom1_update_val(void *builder_, Datum val);
static void bloom1_update_null(void *builder_);
static void bloom1_insert_to_compressed_row(void *builder_, RowCompressor *compressor);
static void bloom1_reset(void *builder_, RowCompressor *compressor);

BatchMetadataBuilder *
batch_metadata_builder_bloom1_create(Oid type_oid, int bloom_attr_offset)
{
	Bloom1MetadataBuilder *builder = palloc(sizeof(*builder));
	TypeCacheEntry *type = lookup_type_cache(type_oid, TYPECACHE_HASH_PROC);

	if (!OidIsValid(type->hash_proc))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("could not identify a hashing function for type %s",
						format_type_be(type_oid))));
	}

	*builder = (Bloom1MetadataBuilder){
		.functions =
			(BatchMetadataBuilder){
				.update_val = bloom1_update_val,
				.update_null = bloom1_update_null,
				.insert_to_compressed_row = bloom1_insert_to_compressed_row,
				.reset = bloom1_reset,
			},
		.type_oid = type_oid,
		.empty = true,
		.has_null = false,
		.type_by_val = type->typbyval,
		.type_len = type->typlen,
		.bloom_attr_offset = bloom_attr_offset,
		.hash_proc_oid = type->hash_proc,

		.nbits = 1024 * 8,

		.nbits_set = 0,
	};

	Assert(builder->nbits % 8 == 0);
	const int bytea_size = VARHDRSZ + builder->nbits / 8;
	builder->bloom_bytea = palloc0(bytea_size);
	SET_VARSIZE(builder->bloom_bytea, bytea_size);

	return &builder->functions;
}

void
bloom1_update_val(void *builder_, Datum val)
{
	Bloom1MetadataBuilder *builder = (Bloom1MetadataBuilder *) builder_;

	const int nbits = builder->nbits;
	const Oid hash_proc_oid = builder->hash_proc_oid;

	/* compute the hashes, used for the bloom filter */
	uint32 datum_hash =
		DatumGetUInt32(OidFunctionCall1Coll(hash_proc_oid, /* collation = */ C_COLLATION_OID, val));
	uint32 h1 = hash_bytes_uint32_extended(datum_hash, BLOOM1_SEED_1) % nbits;
	uint32 h2 = hash_bytes_uint32_extended(datum_hash, BLOOM1_SEED_2) % nbits;

	/* compute the requested number of hashes */
	char *restrict words = VARDATA(builder->bloom_bytea);
	for (int i = 0; i < BLOOM1_HASHES; i++)
	{
		/* h1 + h2 + f(i) */
		uint32 h = (h1 + i * h2) % builder->nbits;
		uint32 byte = (h / 8);
		uint32 bit = (h % 8);

		/* if the bit is not set, set it and remember we did that */
		if (!(words[byte] & (0x01 << bit)))
		{
			words[byte] |= (0x01 << bit);
			builder->nbits_set++;
		}
	}
}

void
bloom1_update_null(void *builder_)
{
	Bloom1MetadataBuilder *builder = (Bloom1MetadataBuilder *) builder_;
	builder->has_null = true;
}

static void
bloom1_insert_to_compressed_row(void *builder_, RowCompressor *compressor)
{
	Bloom1MetadataBuilder *builder = (Bloom1MetadataBuilder *) builder_;

	/*
	compressor->compressed_is_null[builder->bloom_attr_offset] = true;
	compressor->compressed_values[builder->bloom_attr_offset] = 0;
	/*/
	compressor->compressed_is_null[builder->bloom_attr_offset] = !builder->empty;
	compressor->compressed_values[builder->bloom_attr_offset] =
		PointerGetDatum(builder->bloom_bytea);

	fprintf(stderr,
			"set to attoffset %d, varsize %d\n",
			builder->bloom_attr_offset,
			VARSIZE(compressor->compressed_values[builder->bloom_attr_offset]));
	//*/
}

static void
bloom1_reset(void *builder_, RowCompressor *compressor)
{
	Bloom1MetadataBuilder *builder = (Bloom1MetadataBuilder *) builder_;

	builder->empty = true;
	builder->has_null = false;

	builder->nbits_set = 0;
	memset(VARDATA(builder->bloom_bytea), 0, VARSIZE_ANY_EXHDR(builder->bloom_bytea));

	compressor->compressed_is_null[builder->bloom_attr_offset] = true;
	compressor->compressed_values[builder->bloom_attr_offset] = 0;
}
