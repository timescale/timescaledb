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

#include "arrow_c_data_interface.h"

#include "utils/bloom1_sparse_index_params.h"

typedef struct Bloom1MetadataBuilder
{
	BatchMetadataBuilder functions;

	Oid type_oid;
	bool empty;

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
		.type_by_val = type->typbyval,
		.type_len = type->typlen,
		.bloom_attr_offset = bloom_attr_offset,
		.hash_proc_oid = type->hash_proc,

		.nbits = 1024 * 8,

		.nbits_set = 0,
	};

	Assert(builder->nbits % 64 == 0);
	const int bytea_size = bloom1_bytea_alloc_size(builder->nbits);
	builder->bloom_bytea = palloc0(bytea_size);
	SET_VARSIZE(builder->bloom_bytea, bytea_size);

	return &builder->functions;
}

void
bloom1_update_val(void *builder_, Datum val)
{
	Bloom1MetadataBuilder *builder = (Bloom1MetadataBuilder *) builder_;

	const Oid hash_proc_oid = builder->hash_proc_oid;

	/* compute the hashes, used for the bloom filter */
	const uint32 datum_hash =
		DatumGetUInt32(OidFunctionCall1Coll(hash_proc_oid, C_COLLATION_OID, val));

	/* compute the requested number of hashes */
	const int nbits = bloom1_num_bits(builder->bloom_bytea);
	uint64 *restrict words = bloom1_words(builder->bloom_bytea);
	const int word_bits = sizeof(*words) * 8;
	for (int i = 0; i < BLOOM1_HASHES; i++)
	{
		const uint32 h = bloom1_get_one_hash(datum_hash, i) % nbits;
		const uint32 byte = (h / word_bits);
		const uint32 bit = (h % word_bits);
		words[byte] |= (0x01 << bit);
	}
}

void
bloom1_update_null(void *builder_)
{
	/*
	 * A null value cannot match an equality condition that we're optimizing
	 * with bloom filters, so we don't need to consider them here.
	 */
}

PG_USED_FOR_ASSERTS_ONLY static int
bloom1_estimate_ndistinct(bytea *bloom)
{
	const int nbits = bloom1_num_bits(bloom);
	const uint64 *words = bloom1_words(bloom);
	const int nset = arrow_num_valid(words, nbits);
	return -(nbits / BLOOM1_HASHES) * log(1 - nset / (double) nbits);
}

static void
bloom1_insert_to_compressed_row(void *builder_, RowCompressor *compressor)
{
	Bloom1MetadataBuilder *builder = (Bloom1MetadataBuilder *) builder_;

	const int bits_set =
		arrow_num_valid(bloom1_words(builder->bloom_bytea), bloom1_num_bits(builder->bloom_bytea));

	if (bits_set == 0)
	{
		/*
		 * All elements turned out to be null, don't save the empty filter in
		 * that case.
		 */
		compressor->compressed_is_null[builder->bloom_attr_offset] = true;
		compressor->compressed_values[builder->bloom_attr_offset] = NULL;
	}
	else
	{
		/*
		 * There is a simple compression technique for filters that turn out
		 * very sparse: you split the filter in half and bitwise OR the halves.
		 * Repeat this until you reach the occupancy that gives the desired
		 * false positive ratio, e.g. our case with 4 hashes the 1/3 occupancy
		 * would give 1% false positives. We don't apply it at the moment, the
		 * TOAST compression should help somewhat for sparse filters.
		 */
		compressor->compressed_is_null[builder->bloom_attr_offset] = false;
		compressor->compressed_values[builder->bloom_attr_offset] =
			PointerGetDatum(builder->bloom_bytea);
	}

	fprintf(stderr,
			"bloom filter %d bits %d set %d estimate\n",
			builder->nbits,
			bits_set,
			bloom1_estimate_ndistinct(builder->bloom_bytea));
}

static void
bloom1_reset(void *builder_, RowCompressor *compressor)
{
	Bloom1MetadataBuilder *builder = (Bloom1MetadataBuilder *) builder_;

	builder->empty = true;

	builder->nbits_set = 0;
	memset(VARDATA(builder->bloom_bytea), 0, VARSIZE_ANY_EXHDR(builder->bloom_bytea));

	compressor->compressed_is_null[builder->bloom_attr_offset] = true;
	compressor->compressed_values[builder->bloom_attr_offset] = 0;
}
