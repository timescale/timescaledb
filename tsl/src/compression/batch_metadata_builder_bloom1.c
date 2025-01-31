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

#include "sparse_index_bloom1.h"

#define BLOOM1_HASHES 4

typedef struct Bloom1MetadataBuilder
{
	BatchMetadataBuilder functions;

	int16 bloom_attr_offset;

	bytea *bloom_bytea;

	FmgrInfo hash_function;
} Bloom1MetadataBuilder;

static void
bloom1_update_null(void *builder_)
{
	/*
	 * A null value cannot match an equality condition that we're optimizing
	 * with bloom filters, so we don't need to consider them here.
	 */
}

static uint64 *
bloom1_words(bytea *bloom)
{
	uint64 *ptr = (uint64 *) TYPEALIGN(sizeof(ptr), VARDATA(bloom));
	return ptr;
}

static int
bloom1_num_bits(const bytea *bloom)
{
	const uint64 *words = bloom1_words((bytea *) bloom);
	return 8 * (VARSIZE_ANY(bloom) + (char *) bloom - (char *) words);
}

static void
bloom1_reset(void *builder_, RowCompressor *compressor)
{
	Bloom1MetadataBuilder *builder = (Bloom1MetadataBuilder *) builder_;

	memset(bloom1_words(builder->bloom_bytea), 0, bloom1_num_bits(builder->bloom_bytea) / 8);

	compressor->compressed_is_null[builder->bloom_attr_offset] = true;
	compressor->compressed_values[builder->bloom_attr_offset] = 0;
}

PG_USED_FOR_ASSERTS_ONLY static int
bloom1_estimate_ndistinct(bytea *bloom)
{
	const double m = bloom1_num_bits(bloom);
	const double t = arrow_num_valid(bloom1_words(bloom), m);
	const double k = BLOOM1_HASHES;
	return log(1 - t / m) / (k * log(1 - 1 / m));
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
		compressor->compressed_values[builder->bloom_attr_offset] = PointerGetDatum(NULL);
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
			bloom1_num_bits(builder->bloom_bytea),
			bits_set,
			bloom1_estimate_ndistinct(builder->bloom_bytea));
}

static inline uint32
bloom1_get_one_hash(uint64 value_hash, uint32 index)
{
	const uint32 low = value_hash & ~(uint32) 0;
	const uint32 high = (value_hash >> 32) & ~(uint32) 0;
	return low + index * high;
}

static void
bloom1_update_val(void *builder_, Datum val)
{
	Bloom1MetadataBuilder *builder = (Bloom1MetadataBuilder *) builder_;

	// const uint64 datum_hash_1 = builder->hash_function(val);
	const uint64 datum_hash_1 =
		DatumGetUInt64(FunctionCall2Coll(&builder->hash_function, C_COLLATION_OID, val, ~0ULL));
	//	const uint64 datum_hash_2 =
	//		DatumGetUInt64(FunctionCall2Coll(&builder->hash_function, C_COLLATION_OID, val,
	//		BLOOM1_SEED_2));

	const int nbits = bloom1_num_bits(builder->bloom_bytea);
	uint64 *restrict words = bloom1_words(builder->bloom_bytea);
	const int word_bits = sizeof(*words) * 8;
	Assert(nbits % word_bits == 0);
	for (int i = 0; i < BLOOM1_HASHES; i++)
	{
		// const uint32 h = bloom1_get_one_hash(datum_hash_1, i) % nbits;
		// const uint32 h = (datum_hash_1 + i * datum_hash_2) % nbits;
		const uint32 h = bloom1_get_one_hash(datum_hash_1, i) % nbits;
		const uint32 byte = (h / word_bits);
		const uint32 bit = (h % word_bits);
		words[byte] |= (0x01 << bit);
	}
}

TS_FUNCTION_INFO_V1(tsl_bloom1_matches);

/* _timescaledb_functions.ts_bloom1_matches(bytea, anyelement) */
Datum
tsl_bloom1_matches(PG_FUNCTION_ARGS)
{
	/*
	 * This function is not strict, because if we don't have a bloom filter, this
	 * means the condition can potentially be true.
	 */
	if (PG_ARGISNULL(0))
	{
		PG_RETURN_BOOL(true);
	}

	/*
	 * A null value cannot match the equality condition, although this probably
	 * should be optimized away by the planner.
	 */
	if (PG_ARGISNULL(1))
	{
		PG_RETURN_BOOL(false);
	}

	Oid val_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	TypeCacheEntry *type = lookup_type_cache(val_type, TYPECACHE_HASH_EXTENDED_PROC);
	Ensure(OidIsValid(type->hash_extended_proc), "cannot find the hash function");
	const uint64 datum_hash = DatumGetUInt64(
		OidFunctionCall2Coll(type->hash_extended_proc, C_COLLATION_OID, PG_GETARG_DATUM(1), ~0ULL));

	bytea *bloom = PG_GETARG_VARLENA_PP(0);
	const int nbits = bloom1_num_bits(bloom);
	const uint64 *words = bloom1_words(bloom);
	const int word_bits = sizeof(*words) * 8;
	Assert(nbits % word_bits == 0);
	bool match = true;
	for (int i = 0; i < BLOOM1_HASHES; i++)
	{
		const uint32 h = bloom1_get_one_hash(datum_hash, i) % nbits;
		const uint32 word_index = (h / word_bits);
		const uint32 bit = (h % word_bits);
		match = (words[word_index] & (0x01 << bit)) && match;
	}

	PG_RETURN_BOOL(match);
}

static int
bloom1_bytea_alloc_size(int num_bits)
{
	const int words = (num_bits + 63) / 64;
	const int header = TYPEALIGN(8, VARHDRSZ);
	return header + words * 8;
}

BatchMetadataBuilder *
batch_metadata_builder_bloom1_create(Oid type_oid, int bloom_attr_offset)
{
	Bloom1MetadataBuilder *builder = palloc(sizeof(*builder));

	*builder = (Bloom1MetadataBuilder){
		.functions =
			(BatchMetadataBuilder){
				.update_val = bloom1_update_val,
				.update_null = bloom1_update_null,
				.insert_to_compressed_row = bloom1_insert_to_compressed_row,
				.reset = bloom1_reset,
			},
		.bloom_attr_offset = bloom_attr_offset,
	};

	TypeCacheEntry *type = lookup_type_cache(type_oid, TYPECACHE_HASH_EXTENDED_PROC);
	Ensure(OidIsValid(type->hash_extended_proc), "cannot find the hash function");
	fmgr_info(type->hash_extended_proc, &builder->hash_function);

	/*
	 * Initialize the bloom filter.
	 */
	const int desired_bits = TARGET_COMPRESSED_BATCH_SIZE * 8;
	const int bytea_size = bloom1_bytea_alloc_size(desired_bits);
	builder->bloom_bytea = palloc0(bytea_size);
	SET_VARSIZE(builder->bloom_bytea, bytea_size);

	return &builder->functions;
}
