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

	int allocated_bytea_bytes;
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
	Assert(VARATT_IS_4B_U(bloom));

	uint64 *ptr = (uint64 *) TYPEALIGN(sizeof(ptr), VARDATA(bloom));
	Assert((void *) ptr > (void *) bloom);
	return ptr;
}

static int
bloom1_num_bits(const bytea *bloom)
{
	Assert(VARATT_IS_4B_U(bloom));

	const uint64 *words = bloom1_words((bytea *) bloom);
	const uint64 bytes = (char *) bloom + VARSIZE(bloom) - (char *) words;
	Assert(bytes > 0);
	return 8 * bytes;
}

static void
bloom1_reset(void *builder_, RowCompressor *compressor)
{
	Bloom1MetadataBuilder *builder = (Bloom1MetadataBuilder *) builder_;

	bytea *bloom = builder->bloom_bytea;
	memset(bloom, 0, builder->allocated_bytea_bytes);
	SET_VARSIZE(bloom, builder->allocated_bytea_bytes);

	compressor->compressed_is_null[builder->bloom_attr_offset] = true;
	compressor->compressed_values[builder->bloom_attr_offset] = 0;
}

static int
pg_attribute_unused() bloom1_estimate_ndistinct(bytea *bloom)
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
	bytea *bloom = builder->bloom_bytea;
	uint64 *restrict words_buf = bloom1_words(bloom);

	const int orig_num_bits = bloom1_num_bits(bloom);
	const int orig_bits_set = arrow_num_valid(words_buf, orig_num_bits);

	/*
	 * Our filters are sized for the maximum expected number of the unique
	 * elements, so in practice they can be very sparse if the actual number of
	 * the unique elements is less. The TOAST compression doesn't handle even
	 * the sparse filters very well. Apply a simple compression technique: split
	 * the filter in half and bitwise OR the halves. Repeat this until we reach
	 * the filter size that gives the desired false positive ratio of 1%.
	 */
	const double m0 = bloom1_num_bits(bloom);
	//	const double n = bloom1_estimate_ndistinct(bloom);
	const double k = BLOOM1_HASHES;
	const double p = 0.01;
	const double t = arrow_num_valid(bloom1_words(bloom), m0);
	const double m1 = -log(1 - t / m0) / (log(1 - 1 / m0) * log(1 - pow(p, 1 / k)));
	const int starting_pow2 = ceil(log2(m0));
	Assert(pow(2, starting_pow2) == m0);
	/* We don't want to go under 64 bytes. */
	const int final_pow2 = MAX(6, ceil(log2(m1)));

	for (int current_pow2 = starting_pow2; current_pow2 > final_pow2; current_pow2--)
	{
		const int half_words = 1 << (current_pow2 - 6 /* 64-bit word */ - 1 /* half */);
		Assert(half_words > 0);
		const uint64 *words_tail = &words_buf[half_words];
		for (int i = 0; i < half_words; i++)
		{
			words_buf[i] |= words_tail[i];
		}
	}

	if (final_pow2 < starting_pow2)
	{
		SET_VARSIZE(bloom,
					(char *) bloom1_words(bloom) + (1 << (final_pow2 - 3 /* 8-bit byte */)) -
						(char *) bloom);
	}

	Assert(bloom1_num_bits(bloom) % (sizeof(*words_buf) * 8) == 0);
	if (unlikely(arrow_num_valid(bloom1_words(bloom), bloom1_num_bits(bloom))) == 0)
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
		compressor->compressed_is_null[builder->bloom_attr_offset] = false;
		compressor->compressed_values[builder->bloom_attr_offset] = PointerGetDatum(bloom);
	}

	fprintf(stderr,
			"bloom filter %d -> %d (%d) bits %d -> %d set %d estimate\n",
			orig_num_bits,
			bloom1_num_bits(bloom),
			(int) pow(2, ceil(log2(m1))),
			orig_bits_set,
			arrow_num_valid(words_buf, bloom1_num_bits(bloom)),
			bloom1_estimate_ndistinct(bloom));
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

	const int num_bits = bloom1_num_bits(builder->bloom_bytea);
	uint64 *restrict words_buf = bloom1_words(builder->bloom_bytea);
	const int num_word_bits = sizeof(*words_buf) * 8;
	Assert(num_bits % num_word_bits == 0);
	for (int i = 0; i < BLOOM1_HASHES; i++)
	{
		// const uint32 h = bloom1_get_one_hash(datum_hash_1, i) % nbits;
		// const uint32 h = (datum_hash_1 + i * datum_hash_2) % nbits;
		const uint32 h = bloom1_get_one_hash(datum_hash_1, i) % num_bits;
		const uint32 word_index = h / num_word_bits;
		const uint32 bit = h % num_word_bits;
		words_buf[word_index] |= 1ULL << bit;
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

	bytea *bloom = PG_GETARG_VARLENA_P(0);
	const int num_bits = bloom1_num_bits(bloom);
	const uint64 *words_buf = bloom1_words(bloom);
	const int num_word_bits = sizeof(*words_buf) * 8;
	Assert(num_bits % num_word_bits == 0);
	/*
	 * FIXME check compressed data that it's a power of two. Use mask instead
	 * of division.
	 */
	bool match = true;
	for (int i = 0; i < BLOOM1_HASHES; i++)
	{
		const uint32 h = bloom1_get_one_hash(datum_hash, i) % num_bits;
		const uint32 word_index = h / num_word_bits;
		const uint32 bit = h % num_word_bits;
		match = (words_buf[word_index] & (1ULL << bit)) && match;
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
	/*
	 * Better make the bloom filter size a power of two, because we pack the
	 * sparse filters using division in half.
	 * The calculation for the lowest enclosing power of two is
	 * pow(2, floor(log2(x * 2 - 1))).
	 */
	const int expected_elements = TARGET_COMPRESSED_BATCH_SIZE * 8;
	const int lowest_power = pg_leftmost_one_pos32(expected_elements * 2 - 1);
	Assert(lowest_power <= 16);
	const int desired_bits = 1ULL << lowest_power;
	const int bytea_bytes = bloom1_bytea_alloc_size(desired_bits);

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
		.allocated_bytea_bytes = bytea_bytes,
	};

	TypeCacheEntry *type = lookup_type_cache(type_oid, TYPECACHE_HASH_EXTENDED_PROC);
	Ensure(OidIsValid(type->hash_extended_proc), "cannot find the hash function");
	fmgr_info(type->hash_extended_proc, &builder->hash_function);

	/*
	 * Initialize the bloom filter.
	 */
	builder->bloom_bytea = palloc0(bytea_bytes);
	SET_VARSIZE(builder->bloom_bytea, bytea_bytes);

	return &builder->functions;
}
