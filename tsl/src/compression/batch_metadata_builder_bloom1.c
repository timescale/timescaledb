/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include "postgres.h"

#include <access/detoast.h>
#include <catalog/pg_collation_d.h>
#include <common/hashfn.h>
#include <funcapi.h>
#include <utils/builtins.h>
#include <utils/typcache.h>

#include <math.h>

#include "compression.h"

#include "batch_metadata_builder.h"

#include "arrow_c_data_interface.h"

#include "sparse_index_bloom1.h"

#ifdef TS_USE_UMASH
#include "import/umash.h"
#endif

/*
 * Our filters go down to 64 bits and we want to have 0.1% false positives, hence
 * this value.
 */
#define BLOOM1_HASHES 8

/*
 * Limit the bits belonging to the particular elements to a small contiguous
 * region. This improves memory locality when building the bloom filter.
 */
#define BLOOM1_BLOCK_BITS 256

typedef struct Bloom1MetadataBuilder
{
	BatchMetadataBuilder functions;

	int16 bloom_attr_offset;

	int allocated_bytea_bytes;
	bytea *bloom_bytea;

	PGFunction hash_function;
} Bloom1MetadataBuilder;

/*
 * Low-bias invertible hash function from this article:
 * http://web.archive.org/web/20250406022607/https://nullprogram.com/blog/2018/07/31/
 */
static inline uint64
bloom1_hash64(uint64 x)
{
	x ^= x >> 32;
	x *= 0xd6e8feb86659fd93U;
	x ^= x >> 32;
	x *= 0xd6e8feb86659fd93U;
	x ^= x >> 32;
	return x;
}

static Datum
bloom1_hash_2(PG_FUNCTION_ARGS)
{
	PG_RETURN_UINT64(bloom1_hash64(PG_GETARG_UINT16(0)));
}

static Datum
bloom1_hash_4(PG_FUNCTION_ARGS)
{
	PG_RETURN_UINT64(bloom1_hash64(PG_GETARG_UINT32(0)));
}

static Datum
bloom1_hash_8(PG_FUNCTION_ARGS)
{
	PG_RETURN_UINT64(bloom1_hash64(PG_GETARG_INT64(0)));
}

#ifdef TS_USE_UMASH
static struct umash_params *
hashing_params()
{
	static struct umash_params params = { 0 };
	if (params.poly[0][0] == 0)
	{
		umash_params_derive(&params, 0x12345abcdef67890ULL, NULL);
		Assert(params.poly[0][0] != 0);
	}

	return &params;
}

static Datum
bloom1_hash_varlena(PG_FUNCTION_ARGS)
{
	Datum datum = PG_GETARG_DATUM(0);
	const int length = VARSIZE_ANY_EXHDR(datum);
	const char *data = VARDATA_ANY(datum);
	PG_RETURN_UINT64(umash_full(hashing_params(),
								/* seed = */ ~0ULL,
								/* which = */ 0,
								data,
								length));
}

static Datum
bloom1_hash_16(PG_FUNCTION_ARGS)
{
	Datum datum = PG_GETARG_DATUM(0);
	PG_RETURN_UINT64(umash_full(hashing_params(),
								/* seed = */ ~0ULL,
								/* which = */ 0,
								DatumGetPointer(datum),
								16));
}
#endif

/*
 * Get the hash function we use for building a bloom filter for a particular
 * type. Returns NULL if not supported.
 * The signature of the returned function matches the Postgres extended hashing
 * functions like hashtextextended().
 */
PGFunction
bloom1_get_hash_function(Oid type)
{
	/*
	 * FIXME
	 * Fall back to the Postgres extended hashing functions, so that we can use
	 * bloom filters for any types.
	 */
	TypeCacheEntry *entry = lookup_type_cache(type, TYPECACHE_HASH_EXTENDED_PROC_FINFO);
	return entry->hash_extended_proc_finfo.fn_addr;

#ifdef TS_USE_UMASH
	if (type == TEXTOID)
	{
		return bloom1_hash_varlena;
	}
#endif

	int16 typlen;
	bool typbyval;
	get_typlenbyval(type, &typlen, &typbyval);

	switch (typlen)
	{
		case 2:
			return bloom1_hash_2;
		case 4:
			return bloom1_hash_4;
		case 8:
			return bloom1_hash_8;
#ifdef TS_USE_UMASH
		case 16:
			/* For UUID. */
			return bloom1_hash_16;
#endif
	}
}

static void
bloom1_update_null(void *builder_)
{
	/*
	 * A null value cannot match an equality condition that we're optimizing
	 * with bloom filters, so we don't need to consider them here.
	 */
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

static char *
bloom1_words(bytea *bloom)
{
	return VARDATA_ANY(bloom);
}

static int
bloom1_num_bits(const bytea *bloom)
{
	return 8 * VARSIZE_ANY_EXHDR(bloom);
}

static int
bloom1_estimate_ndistinct(bytea *bloom)
{
	const double m = bloom1_num_bits(bloom);
	const double t = pg_popcount(bloom1_words(bloom), m / 8);
	const double k = BLOOM1_HASHES;
	return log(1 - t / m) / (k * log(1 - 1 / m));
}

static void
bloom1_insert_to_compressed_row(void *builder_, RowCompressor *compressor)
{
	Bloom1MetadataBuilder *builder = (Bloom1MetadataBuilder *) builder_;
	bytea *bloom = builder->bloom_bytea;
	char *restrict words_buf = bloom1_words(bloom);

	const int orig_num_bits = bloom1_num_bits(bloom);
	Assert(orig_num_bits % 8 == 0);
	Assert(orig_num_bits % 64 == 0);
	const int orig_bits_set = pg_popcount(words_buf, orig_num_bits / 8);

	if (unlikely(orig_num_bits == 0 || orig_num_bits == orig_bits_set))
	{
		/*
		 * 1) All elements turned out to be null, don't save the empty filter in
		 * that case.
		 * 2) All bits are set, this filter is useless. Shouldn't really happen,
		 * but technically possible, and the following calculations will
		 * segfault in this case.
		 */
		compressor->compressed_is_null[builder->bloom_attr_offset] = true;
		compressor->compressed_values[builder->bloom_attr_offset] = PointerGetDatum(NULL);
	}

	/*
	 * Our filters are sized for the maximum expected number of the unique
	 * elements, so in practice they can be very sparse if the actual number of
	 * the unique elements is less. The TOAST compression doesn't handle even
	 * the sparse filters very well. Apply a simple compression technique: split
	 * the filter in half and bitwise OR the halves. Repeat this until we reach
	 * the filter bit length that gives the desired false positive ratio of 0.1%.
	 * The desired filter bit length is given by m1, we will now estimate it
	 * based on the estimated current number of elements in the bloom filter (1)
	 * and the ideal number of elements for a bloom filter of given size (2).
	 * (1) n = log(1 - t/m0) / (k * log(1 - 1/m0)),
	 * (2) n = -m1 * log(1 - p ^ (1/k)) / k.
	 */
	const double m0 = orig_num_bits;
	const double k = BLOOM1_HASHES;
	const double p = 0.001;
	const double t = orig_bits_set;
	const double m1 = -log(1 - t / m0) / (log(1 - 1 / m0) * log(1 - pow(p, 1 / k)));

	/*
	 * Compute powers of two corresponding to the current and desired filter
	 * bit length.
	 */
	const int starting_pow2 = ceil(log2(m0));
	Assert(pow(2, starting_pow2) == m0);
	/* We don't want to go under 64 bytes. */
	const int final_pow2 = MAX(6, ceil(log2(m1)));
	Assert(final_pow2 >= 6);

	/*
	 * Fold filter in half, applying bitwise OR, until we reach the desired
	 * filter bit length.
	 */
	for (int current_pow2 = starting_pow2; current_pow2 > final_pow2; current_pow2--)
	{
		const int half_words = 1 << (current_pow2 - 3 /* 8-bit byte */ - 1 /* half */);
		Assert(half_words > 0);
		const char *words_tail = &words_buf[half_words];
		for (int i = 0; i < half_words; i++)
		{
			words_buf[i] |= words_tail[i];
		}
	}

	/*
	 * If we have resized the filter, update the nominal size of the varlena
	 * object.
	 */
	if (final_pow2 < starting_pow2)
	{
		SET_VARSIZE(bloom,
					(char *) bloom1_words(bloom) + (1 << (final_pow2 - 3 /* 8-bit byte */)) -
						(char *) bloom);
	}

	Assert(bloom1_num_bits(bloom) % (sizeof(*words_buf) * 8) == 0);
	Assert(bloom1_num_bits(bloom) % 64 == 0);

	compressor->compressed_is_null[builder->bloom_attr_offset] = false;
	compressor->compressed_values[builder->bloom_attr_offset] = PointerGetDatum(bloom);
}

/*
 * Call a hash function that uses a postgres "extended hash" signature.
 */
static inline uint64
calculate_hash(PGFunction hash_function, Datum needle)
{
	LOCAL_FCINFO(hashfcinfo, 2);
	*hashfcinfo = (FunctionCallInfoBaseData){ 0 };

	/*
	 * Our hashing is not collation-sensitive, but the Postgres hashing functions
	 * might refuse to work if the collation is not deterministic, so make them
	 * happy.
	 */
	hashfcinfo->fncollation = C_COLLATION_OID;

	hashfcinfo->nargs = 2;
	hashfcinfo->args[0].value = needle;
	hashfcinfo->args[0].isnull = false;
	/*
	 * Seed. Note that on 32-bit systems it is by-reference.
	 */
	const int64 seed = 0;
	hashfcinfo->args[1].value = Int64GetDatumFast(seed);
	hashfcinfo->args[1].isnull = false;

	return DatumGetUInt64(hash_function(hashfcinfo));
}

/*
 * The offset of nth bit we're going to set.
 */
static inline uint32
bloom1_get_one_offset(uint64 value_hash, uint32 index)
{
	const uint32 low = value_hash & ~(uint32) 0;
	const uint32 high = (value_hash >> 32) & ~(uint32) 0;

	/*
	 * Add a quadratic component to lessen degradation in the unlikely case when
	 * 'high' is a multiple of block bits.
	 */
	return low + (index * high + index * index) % BLOOM1_BLOCK_BITS;
}

static void
bloom1_update_val(void *builder_, Datum needle)
{
	Bloom1MetadataBuilder *builder = (Bloom1MetadataBuilder *) builder_;

	char *restrict words_buf = bloom1_words(builder->bloom_bytea);
	const uint32 num_bits = bloom1_num_bits(builder->bloom_bytea);

	/*
	 * These calculations are a little inconvenient, but I had to switch to
	 * another buffer word size already, so for now I'm keeping the code generic
	 * relative to this size.
	 */
	const uint32 num_word_bits = sizeof(*words_buf) * 8;
	Assert(num_bits % num_word_bits == 0);
	const uint32 log2_word_bits = pg_leftmost_one_pos32(num_word_bits);
	Assert(num_word_bits == (1ULL << log2_word_bits));

	const uint32 word_mask = num_word_bits - 1;
	Assert((word_mask >> num_word_bits) == 0);

	const uint64 datum_hash_1 = calculate_hash(builder->hash_function, needle);
	const uint32 absolute_mask = num_bits - 1;
	for (int i = 0; i < BLOOM1_HASHES; i++)
	{
		const uint32 absolute_bit_index = bloom1_get_one_offset(datum_hash_1, i) & absolute_mask;
		const uint32 word_index = absolute_bit_index >> log2_word_bits;
		const uint32 word_bit_index = absolute_bit_index & word_mask;
		words_buf[word_index] |= 1ULL << word_bit_index;
	}
}

/*
 * Checks whether the given element can be present in the given bloom filter.
 * This is what we use in predicate pushdown. The SQL signature is:
 * _timescaledb_functions.bloom1_contains(bytea, anyelement)
 */
Datum
bloom1_contains(PG_FUNCTION_ARGS)
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

	Oid type_oid = get_fn_expr_argtype(fcinfo->flinfo, 1);
	TypeCacheEntry *type_entry = lookup_type_cache(type_oid, TYPECACHE_HASH_EXTENDED_PROC);
	Ensure(OidIsValid(type_entry->hash_extended_proc), "cannot find the hash function");
	Datum needle = PG_GETARG_DATUM(1);
	if (type_entry->typlen == -1)
	{
		/*
		 * Detoast the varlena type once to not accidentally detoast it in
		 * several places.
		 */
		needle = PointerGetDatum(PG_DETOAST_DATUM_PACKED(needle));
	}

	PGFunction fn = bloom1_get_hash_function(type_oid);

	bytea *bloom = PG_GETARG_VARLENA_P(0);
	const char *words_buf = bloom1_words(bloom);
	const uint32 num_bits = bloom1_num_bits(bloom);

	/* Must be a power of two. */
	CheckCompressedData(num_bits == (1ULL << pg_leftmost_one_pos32(num_bits)));

	/* Must be >= 64 bits. */
	CheckCompressedData(num_bits >= 64);

	const uint32 num_word_bits = sizeof(*words_buf) * 8;
	Assert(num_bits % num_word_bits == 0);
	const uint32 log2_word_bits = pg_leftmost_one_pos32(num_word_bits);
	Assert(num_word_bits == (1ULL << log2_word_bits));

	const uint32 word_mask = num_word_bits - 1;
	Assert((word_mask >> num_word_bits) == 0);

	const uint64 datum_hash_1 = calculate_hash(fn, needle);
	const uint32 absolute_mask = num_bits - 1;
	for (int i = 0; i < BLOOM1_HASHES; i++)
	{
		const uint32 absolute_bit_index = bloom1_get_one_offset(datum_hash_1, i) & absolute_mask;
		const uint32 word_index = absolute_bit_index >> log2_word_bits;
		const uint32 word_bit_index = absolute_bit_index & word_mask;
		if ((words_buf[word_index] & (1ULL << word_bit_index)) == 0)
		{
			PG_RETURN_BOOL(false);
		}
	}
	PG_RETURN_BOOL(true);
}

static int
bloom1_bytea_alloc_size(int num_bits)
{
	Assert(num_bits % 8 == 0);
	Assert(num_bits % 64 == 0);
	return VARHDRSZ + num_bits / 8;
}

BatchMetadataBuilder *
batch_metadata_builder_bloom1_create(Oid type_oid, int bloom_attr_offset)
{
	/*
	 * Better make the bloom filter size a power of two, because we compress the
	 * sparse filters using division in half. The formula for the lowest
	 * enclosing power of two is pow(2, floor(log2(x * 2 - 1))).
	 */
	const int expected_elements = TARGET_COMPRESSED_BATCH_SIZE * 16;
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

	builder->hash_function = bloom1_get_hash_function(type_oid);

	/*
	 * Initialize the bloom filter.
	 */
	builder->bloom_bytea = palloc0(bytea_bytes);
	SET_VARSIZE(builder->bloom_bytea, bytea_bytes);

	return &builder->functions;
}

TS_FUNCTION_INFO_V1(ts_bloom1_debug);

/*
 * We're slightly modifying this Postgres macro to avoid a warning about signed
 * vs unsigned comparison.
 */
#define TS_VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer)                                            \
	((int) VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer) <                                            \
	 (int) ((toast_pointer).va_rawsize - VARHDRSZ))

/*
 * A function to output various debugging info about a bloom filter.
 */
Datum
ts_bloom1_debug(PG_FUNCTION_ARGS)
{
	/* Build a tuple descriptor for our result type */
	TupleDesc tuple_desc;
	if (get_call_result_type(fcinfo, NULL, &tuple_desc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	/* Output columns of this function. */
	enum
	{
		out_toast_header = 0,
		out_toasted_bytes,
		out_compressed_bytes,
		out_detoasted_bytes,
		out_bits_total,
		out_bits_set,
		out_estimated_elements,
		_out_columns
	};
	Datum values[_out_columns] = { 0 };
	bool nulls[_out_columns] = { 0 };

	Datum toasted = PG_GETARG_DATUM(0);
	values[out_toast_header] = Int32GetDatum(((varattrib_1b *) toasted)->va_header);
	values[out_toasted_bytes] = Int32GetDatum(VARSIZE_ANY_EXHDR(toasted));

	bytea *detoasted = PG_DETOAST_DATUM(toasted);
	values[out_detoasted_bytes] = Int32GetDatum(VARSIZE_ANY_EXHDR(detoasted));

	const int bits_total = bloom1_num_bits(detoasted);
	values[out_bits_total] = Int32GetDatum(bits_total);

	const char *words = bloom1_words(detoasted);

	values[out_bits_set] = Int32GetDatum(pg_popcount(words, bits_total / 8));

	values[out_estimated_elements] = Int32GetDatum(bloom1_estimate_ndistinct(detoasted));

	if (VARATT_IS_EXTERNAL_ONDISK(toasted))
	{
		struct varatt_external toast_pointer;
		VARATT_EXTERNAL_GET_POINTER(toast_pointer, toasted);

		if (TS_VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer))
		{
			values[out_compressed_bytes] = VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer);
		}
		else
		{
			nulls[out_compressed_bytes] = true;
		}
	}
	else if (VARATT_IS_COMPRESSED(toasted))
	{
		values[out_compressed_bytes] = VARDATA_COMPRESSED_GET_EXTSIZE(toasted);
	}
	else
	{
		nulls[out_compressed_bytes] = true;
	}

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tuple_desc, values, nulls)));
}

TS_FUNCTION_INFO_V1(ts_bloom1_hash);

Datum
ts_bloom1_hash(PG_FUNCTION_ARGS)
{
	Oid type_oid = get_fn_expr_argtype(fcinfo->flinfo, 0);
	TypeCacheEntry *type_entry = lookup_type_cache(type_oid, TYPECACHE_HASH_EXTENDED_PROC);
	Ensure(OidIsValid(type_entry->hash_extended_proc), "cannot find postgres hash function");
	Assert(!PG_ARGISNULL(0));
	Datum needle = PG_GETARG_DATUM(0);
	if (type_entry->typlen == -1)
	{
		needle = PointerGetDatum(PG_DETOAST_DATUM_PACKED(needle));
	}

	PGFunction fn = bloom1_get_hash_function(type_oid);
	Ensure(fn != NULL, "cannot find our hash function");

	PG_RETURN_UINT64(calculate_hash(fn, needle));
}
