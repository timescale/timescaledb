/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include "batch_metadata_builder.h"
#include "arrow_c_data_interface.h"
#include "city_combine.h"
#include "compression.h"
#include "postgres.h"
#include "sparse_index_bloom1.h"
#include <access/detoast.h>
#include <catalog/pg_collation_d.h>
#include <common/hashfn.h>
#include <funcapi.h>
#include <math.h>
#include <utils/builtins.h>
#include <utils/typcache.h>

#ifdef TS_USE_UMASH
#include "import/umash.h"
#endif

/*
 * There is a tradeoff between making the bloom filters selective enough but
 * not too big. Testing on some real words datasets, the optimal value seems to
 * be about 2% false positives. We also want to be able to reduce the filter to
 * up to 64 bits, so that it fits in the main table. Hence the optimal number of
 * hashes. This number actually gives a slightly different false positive rate,
 * so this is what we ultimately use.
 * Calculator: https://hur.st/bloomfilter/?p=0.02&m=64
 */

#define BLOOM1_FALSE_POSITIVES 0.022
#define BLOOM1_HASHES 6

/*
 * Limit the bits belonging to the particular elements to a small contiguous
 * region. This improves memory locality when building the bloom filter.
 */
#define BLOOM1_BLOCK_BITS 256

/* The NULL marker is chosen to be a value that doesn't cancel out with a left rotation and XOR
 * operation, so NULL positions are preserved in the composite hash. The value is coming from Golden
 * ratio constant that has no mathematical relationship with the UMASH GF(2^64) space, so it is
 * unlikely to degrade the collision resistance of the bloom filter. */
#define NULL_MARKER 0x9E3779B97F4A7C15ULL

typedef struct Bloom1HasherInternal
{
	Bloom1Hasher functions;
	PGFunction hash_functions[MAX_BLOOM_FILTER_COLUMNS];
	FmgrInfo *hash_function_finfos[MAX_BLOOM_FILTER_COLUMNS];
} Bloom1HasherInternal;

typedef struct Bloom1MetadataBuilder
{
	BatchMetadataBuilder functions;
	int16 bloom_attr_offset;
	int allocated_varlena_bytes;
	struct varlena *bloom_varlena;
	AttrNumber input_columns[MAX_BLOOM_FILTER_COLUMNS];
	Bloom1HasherInternal hasher;
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
bloom1_hash_8(PG_FUNCTION_ARGS)
{
	PG_RETURN_UINT64(bloom1_hash64(PG_GETARG_INT64(0)));
}

static Datum
bloom1_hash_4(PG_FUNCTION_ARGS)
{
	PG_RETURN_UINT64(bloom1_hash64(PG_GETARG_INT32(0)));
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
	struct varlena *needle = PG_DETOAST_DATUM_PACKED(PG_GETARG_DATUM(0));
	const int length = VARSIZE_ANY_EXHDR(needle);
	const char *data = VARDATA_ANY(needle);
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
 * It's possible, though impractical, for a hash function to be implemented in a
 * procedural language, not in C. In this case, we need the proper FmgrInfo to
 * call it. We fetch it from the type cache. For our custom functions, it is NULL.
 */
PGFunction
bloom1_get_hash_function(Oid type, FmgrInfo **finfo)
{
	*finfo = NULL;

	/*
	 * By default, we use the Postgres extended hashing functions, so that we
	 * can use bloom filters for any types.
	 * We request also the opfamily info and the equality operator, because
	 * otherwise the Postgres type cache code fails obtusely on types with
	 * improper opclasses. It picks up the btree opclass from a binary compatible
	 * type (see GetDefaultOpClass), then an equality operator from this opclass,
	 * and then refuses to return the hash functions because the hash opclass has
	 * a different equality operator. The problem is that this happens over two
	 * consecutive calls to lookup_type_cache(), so the first invocation of our
	 * function says that we have a hash, and the second says that we don't.
	 */
	TypeCacheEntry *entry = lookup_type_cache(type,
											  TYPECACHE_EQ_OPR | TYPECACHE_BTREE_OPFAMILY |
												  TYPECACHE_HASH_EXTENDED_PROC_FINFO);
	/*
	 * For some types we use our custom hash functions. We only do it for the
	 * builtin Postgres types to be on the safe side, and also simplify the
	 * testing by creating bad hash functions from SQL tests. If you change this,
	 * you might have to change the bad hash testing in compress_bloom_sparse.sql.
	 */
	switch (entry->hash_extended_proc)
	{
#ifdef TS_USE_UMASH
		case F_HASHTEXTEXTENDED:
			return bloom1_hash_varlena;

		case F_UUID_HASH_EXTENDED:
			return bloom1_hash_16;
#endif
		case F_HASHINT8EXTENDED:
			return bloom1_hash_8;

		case F_HASHINT4EXTENDED:
#if PG18_GE
		/*
		 * PG18 added a custom hashing function for date type.
		 * For backwards compatibility, we need to continue using
		 * our own custom function which was used for < PG18.
		 *
		 * https://github.com/postgres/postgres/commit/23d0b484
		 */
		case F_HASHDATEEXTENDED:
#endif
			return bloom1_hash_4;
		default:
			/*
			 * Use the Postgres hash function. We might require the finfo, for
			 * example for functions defined in procedural languages.
			 */
			*finfo = &entry->hash_extended_proc_finfo;
			return entry->hash_extended_proc_finfo.fn_addr;
	}
}

static void
bloom1_reset(void *builder_, RowCompressor *compressor)
{
	Bloom1MetadataBuilder *builder = (Bloom1MetadataBuilder *) builder_;
	Assert(builder->functions.builder_type == METADATA_BUILDER_BLOOM1);

	struct varlena *bloom = builder->bloom_varlena;
	memset(bloom, 0, builder->allocated_varlena_bytes);
	SET_VARSIZE(bloom, builder->allocated_varlena_bytes);

	compressor->compressed_is_null[builder->bloom_attr_offset] = true;
	compressor->compressed_values[builder->bloom_attr_offset] = 0;
}

static char *
bloom1_words_buf(struct varlena *bloom)
{
	return VARDATA_ANY(bloom);
}

static int
bloom1_num_bits(const struct varlena *bloom)
{
	return 8 * VARSIZE_ANY_EXHDR(bloom);
}

void
batch_metadata_builder_bloom1_insert_bloom_filter_to_compressed_row(void *bloom_varlena,
																	int16 bloom_attr_offset,
																	RowCompressor *compressor)
{
	struct varlena *bloom = (struct varlena *) bloom_varlena;
	char *restrict words_buf = bloom1_words_buf(bloom);

	const int orig_num_bits = bloom1_num_bits(bloom);
	Assert(orig_num_bits % 8 == 0);
	Assert(orig_num_bits % 64 == 0);
	const int orig_bits_set = pg_popcount(words_buf, orig_num_bits / 8);

	if (unlikely(orig_bits_set == 0 || orig_bits_set == orig_num_bits))
	{
		/*
		 * 1) All elements turned out to be null, don't save the empty filter in
		 * that case. The compressed batch will be compressed using the NULL
		 * compression algorithm, so actually checking the rows will be efficient
		 * enough.
		 *
		 * 2) All bits are set, this filter is useless. Shouldn't really happen,
		 * but technically possible, and the following calculations will
		 * segfault in this case.
		 */
		compressor->compressed_is_null[bloom_attr_offset] = true;
		compressor->compressed_values[bloom_attr_offset] = PointerGetDatum(NULL);
		return;
	}

	/*
	 * Our filters are sized for the maximum expected number of the unique
	 * elements, so in practice they can be very sparse if the actual number of
	 * the unique elements is less. The TOAST compression doesn't handle even
	 * the sparse filters very well. Apply a simple compression technique: split
	 * the filter in half and bitwise OR the halves. Repeat this until we reach
	 * the filter bit length that gives the desired false positive ratio.
	 * The desired filter bit length is given by m1, we will now estimate it
	 * based on the estimated current number of elements in the bloom filter (1)
	 * and the ideal number of elements for a bloom filter of given size (2).
	 * (1) n = log(1 - t/m0) / (k * log(1 - 1/m0)),
	 * (2) n = -m1 * log(1 - p ^ (1/k)) / k.
	 */
	const double m0 = orig_num_bits;
	const double k = BLOOM1_HASHES;
	const double p = BLOOM1_FALSE_POSITIVES;
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
					(char *) bloom1_words_buf(bloom) + (1 << (final_pow2 - 3 /* 8-bit byte */)) -
						(char *) bloom);
	}

	Assert(bloom1_num_bits(bloom) % (sizeof(*words_buf) * 8) == 0);
	Assert(bloom1_num_bits(bloom) % 64 == 0);

	compressor->compressed_is_null[bloom_attr_offset] = false;
	compressor->compressed_values[bloom_attr_offset] = PointerGetDatum(bloom);
}

static void
bloom1_insert_to_compressed_row(void *builder_, RowCompressor *compressor)
{
	Bloom1MetadataBuilder *builder = (Bloom1MetadataBuilder *) builder_;
	batch_metadata_builder_bloom1_insert_bloom_filter_to_compressed_row(builder->bloom_varlena,
																		builder->bloom_attr_offset,
																		compressor);
}

/*
 * Call a hash function that uses a postgres "extended hash" signature.
 */
uint64
batch_metadata_builder_bloom1_calculate_hash(PGFunction hash_function, FmgrInfo *finfo,
											 Datum needle)
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

	/*
	 * Needed for hash functions defined in procedural languages, not C. While
	 * unlikely, we shouldn't segfault. The finfo is cached in the type cache.
	 */
	hashfcinfo->flinfo = finfo;

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

void
batch_metadata_builder_bloom1_update_bloom_filter_with_hash(void *varlena_ptr, uint64 hash)
{
	Assert(varlena_ptr != NULL);
	struct varlena *bloom_varlena = (struct varlena *) varlena_ptr;

	char *restrict words_buf = bloom1_words_buf(bloom_varlena);
	const uint32 num_bits = bloom1_num_bits(bloom_varlena);

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

	const uint32 absolute_mask = num_bits - 1;
	for (int i = 0; i < BLOOM1_HASHES; i++)
	{
		const uint32 absolute_bit_index = bloom1_get_one_offset(hash, i) & absolute_mask;
		const uint32 word_index = absolute_bit_index >> log2_word_bits;
		const uint32 word_bit_index = absolute_bit_index & word_mask;
		words_buf[word_index] |= 1ULL << word_bit_index;
	}
}

static uint64
bloom1_hash_values(void *hasher_, const NullableDatum *values)
{
	Bloom1HasherInternal *hasher = (Bloom1HasherInternal *) hasher_;
	int num_columns = hasher->functions.num_columns;

	if (num_columns == 1)
	{
		if (values[0].isnull)
			return NULL_MARKER;
		return batch_metadata_builder_bloom1_calculate_hash(hasher->hash_functions[0],
															hasher->hash_function_finfos[0],
															values[0].value);
	}

	uint64 accumulated = 0;
	for (int i = 0; i < num_columns; i++)
	{
		if (values[i].isnull)
		{
			accumulated = city_hash_combine(accumulated, NULL_MARKER);
		}
		else
		{
			uint64 h = batch_metadata_builder_bloom1_calculate_hash(hasher->hash_functions[i],
																	hasher->hash_function_finfos[i],
																	values[i].value);
			accumulated = city_hash_combine(accumulated, h);
		}
	}

	return accumulated;
}

static void
bloom1_update_row(void *builder_, TupleTableSlot *slot)
{
	Bloom1MetadataBuilder *builder = (Bloom1MetadataBuilder *) builder_;
	Bloom1Hasher *hasher = &builder->hasher.functions;
	int num_columns = hasher->num_columns;
	NullableDatum values[MAX_BLOOM_FILTER_COLUMNS];

	for (int i = 0; i < num_columns; i++)
	{
		values[i].value = slot_getattr(slot, builder->input_columns[i], &values[i].isnull);
	}

	/* For single-column blooms, skip NULLs to match old bloom1_update_null (no-op) behavior. */
	if (num_columns == 1 && values[0].isnull)
		return;

	uint64 hash = hasher->hash_values(hasher, values);
	batch_metadata_builder_bloom1_update_bloom_filter_with_hash(builder->bloom_varlena, hash);
}

/*
 * We cache some information across function calls in this context.
 */
typedef struct Bloom1ContainsContext
{
	Oid element_type;
	int16 element_typlen;
	bool element_typbyval;
	char element_typalign;

	/* This is per-row, here for convenience. */
	struct varlena *current_row_bloom;

	Bloom1Hasher *bloom_hasher;
	int num_columns;

	/* Stability detection and hash caching */
	bool arg_is_stable;
	bool hash_is_cached;
	uint64 cached_hash;
} Bloom1ContainsContext;

/*
 * Check if a single expression node is stable (Const or PARAM_EXTERN).
 */
static bool
expr_is_stable(Node *node)
{
	if (node == NULL)
		return false;
	if (IsA(node, Const))
		return true;
	if (IsA(node, Param) && ((Param *) node)->paramkind == PARAM_EXTERN)
		return true;
	return false;
}

/*
 * Check if the argument is stable for caching.
 * For composite blooms, checks each element of the RowExpr.
 */
static bool
bloom1_arg_is_stable(FmgrInfo *flinfo, int num_columns)
{
	if (!flinfo || !flinfo->fn_expr || !IsA(flinfo->fn_expr, FuncExpr))
		return false;

	Node *arg = (Node *) lsecond(((FuncExpr *) flinfo->fn_expr)->args);

	if (num_columns == 1)
		return expr_is_stable(arg);

	/* Composite: check each element of ROW(...) */
	if (!IsA(arg, RowExpr))
		return false;

	ListCell *lc;
	foreach (lc, ((RowExpr *) arg)->args)
	{
		if (!expr_is_stable((Node *) lfirst(lc)))
			return false;
	}
	return true;
}

static Bloom1ContainsContext *
bloom1_contains_context_prepare(FunctionCallInfo fcinfo, bool use_element_type)
{
	Bloom1ContainsContext *context = (Bloom1ContainsContext *) fcinfo->flinfo->fn_extra;
	if (context == NULL)
	{
		Ensure(PG_NARGS() == 2, "bloom1_contains called with wrong number of arguments");

		context = MemoryContextAllocZero(fcinfo->flinfo->fn_mcxt, sizeof(*context));

		context->element_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
		if (use_element_type)
		{
			context->element_type = get_element_type(context->element_type);
			Ensure(OidIsValid(context->element_type),
				   "cannot determine array element type for bloom1_contains_any");
		}

		Oid type_oids[MAX_BLOOM_FILTER_COLUMNS];
		int num_columns;

		if (context->element_type == RECORDOID)
		{
			HeapTupleHeader tuple = DatumGetHeapTupleHeader(PG_GETARG_DATUM(1));
			Oid tupType = HeapTupleHeaderGetTypeId(tuple);
			int32 tupTypmod = HeapTupleHeaderGetTypMod(tuple);
			TupleDesc tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

			num_columns = tupdesc->natts;
			if (num_columns > MAX_BLOOM_FILTER_COLUMNS)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_EXCEPTION),
						 errmsg("composite bloom filter supports at most %d columns, got %d",
								MAX_BLOOM_FILTER_COLUMNS,
								num_columns)));

			for (int i = 0; i < num_columns; i++)
				type_oids[i] = TupleDescAttr(tupdesc, i)->atttypid;
			ReleaseTupleDesc(tupdesc);
		}
		else
		{
			type_oids[0] = context->element_type;
			num_columns = 1;
		}

		MemoryContext oldctx = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
		context->bloom_hasher = bloom1_hasher_create(type_oids, num_columns);
		MemoryContextSwitchTo(oldctx);
		context->num_columns = num_columns;

		get_typlenbyvalalign(context->element_type,
							 &context->element_typlen,
							 &context->element_typbyval,
							 &context->element_typalign);

		context->arg_is_stable = bloom1_arg_is_stable(fcinfo->flinfo, num_columns);
		context->hash_is_cached = false;

		fcinfo->flinfo->fn_extra = context;
	}

	if (PG_ARGISNULL(0))
	{
		context->current_row_bloom = NULL;
	}
	else
	{
		context->current_row_bloom = PG_GETARG_VARLENA_P(0);
	}

	return context;
}

static inline bool
check_bloom_bits(const char *words_buf, uint32 num_bits, uint64 hash)
{
	Assert(words_buf != NULL);

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

	const uint32 absolute_mask = num_bits - 1;
	for (int i = 0; i < BLOOM1_HASHES; i++)
	{
		const uint32 absolute_bit_index = bloom1_get_one_offset(hash, i) & absolute_mask;
		const uint32 word_index = absolute_bit_index >> log2_word_bits;
		const uint32 word_bit_index = absolute_bit_index & word_mask;
		if ((words_buf[word_index] & (1ULL << word_bit_index)) == 0)
		{
			return false;
		}
	}
	return true;
}

/*
 * Checks whether the given element can be present in the given bloom filter.
 * This is what we use in predicate pushdown. The SQL signature is:
 * _timescaledb_functions.bloom1_contains(bloom1, anyelement)
 */
Datum
bloom1_contains(PG_FUNCTION_ARGS)
{
	Bloom1ContainsContext *context =
		bloom1_contains_context_prepare(fcinfo, /* use_element_type = */ false);

	/*
	 * This function is not strict, because if we don't have a bloom filter, this
	 * means the condition can potentially be true.
	 */
	struct varlena *bloom = context->current_row_bloom;
	if (bloom == NULL)
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

	uint64 hash = 0;
	if (context->arg_is_stable && context->hash_is_cached)
	{
		hash = context->cached_hash;
	}
	else
	{
		NullableDatum values[MAX_BLOOM_FILTER_COLUMNS];

		if (context->num_columns > 1)
		{
			HeapTupleHeader tuple = DatumGetHeapTupleHeader(PG_GETARG_DATUM(1));
			for (int i = 0; i < context->num_columns; i++)
				values[i].value = GetAttributeByNum(tuple, i + 1, &values[i].isnull);
		}
		else
		{
			values[0].value = PG_GETARG_DATUM(1);
			values[0].isnull = false;
		}

		hash = context->bloom_hasher->hash_values(context->bloom_hasher, values);

		if (context->arg_is_stable)
		{
			context->cached_hash = hash;
			context->hash_is_cached = true;
		}
	}
	PG_RETURN_BOOL(batch_metadata_builder_bloom1_hash_maybe_present(PointerGetDatum(bloom), hash));
}

#define ST_SORT sort_hashes
#define ST_ELEMENT_TYPE uint64
#define ST_COMPARE(a, b) ((*(a) > *(b)) - (*(a) < *(b)))
#define ST_SCOPE static
#define ST_DEFINE
#include <lib/sort_template.h>

/*
 * Checks whether any element of the given array can be present in the given
 * bloom filter. This is used for predicate pushdown for x = any(array[...]).
 * The SQL signature is:
 * _timescaledb_functions.bloom1_contains_any(bloom1, anyarray)
 */
Datum
bloom1_contains_any(PG_FUNCTION_ARGS)
{
	Bloom1ContainsContext *context =
		bloom1_contains_context_prepare(fcinfo, /* use_element_type = */ true);

	/*
	 * This function is not strict, because if we don't have a bloom filter, this
	 * means the condition can potentially be true.
	 */
	struct varlena *bloom = context->current_row_bloom;
	if (bloom == NULL)
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

	int num_items;
	Datum *items;
	bool *nulls;
	deconstruct_array(PG_GETARG_ARRAYTYPE_P(1),
					  context->element_type,
					  context->element_typlen,
					  context->element_typbyval,
					  context->element_typalign,
					  &items,
					  &nulls,
					  &num_items);

	if (num_items == 0)
	{
		PG_RETURN_BOOL(false);
	}

	/*
	 * Calculate the per-item base hashes that will be used for computing the
	 * individual bloom filter bit offsets. We can reuse the "items" space to
	 * avoid more allocations, but have to allocate as a fallback on 32-bit
	 * systems.
	 */
#if FLOAT8PASSBYVAL
	uint64 *item_base_hashes = (uint64 *) items;
#else
	uint64 *item_base_hashes = palloc(sizeof(uint64) * num_items);
#endif

	Bloom1HasherInternal *internal = (Bloom1HasherInternal *) context->bloom_hasher;
	FmgrInfo *finfo = internal->hash_function_finfos[0];
	PGFunction hash_fn = internal->hash_functions[0];

	int valid = 0;
	for (int i = 0; i < num_items; i++)
	{
		if (nulls[i])
		{
			/*
			 * A null value cannot match the equality condition.
			 */
			continue;
		}

		item_base_hashes[valid++] =
			batch_metadata_builder_bloom1_calculate_hash(hash_fn, finfo, items[i]);
	}

	if (valid == 0)
	{
		/*
		 * No non-null elements.
		 */
		PG_RETURN_BOOL(false);
	}

	/*
	 * Sort the hashes for cache-friendly probing.
	 */
	sort_hashes(item_base_hashes, valid);

	/*
	 * Get the bloom filter parameters.
	 */
	const char *words_buf = bloom1_words_buf(bloom);
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

	const uint32 absolute_mask = num_bits - 1;

	/* Probe the bloom filter. */
	for (int item_index = 0; item_index < valid; item_index++)
	{
		const uint64 base_hash = item_base_hashes[item_index];
		bool match = true;
		for (int i = 0; i < BLOOM1_HASHES; i++)
		{
			const uint32 absolute_bit_index = bloom1_get_one_offset(base_hash, i) & absolute_mask;
			const uint32 word_index = absolute_bit_index >> log2_word_bits;
			const uint32 word_bit_index = absolute_bit_index & word_mask;
			if ((words_buf[word_index] & (1ULL << word_bit_index)) == 0)
			{
				match = false;
				break;
			}
		}
		if (match)
		{
			PG_RETURN_BOOL(true);
		}
	}

	PG_RETURN_BOOL(false);
}

/*
 * Checks whether any hashes of the given array can be present in the given
 * bloom filter. This is used for predicate pushdown where the values are
 * pre-hashed at planning time.
 *
 * The SQL signature is:
 * _timescaledb_functions.bloom1_contains_hashes(bloom1, int8[])
 */
Datum
bloom1_contains_hashes(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
		PG_RETURN_BOOL(true);
	if (PG_ARGISNULL(1))
		PG_RETURN_BOOL(false);

	struct varlena *bloom = PG_GETARG_VARLENA_P(0);

	int num_hashes;
	Datum *hash_datums;
	bool *hash_nulls;
	deconstruct_array(PG_GETARG_ARRAYTYPE_P(1),
					  INT8OID,
					  sizeof(int64),
					  true,
					  TYPALIGN_DOUBLE,
					  &hash_datums,
					  &hash_nulls,
					  &num_hashes);

	if (num_hashes == 0)
		PG_RETURN_BOOL(false);

	const char *words_buf = bloom1_words_buf(bloom);
	const uint32 num_bits = bloom1_num_bits(bloom);

	for (int i = 0; i < num_hashes; i++)
	{
		if (hash_nulls[i])
			continue;
		uint64 hash = (uint64) DatumGetInt64(hash_datums[i]);
		if (check_bloom_bits(words_buf, num_bits, hash))
			PG_RETURN_BOOL(true);
	}

	PG_RETURN_BOOL(false);
}

static int
bloom1_varlena_alloc_size(int num_bits)
{
	/*
	 * We are not supposed to go below 64 bits because we work in 64-bit words.
	 */
	Assert(num_bits % 64 == 0);
	return VARHDRSZ + num_bits / 8;
}

int
batch_metadata_builder_bloom1_varlena_size(void)
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
	return bloom1_varlena_alloc_size(desired_bits);
}

static void
bloom1_hasher_init(Bloom1HasherInternal *hasher, const Oid *type_oids, int num_columns)
{
	*hasher = (Bloom1HasherInternal){
		.functions =
			(Bloom1Hasher){
				.hash_values = bloom1_hash_values,
				.num_columns = num_columns,
			},
	};

	for (int i = 0; i < num_columns; i++)
	{
		hasher->hash_functions[i] =
			bloom1_get_hash_function(type_oids[i], &hasher->hash_function_finfos[i]);
		if (hasher->hash_functions[i] == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("the argument type %s lacks an extended hash function",
							format_type_be(type_oids[i]))));
	}
}

Bloom1Hasher *
bloom1_hasher_create(const Oid *type_oids, int num_columns)
{
	Bloom1HasherInternal *hasher = palloc(sizeof(*hasher));
	bloom1_hasher_init(hasher, type_oids, num_columns);
	return &hasher->functions;
}

BatchMetadataBuilder *
batch_metadata_builder_bloom1_create(int num_columns, const Oid *type_oids,
									 const AttrNumber *attnums, int bloom_attr_offset)
{
	Assert(num_columns >= 1 && num_columns <= MAX_BLOOM_FILTER_COLUMNS);

	const int varlena_bytes = batch_metadata_builder_bloom1_varlena_size();

	Bloom1MetadataBuilder *builder = palloc(sizeof(*builder));
	*builder = (Bloom1MetadataBuilder){
		.functions =
			(BatchMetadataBuilder){
				.update_row = bloom1_update_row,
				.insert_to_compressed_row = bloom1_insert_to_compressed_row,
				.reset = bloom1_reset,
				.builder_type = METADATA_BUILDER_BLOOM1,
			},
		.bloom_attr_offset = bloom_attr_offset,
		.allocated_varlena_bytes = varlena_bytes,
	};

	memcpy(builder->input_columns, attnums, num_columns * sizeof(AttrNumber));

	/* Initialize the embedded hasher */
	bloom1_hasher_init(&builder->hasher, type_oids, num_columns);

	/*
	 * Initialize the bloom filter.
	 */
	builder->bloom_varlena = palloc0(varlena_bytes);
	SET_VARSIZE(builder->bloom_varlena, varlena_bytes);

	return &builder->functions;
}

#ifndef NDEBUG

static int
bloom1_estimate_ndistinct(struct varlena *bloom)
{
	const double m = bloom1_num_bits(bloom);
	const double t = pg_popcount(bloom1_words_buf(bloom), m / 8);
	const double k = BLOOM1_HASHES;
	return log(1 - t / m) / (k * log(1 - 1 / m));
}

/*
 * We're slightly modifying this Postgres macro to avoid a warning about signed
 * vs unsigned comparison.
 */
#define TS_VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer)                                            \
	((int) VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer) <                                            \
	 (int) ((toast_pointer).va_rawsize - VARHDRSZ))

TS_FUNCTION_INFO_V1(ts_bloom1_debug_info);

/*
 * A function to output various debugging info about a bloom filter.
 *
 * Usage hints in the tests.
 */
Datum
ts_bloom1_debug_info(PG_FUNCTION_ARGS)
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

	struct varlena *detoasted = PG_DETOAST_DATUM(toasted);
	values[out_detoasted_bytes] = Int32GetDatum(VARSIZE_ANY_EXHDR(detoasted));

	const int bits_total = bloom1_num_bits(detoasted);
	values[out_bits_total] = Int32GetDatum(bits_total);

	const char *words = bloom1_words_buf(detoasted);

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

TS_FUNCTION_INFO_V1(ts_bloom1_debug_hash);

/*
 * A debug function to inspect the actual hash value used for the bloom filter,
 * e.g. to find the very even hashes with many low bits equal to zero.
 */
Datum
ts_bloom1_debug_hash(PG_FUNCTION_ARGS)
{
	Oid type_oid = get_fn_expr_argtype(fcinfo->flinfo, 0);
	FmgrInfo *finfo = NULL;
	PGFunction fn = bloom1_get_hash_function(type_oid, &finfo);
	Ensure(fn != NULL, "cannot find our hash function");

	Assert(!PG_ARGISNULL(0));
	Datum needle = PG_GETARG_DATUM(0);
	PG_RETURN_UINT64(batch_metadata_builder_bloom1_calculate_hash(fn, finfo, needle));
}

TS_FUNCTION_INFO_V1(ts_bloom1_composite_debug_hash);

Datum
ts_bloom1_composite_debug_hash(PG_FUNCTION_ARGS)
{
	ArrayType *type_oids_array = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType *field_values_array = PG_GETARG_ARRAYTYPE_P(1);
	ArrayType *field_nulls_array = PG_GETARG_ARRAYTYPE_P(2);

	int num_fields;
	Datum *type_oid_datums;
	bool *type_oid_nulls;
	deconstruct_array(type_oids_array,
					  OIDOID,
					  sizeof(Oid),
					  true,
					  TYPALIGN_INT,
					  &type_oid_datums,
					  &type_oid_nulls,
					  &num_fields);

	if (num_fields < 2 || num_fields > MAX_BLOOM_FILTER_COLUMNS)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("composite bloom requires 2-%d fields, got %d",
						MAX_BLOOM_FILTER_COLUMNS,
						num_fields)));

	Oid type_oids[MAX_BLOOM_FILTER_COLUMNS];
	for (int i = 0; i < num_fields; i++)
		type_oids[i] = DatumGetObjectId(type_oid_datums[i]);

	Datum *field_value_datums;
	bool *field_value_nulls;
	int num_values;
	int16 elmlen;
	bool elmbyval;
	char elmalign;

	get_typlenbyvalalign(ARR_ELEMTYPE(field_values_array), &elmlen, &elmbyval, &elmalign);
	deconstruct_array(field_values_array,
					  ARR_ELEMTYPE(field_values_array),
					  elmlen,
					  elmbyval,
					  elmalign,
					  &field_value_datums,
					  &field_value_nulls,
					  &num_values);

	if (num_values != num_fields)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("type_oids and field_values must have same length")));

	Datum *null_flag_datums;
	bool *null_flag_nulls;
	int num_nulls;
	deconstruct_array(field_nulls_array,
					  BOOLOID,
					  sizeof(bool),
					  true,
					  TYPALIGN_CHAR,
					  &null_flag_datums,
					  &null_flag_nulls,
					  &num_nulls);

	if (num_nulls != num_fields)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("type_oids and field_nulls must have same length")));

	Bloom1Hasher *hasher = bloom1_hasher_create(type_oids, num_fields);

	NullableDatum values[MAX_BLOOM_FILTER_COLUMNS];
	Oid array_elemtype = ARR_ELEMTYPE(field_values_array);
	for (int i = 0; i < num_fields; i++)
	{
		values[i].isnull = DatumGetBool(null_flag_datums[i]);
		if (!values[i].isnull)
		{
			Datum value = field_value_datums[i];
			if (array_elemtype != type_oids[i])
			{
				Oid typinput, typioparam, typoutput;
				bool typIsVarlena;
				char *str;

				getTypeOutputInfo(array_elemtype, &typoutput, &typIsVarlena);
				str = OidOutputFunctionCall(typoutput, value);

				getTypeInputInfo(type_oids[i], &typinput, &typioparam);
				value = OidInputFunctionCall(typinput, str, typioparam, -1);

				pfree(str);
			}
			values[i].value = value;
		}
	}

	uint64 hash = hasher->hash_values(hasher, values);
	PG_RETURN_INT64((int64) hash);
}

#endif // #ifndef NDEBUG

char const *bloom1_column_prefix = NULL;

bool
batch_metadata_builder_bloom1_hash_maybe_present(Datum bloom_datum, uint64 hash)
{
	struct varlena *bloom = DatumGetByteaPP(bloom_datum);
	if (bloom == NULL)
		return true; /* No bloom = might match */

	const char *words_buf = VARDATA_ANY(bloom);
	const uint32 num_bits = 8 * VARSIZE_ANY_EXHDR(bloom);

	/* Validate bloom structure */
	CheckCompressedData(num_bits == (1ULL << pg_leftmost_one_pos32(num_bits)));
	CheckCompressedData(num_bits >= 64);

	return check_bloom_bits(words_buf, num_bits, hash);
}
