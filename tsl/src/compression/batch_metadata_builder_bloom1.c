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

#include "import/umash.h"

#include "sparse_index_bloom1.h"

typedef struct Bloom1MetadataBuilder
{
	BatchMetadataBuilder functions;

	int16 bloom_attr_offset;

	bytea *bloom_bytea;

	HashFunction hash_function;
} Bloom1MetadataBuilder;

static void bloom1_update_val(void *builder_, Datum val);
static void bloom1_update_null(void *builder_);
static void bloom1_insert_to_compressed_row(void *builder_, RowCompressor *compressor);
static void bloom1_reset(void *builder_, RowCompressor *compressor);

static uint64_t
bloom1_hash_2(Datum datum)
{
	uint64 tmp = DatumGetUInt16(datum);
	return bloom1_hash64(tmp);
}

static uint64_t
bloom1_hash_4(Datum datum)
{
	uint64 tmp = DatumGetUInt32(datum);
	return bloom1_hash64(tmp);
}

static uint64_t
bloom1_hash_8(Datum datum)
{
	uint64 tmp = DatumGetUInt64(datum);
	return bloom1_hash64(tmp);
}

#ifdef TS_USE_UMASH
static struct umash_params *
hashing_params()
{
	static struct umash_params params = { 0 };
	if (params.poly[0][0] == 0)
	{
		umash_params_derive(&params, 0x12345abcdef67890ull, NULL);
		Assert(params.poly[0][0] != 0);
	}

	return &params;
}

static uint64_t
bloom1_hash_varlena(Datum datum)
{
	const int length = VARSIZE_ANY_EXHDR(datum);
	const char *data = VARDATA_ANY(datum);
	return umash_full(hashing_params(),
					  /* seed = */ ~0ULL,
					  /* which = */ 0,
					  data,
					  length);
}

static uint64_t
bloom1_hash_16(Datum datum)
{
	return umash_full(hashing_params(),
					  /* seed = */ ~0ULL,
					  /* which = */ 0,
					  DatumGetPointer(datum),
					  16);
}
#endif

HashFunction
bloom1_get_hash_function(Oid type)
{
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
		default:
			return NULL;
	}
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
		.hash_function = bloom1_get_hash_function(type_oid),
	};

	Ensure(builder->hash_function != NULL,
		   "cannot find bloom1 hash function for type %d",
		   type_oid);

	/*
	 * Initialize the bloom filter.
	 */
	const int desired_bits = TARGET_COMPRESSED_BATCH_SIZE * 8;
	const int bytea_size = bloom1_bytea_alloc_size(desired_bits);
	builder->bloom_bytea = palloc0(bytea_size);
	SET_VARSIZE(builder->bloom_bytea, bytea_size);

	return &builder->functions;
}

void
bloom1_update_val(void *builder_, Datum val)
{
	Bloom1MetadataBuilder *builder = (Bloom1MetadataBuilder *) builder_;

	const uint64 datum_hash_1 = builder->hash_function(val);
	//	const uint64 datum_hash_1 =
	//		DatumGetUInt64(FunctionCall2Coll(&builder->hash_function, C_COLLATION_OID, val,
	//		BLOOM1_SEED_1));
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
	return -(nbits / (double) BLOOM1_HASHES) * log(1 - nset / (double) nbits);
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

static void
bloom1_reset(void *builder_, RowCompressor *compressor)
{
	Bloom1MetadataBuilder *builder = (Bloom1MetadataBuilder *) builder_;

	memset(VARDATA(builder->bloom_bytea), 0, VARSIZE_ANY_EXHDR(builder->bloom_bytea));

	compressor->compressed_is_null[builder->bloom_attr_offset] = true;
	compressor->compressed_values[builder->bloom_attr_offset] = 0;
}

typedef struct Bloom1MatchesCache
{
} Bloom1MatchesCache;

TS_FUNCTION_INFO_V1(tsl_bloom1_matches);

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

	HashFunction hash_function = fcinfo->flinfo->fn_extra;
	if (hash_function == NULL)
	{
		Oid val_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
		Ensure(OidIsValid(val_type), "cannot determine argument type");
		hash_function = bloom1_get_hash_function(val_type);
		fcinfo->flinfo->fn_extra = hash_function;
		Ensure(hash_function != NULL, "cannot find bloom1 hash function for type %d", val_type);
	}

	const uint64 datum_hash = hash_function(PG_GETARG_DATUM(1));
	//	const uint64 datum_hash_1 =
	//		DatumGetUInt64(OidFunctionCall2Coll(hash_proc_oid, C_COLLATION_OID, val,
	//			BLOOM1_SEED_1));
	//	const uint64 datum_hash_2 =
	//		DatumGetUInt64(OidFunctionCall2Coll(hash_proc_oid, C_COLLATION_OID, val,
	//			BLOOM1_SEED_2));

	bytea *bloom = PG_GETARG_VARLENA_PP(0);
	const int nbits = bloom1_num_bits(bloom);
	const uint64 *words = bloom1_words(bloom);
	const int word_bits = sizeof(*words) * 8;
	Assert(nbits % word_bits == 0);
	bool match = true;
	for (int i = 0; i < BLOOM1_HASHES; i++)
	{
		// const uint32 h = (datum_hash_1 + i * datum_hash_2) % nbits;
		const uint32 h = bloom1_get_one_hash(datum_hash, i) % nbits;
		const uint32 word_index = (h / word_bits);
		const uint32 bit = (h % word_bits);
		match = (words[word_index] & (0x01 << bit)) && match;
	}

	PG_RETURN_BOOL(match);
}
