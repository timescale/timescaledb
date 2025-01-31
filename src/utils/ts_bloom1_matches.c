/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include "postgres.h"

#include <catalog/pg_collation_d.h>
#include <common/hashfn.h>
#include <utils/builtins.h>
#include <utils/typcache.h>

#include "debug_assert.h"
#include "export.h"

#include "utils/bloom1_sparse_index_params.h"

TS_FUNCTION_INFO_V1(ts_bloom1_matches);

Datum
ts_bloom1_matches(PG_FUNCTION_ARGS)
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
	Ensure(OidIsValid(val_type), "cannot determine argument type");
	TypeCacheEntry *val_entry = lookup_type_cache(val_type, TYPECACHE_HASH_PROC);
	Ensure(OidIsValid(val_entry->hash_proc), "cannot determine hash function");
	const Oid hash_proc_oid = val_entry->hash_proc;

	/* compute the hashes, used for the bloom filter */
	Datum val = PG_GETARG_DATUM(1);
	const uint32 datum_hash =
		DatumGetUInt32(OidFunctionCall1Coll(hash_proc_oid, C_COLLATION_OID, val));

	/* compute the requested number of hashes */
	bytea *bloom = PG_GETARG_VARLENA_PP(0);
	const int nbits = bloom1_num_bits(bloom);
	const uint64 *words = bloom1_words(bloom);
	const int word_bits = sizeof(*words) * 8;
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
