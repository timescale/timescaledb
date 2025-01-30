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
	bytea *bloom = PG_GETARG_VARLENA_PP(0);
	Datum val = PG_GETARG_DATUM(1);

	const int nbits = VARSIZE_ANY_EXHDR(bloom) * 8;

	Oid val_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	Ensure(OidIsValid(val_type), "cannot determine argument type");
	TypeCacheEntry *val_entry = lookup_type_cache(val_type, TYPECACHE_HASH_PROC);
	Ensure(OidIsValid(val_entry->hash_proc), "cannot determine hash function");
	const Oid hash_proc_oid = val_entry->hash_proc;

	/* compute the hashes, used for the bloom filter */
	uint32 datum_hash = DatumGetUInt32(OidFunctionCall1Coll(hash_proc_oid, C_COLLATION_OID, val));
	uint32 h1 = hash_bytes_uint32_extended(datum_hash, BLOOM1_SEED_1) % nbits;
	uint32 h2 = hash_bytes_uint32_extended(datum_hash, BLOOM1_SEED_2) % nbits;

	/* compute the requested number of hashes */
	const char *words = VARDATA_ANY(bloom);
	const int word_bits = sizeof(*words) * 8;
	bool match = true;
	for (int i = 0; i < BLOOM1_HASHES; i++)
	{
		/* h1 + h2 + f(i) */
		uint32 h = (h1 + i * h2) % nbits;
		uint32 word_index = (h / word_bits);
		uint32 bit = (h % word_bits);

		/* if the bit is not set, set it and remember we did that */
		match = (words[word_index] & (0x01 << bit)) && match;
	}

	PG_RETURN_BOOL(match);
}
