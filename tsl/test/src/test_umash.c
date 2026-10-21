/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>

#include <fmgr.h>

#include "export.h"
#include "test_utils.h"

#ifdef TS_USE_UMASH
#include "import/umash.h"
#endif

TS_FUNCTION_INFO_V1(ts_test_umash_portable_clmul);
TS_FUNCTION_INFO_V1(ts_test_umash_disable_pmull);

/*
 * Check that the portable carryless multiplication gives the same hashes as
 * PMULL.
 */
Datum
ts_test_umash_portable_clmul(PG_FUNCTION_ARGS)
{
#ifdef TS_UMASH_RUNTIME_PMULL
	if (!ts_umash_use_pmull)
	{
		PG_RETURN_VOID();
	}

	struct umash_params params;
	umash_params_derive(&params, 0x12345abcdef67890ULL, NULL);

	uint8 data[1024];
	uint64 x = 0x9e3779b97f4a7c15ULL;
	for (size_t i = 0; i < sizeof(data); i++)
	{
		x ^= x << 13;
		x ^= x >> 7;
		x ^= x << 17;
		data[i] = (uint8) x;
	}

	for (size_t n = 0; n <= sizeof(data); n++)
	{
		const size_t offset = n % 7;
		const size_t length = Min(n, sizeof(data) - offset);

		ts_umash_use_pmull = true;
		const uint64 h0 = umash_full(&params, ~0ULL, 0, &data[offset], length);
		const uint64 h1 = umash_full(&params, ~0ULL, 1, &data[offset], length);
		const struct umash_fp fp = umash_fprint(&params, ~0ULL, &data[offset], length);

		ts_umash_use_pmull = false;
		TestAssertInt64Eq(umash_full(&params, ~0ULL, 0, &data[offset], length), h0);
		TestAssertInt64Eq(umash_full(&params, ~0ULL, 1, &data[offset], length), h1);
		const struct umash_fp portable_fp = umash_fprint(&params, ~0ULL, &data[offset], length);
		TestAssertInt64Eq(portable_fp.hash[0], fp.hash[0]);
		TestAssertInt64Eq(portable_fp.hash[1], fp.hash[1]);

		ts_umash_use_pmull = true;
	}
#endif

	PG_RETURN_VOID();
}

/*
 * Use the portable carryless multiplication for the rest of the session.
 */
Datum
ts_test_umash_disable_pmull(PG_FUNCTION_ARGS)
{
#ifdef TS_UMASH_RUNTIME_PMULL
	ts_umash_use_pmull = false;
#endif

	PG_RETURN_VOID();
}
