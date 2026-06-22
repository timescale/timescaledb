/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * fastlanes/fastlanes_pack.c -- public plain pack/unpack entry points.
 *
 * Uses fastlanes_tier_sizing.h + fastlanes_tier_pack_impl.h as
 * templates driven by the FL_TIER.
 *
 * Defines fl_pack and fl_unpack as switches
 * on fl_tier_select that forward to the matching per-tier static.
 */

#include <postgres.h>

#include "fastlanes.h"
#include "fastlanes_common.h"

/*
 * These extern declarations are needed because MSVC runs out of the heap space if all `pack`
 * macro expansion happens in the same C file. These functions are intentionally
 * not placed in a header file, to discourage direct use.
 */

extern size_t fl8_pack(const void *values, void *packed, uint32 n, uint8 w, fl_elem_width_t t);
extern size_t fl16_pack(const void *values, void *packed, uint32 n, uint8 w, fl_elem_width_t t);
extern size_t fl32_pack(const void *values, void *packed, uint32 n, uint8 w, fl_elem_width_t t);
extern size_t fl64_pack(const void *values, void *packed, uint32 n, uint8 w, fl_elem_width_t t);
extern size_t fl128_pack(const void *values, void *packed, uint32 n, uint8 w, fl_elem_width_t t);
extern size_t fl256_pack(const void *values, void *packed, uint32 n, uint8 w, fl_elem_width_t t);
extern void fl8_unpack(const void *packed, void *values, uint32 n, uint8 w, fl_elem_width_t t);
extern void fl16_unpack(const void *packed, void *values, uint32 n, uint8 w, fl_elem_width_t t);
extern void fl32_unpack(const void *packed, void *values, uint32 n, uint8 w, fl_elem_width_t t);
extern void fl64_unpack(const void *packed, void *values, uint32 n, uint8 w, fl_elem_width_t t);
extern void fl128_unpack(const void *packed, void *values, uint32 n, uint8 w, fl_elem_width_t t);
extern void fl256_unpack(const void *packed, void *values, uint32 n, uint8 w, fl_elem_width_t t);

/* Public plain pack/unpack
 *
 * Each per-tier fl{T}_pack already computes the truncated byte count
 * internally (its return is fl{T}_words_needed(n, w, t) * WORD_BYTES),
 * so the dispatcher just forwards the result.
 */

size_t
fl_pack(const void *values, void *packed, uint32 n, uint8 w, fl_elem_width_t t)
{
	switch (fl_tier_select(n, t))
	{
		case FL_TIER_W8:
			return fl8_pack(values, packed, n, w, t);
		case FL_TIER_W16:
			return fl16_pack(values, packed, n, w, t);
		case FL_TIER_W32:
			return fl32_pack(values, packed, n, w, t);
		case FL_TIER_W64:
			return fl64_pack(values, packed, n, w, t);
		case FL_TIER_W128:
			return fl128_pack(values, packed, n, w, t);
		case FL_TIER_W256:
			return fl256_pack(values, packed, n, w, t);
	}
	Assert(false);
	return 0;
}

void
fl_unpack(const void *packed, void *values, uint32 n, uint8 w, fl_elem_width_t t)
{
	switch (fl_tier_select(n, t))
	{
		case FL_TIER_W8:
			fl8_unpack(packed, values, n, w, t);
			break;
		case FL_TIER_W16:
			fl16_unpack(packed, values, n, w, t);
			break;
		case FL_TIER_W32:
			fl32_unpack(packed, values, n, w, t);
			break;
		case FL_TIER_W64:
			fl64_unpack(packed, values, n, w, t);
			break;
		case FL_TIER_W128:
			fl128_unpack(packed, values, n, w, t);
			break;
		case FL_TIER_W256:
			fl256_unpack(packed, values, n, w, t);
			break;
	}
}
