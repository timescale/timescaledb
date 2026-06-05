/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * fastlanes/fastlanes_ffor.c -- public FFOR (Frame Of Reference)
 * pack/unpack entry points.
 *
 * Defines fl_pack_ffor and fl_unpack_ffor as switches
 * on fl_tier_select that forward to the matching per-tier static.
 */

#include <postgres.h>

#include "fastlanes.h"
#include "fastlanes_common.h"

/*
 * These extern declarations are needed because MSVC runs out of the heap space if all FFOR
 * macro expansion happens in the same C file. These functions are intentionally
 * not placed in a header file, to discourage direct use.
 */
extern size_t fl8_pack_ffor(const void *values, void *packed, uint32 n, uint8 w, fl_elem_width_t t,
							uint64 base);
extern size_t fl16_pack_ffor(const void *values, void *packed, uint32 n, uint8 w, fl_elem_width_t t,
							 uint64 base);
extern size_t fl32_pack_ffor(const void *values, void *packed, uint32 n, uint8 w, fl_elem_width_t t,
							 uint64 base);
extern size_t fl64_pack_ffor(const void *values, void *packed, uint32 n, uint8 w, fl_elem_width_t t,
							 uint64 base);
extern size_t fl128_pack_ffor(const void *values, void *packed, uint32 n, uint8 w,
							  fl_elem_width_t t, uint64 base);
extern size_t fl256_pack_ffor(const void *values, void *packed, uint32 n, uint8 w,
							  fl_elem_width_t t, uint64 base);
extern void fl8_unpack_ffor(const void *packed, void *values, uint32 n, uint8 w, fl_elem_width_t t,
							uint64 base);
extern void fl16_unpack_ffor(const void *packed, void *values, uint32 n, uint8 w, fl_elem_width_t t,
							 uint64 base);
extern void fl32_unpack_ffor(const void *packed, void *values, uint32 n, uint8 w, fl_elem_width_t t,
							 uint64 base);
extern void fl64_unpack_ffor(const void *packed, void *values, uint32 n, uint8 w, fl_elem_width_t t,
							 uint64 base);
extern void fl128_unpack_ffor(const void *packed, void *values, uint32 n, uint8 w,
							  fl_elem_width_t t, uint64 base);
extern void fl256_unpack_ffor(const void *packed, void *values, uint32 n, uint8 w,
							  fl_elem_width_t t, uint64 base);

/* Public FFOR pack/unpack
 *
 * Each per-tier fl{T}_pack_ffor already computes the truncated byte
 * count internally, so the dispatcher just forwards the result.
 */

size_t
fl_pack_ffor(const void *values, void *packed, uint32 n, uint8 w, fl_elem_width_t t, uint64 base)
{
	switch (fl_tier_select(n, t))
	{
		case FL_TIER_W8:
			return fl8_pack_ffor(values, packed, n, w, t, base);
		case FL_TIER_W16:
			return fl16_pack_ffor(values, packed, n, w, t, base);
		case FL_TIER_W32:
			return fl32_pack_ffor(values, packed, n, w, t, base);
		case FL_TIER_W64:
			return fl64_pack_ffor(values, packed, n, w, t, base);
		case FL_TIER_W128:
			return fl128_pack_ffor(values, packed, n, w, t, base);
		case FL_TIER_W256:
			return fl256_pack_ffor(values, packed, n, w, t, base);
	}
	Assert(false);
	return 0;
}

void
fl_unpack_ffor(const void *packed, void *values, uint32 n, uint8 w, fl_elem_width_t t, uint64 base)
{
	switch (fl_tier_select(n, t))
	{
		case FL_TIER_W8:
			fl8_unpack_ffor(packed, values, n, w, t, base);
			break;
		case FL_TIER_W16:
			fl16_unpack_ffor(packed, values, n, w, t, base);
			break;
		case FL_TIER_W32:
			fl32_unpack_ffor(packed, values, n, w, t, base);
			break;
		case FL_TIER_W64:
			fl64_unpack_ffor(packed, values, n, w, t, base);
			break;
		case FL_TIER_W128:
			fl128_unpack_ffor(packed, values, n, w, t, base);
			break;
		case FL_TIER_W256:
			fl256_unpack_ffor(packed, values, n, w, t, base);
			break;
	}
}
