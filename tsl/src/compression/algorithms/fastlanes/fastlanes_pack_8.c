/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

#include "fastlanes.h"
#include "fastlanes_common.h"

/*
 * These extern declarations are needed because MSVC runs out of the heap space if all `pack`
 * macro expansion happens in the same C file. The functions are being called from
 * `fastlanes_pack.c`, which dispatches between the tiers. These functions are intentionally
 * not placed in a header file, to discourage direct use.
 */
extern size_t fl8_pack(const void *values, void *packed, uint32 n, uint8 w, fl_elem_width_t t);
extern void fl8_unpack(const void *packed, void *values, uint32 n, uint8 w, fl_elem_width_t t);

/* clang format would reorder the headers which is not desired here */
/* clang-format off */

#define FL_TIER 8
#include "fastlanes_tier_sizing.h"
#include "fastlanes_tier_pack_impl.h"
#undef FL_TIER

/* clang-format on */
