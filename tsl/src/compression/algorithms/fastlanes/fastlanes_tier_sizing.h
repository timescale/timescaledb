/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * fastlanes/fastlanes_tier_sizing.h -- per-tier sizing helper.
 *
 * Usage:
 *     #define FL_TIER 256                 // 8/16/32/64/128/256
 *     #include "fastlanes_tier_sizing.h"
 *     #undef FL_TIER
 *
 * Emits this static inline helper per included tier:
 *
 *   fl{T}_words_needed     // bit-arithmetic primitive
 */

#if !defined(FL_TIER)
#error "fastlanes_tier_sizing.h requires FL_TIER (one of 8, 16, 32, 64, 128, 256)"
#endif

#if FL_TIER != 8 && FL_TIER != 16 && FL_TIER != 32 && FL_TIER != 64 && FL_TIER != 128 &&           \
	FL_TIER != 256
#error "FL_TIER must be 8, 16, 32, 64, 128, or 256"
#endif

#include "fastlanes_types.h"

/* Symbol-name plumbing */
#define FL_PASTE3_(a, b, c) a##b##c
#define FL_PASTE3(a, b, c) FL_PASTE3_(a, b, c)
#define FL_FN(suffix) FL_PASTE3(fl, FL_TIER, suffix)
#define FL_VEC_SIZE FL_TIER
#define FL_WORD_BYTES (FL_TIER / 8)

static inline uint32
FL_FN(_words_needed)(uint32 n, uint8 w, fl_elem_width_t t)
{
	uint32 s = FL_VEC_SIZE / t;
	uint32 r = (n + s - 1) / s;
	return (r * w + t - 1) / t;
}

/* Teardown */
#undef FL_FN
#undef FL_PASTE3
#undef FL_PASTE3_
#undef FL_VEC_SIZE
#undef FL_WORD_BYTES
/* Leaves FL_TIER defined; the caller undefs it after the final tier include. */
