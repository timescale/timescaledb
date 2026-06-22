/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * fastlanes/fastlanes_tier_ffor_impl.h -- FFOR pack/unpack template.
 *
 * Usage:
 *     #define FL_TIER 256                 // 8/16/32/64/128/256
 *     #include "fastlanes_tier_sizing.h"  // REQUIRED first --
 *                                         // FFOR pack returns size
 *                                         // via fl{T}_words_needed
 *     #include "fastlanes_tier_ffor_impl.h"
 *     #undef FL_TIER
 *
 * Emits:
 *   - FFOR per-W kernels: fl{T}_ffor_{pack,unpack}_u{8..64}_w{1..T}
 *   - FFOR per-T dispatchers: fl{T}_ffor_{pack,unpack}_t{8..64}
 *   - public entry points: fl{T}_pack_ffor, fl{T}_unpack_ffor
 */

#if !defined(FL_TIER)
#error "fastlanes_tier_ffor_impl.h requires FL_TIER (one of 8, 16, 32, 64, 128, 256)"
#endif

#if FL_TIER != 8 && FL_TIER != 16 && FL_TIER != 32 && FL_TIER != 64 && FL_TIER != 128 &&           \
	FL_TIER != 256
#error "FL_TIER must be 8, 16, 32, 64, 128, or 256"
#endif

#include "fastlanes_template_macros.h"

/* Symbol-name plumbing */
#define FL_PASTE3_(a, b, c) a##b##c
#define FL_PASTE3(a, b, c) FL_PASTE3_(a, b, c)
#define FL_FN(suffix) FL_PASTE3(fl, FL_TIER, suffix)
#define FL_FNW(suffix, W) FL_PASTE3(FL_FN(suffix), _w, W)
#define FL_VEC_SIZE FL_TIER
#define FL_WORD_BYTES (FL_TIER / 8)
/* Per-tier S = FL_TIER / T constants. Baked to single literals so MSVC's
 * preprocessor doesn't blow up. */
#if FL_TIER == 256
#define FL_S8 32
#define FL_S16 16
#define FL_S32 8
#define FL_S64 4
#elif FL_TIER == 128
#define FL_S8 16
#define FL_S16 8
#define FL_S32 4
#define FL_S64 2
#elif FL_TIER == 64
#define FL_S8 8
#define FL_S16 4
#define FL_S32 2
#define FL_S64 1
#elif FL_TIER == 32
#define FL_S8 4
#define FL_S16 2
#define FL_S32 1
#elif FL_TIER == 16
#define FL_S8 2
#define FL_S16 1
#elif FL_TIER == 8
#define FL_S8 1
#endif

/* Bind FL_FP1 / FL_FU1 to the promoted FFOR row bodies */

#undef FL_FP1
#define FL_FP1(T, S, W, r) FL_FFOR_PACK_ROW(T, S, W, r)

#undef FL_FU1
#define FL_FU1(TYPE, T, S, W, r)                                                                   \
	if ((r) > 0)                                                                                   \
	{                                                                                              \
		FL_UNPACK_LOAD(T, S, W, r)                                                                 \
	}                                                                                              \
	FL_FFOR_UNPACK_ROW(TYPE, T, S, W, r)

/* Per-W FFOR kernel generators */

#if FL_TIER >= 64
#define FL_DEF_FFOR_PACK_U64(W)                                                                    \
	static void FL_FNW(_ffor_pack_u64,                                                             \
					   W)(const uint64 *restrict in, uint64 *restrict out, uint64 base)            \
	{                                                                                              \
		const uint64 mask = ((W) == 64) ? UINT64_MAX : ((uint64) 1 << ((W) & 63)) - 1;             \
		for (int lane = 0; lane < FL_S64; lane++)                                                  \
		{                                                                                          \
			uint64 tmp = 0, src;                                                                   \
			FL_FFOR_PACK_ALL_64(FL_S64, W)                                                         \
			FL_PACK_FLUSH(64, FL_S64, W, 64)                                                       \
		}                                                                                          \
	}
#define FL_DEF_FFOR_UNPACK_U64(W)                                                                  \
	static void FL_FNW(_ffor_unpack_u64,                                                           \
					   W)(const uint64 *restrict packed, uint64 *restrict out, uint64 base)        \
	{                                                                                              \
		const uint64 mask = ((W) == 64) ? UINT64_MAX : ((uint64) 1 << ((W) & 63)) - 1;             \
		for (int lane = 0; lane < FL_S64; lane++)                                                  \
		{                                                                                          \
			uint64 src = packed[lane], tmp;                                                        \
			FL_FFOR_UNPACK_ALL_64(uint64, FL_S64, W)                                               \
		}                                                                                          \
	}
FL_W_LIST_64(FL_DEF_FFOR_PACK_U64)
FL_W_LIST_64(FL_DEF_FFOR_UNPACK_U64)
#endif /* FL_TIER >= 64 */

#if FL_TIER >= 32
#define FL_DEF_FFOR_PACK_U32(W)                                                                    \
	static void FL_FNW(_ffor_pack_u32,                                                             \
					   W)(const uint32 *restrict in, uint32 *restrict out, uint32 base)            \
	{                                                                                              \
		const uint32 mask = ((W) == 32) ? UINT32_MAX : ((uint32) 1 << ((W) & 31)) - 1;             \
		for (int lane = 0; lane < FL_S32; lane++)                                                  \
		{                                                                                          \
			uint32 tmp = 0, src;                                                                   \
			FL_FFOR_PACK_ALL_32(FL_S32, W)                                                         \
			FL_PACK_FLUSH(32, FL_S32, W, 32)                                                       \
		}                                                                                          \
	}
#define FL_DEF_FFOR_UNPACK_U32(W)                                                                  \
	static void FL_FNW(_ffor_unpack_u32,                                                           \
					   W)(const uint32 *restrict packed, uint32 *restrict out, uint32 base)        \
	{                                                                                              \
		const uint32 mask = ((W) == 32) ? UINT32_MAX : ((uint32) 1 << ((W) & 31)) - 1;             \
		for (int lane = 0; lane < FL_S32; lane++)                                                  \
		{                                                                                          \
			uint32 src = packed[lane], tmp;                                                        \
			FL_FFOR_UNPACK_ALL_32(uint32, FL_S32, W)                                               \
		}                                                                                          \
	}
FL_W_LIST_32(FL_DEF_FFOR_PACK_U32)
FL_W_LIST_32(FL_DEF_FFOR_UNPACK_U32)
#endif /* FL_TIER >= 32 */

#if FL_TIER >= 16
#define FL_DEF_FFOR_PACK_U16(W)                                                                    \
	static void FL_FNW(_ffor_pack_u16,                                                             \
					   W)(const uint16 *restrict in, uint16 *restrict out, uint16 base)            \
	{                                                                                              \
		const uint16 mask =                                                                        \
			((W) == 16) ? (uint16) UINT16_MAX : (uint16) (((uint16) 1 << ((W) & 15)) - 1);         \
		for (int lane = 0; lane < FL_S16; lane++)                                                  \
		{                                                                                          \
			uint16 tmp = 0, src;                                                                   \
			FL_FFOR_PACK_ALL_16(FL_S16, W)                                                         \
			FL_PACK_FLUSH(16, FL_S16, W, 16)                                                       \
		}                                                                                          \
	}
#define FL_DEF_FFOR_UNPACK_U16(W)                                                                  \
	static void FL_FNW(_ffor_unpack_u16,                                                           \
					   W)(const uint16 *restrict packed, uint16 *restrict out, uint16 base)        \
	{                                                                                              \
		const uint16 mask =                                                                        \
			((W) == 16) ? (uint16) UINT16_MAX : (uint16) (((uint16) 1 << ((W) & 15)) - 1);         \
		for (int lane = 0; lane < FL_S16; lane++)                                                  \
		{                                                                                          \
			uint16 src = packed[lane], tmp;                                                        \
			FL_FFOR_UNPACK_ALL_16(uint16, FL_S16, W)                                               \
		}                                                                                          \
	}
FL_W_LIST_16(FL_DEF_FFOR_PACK_U16)
FL_W_LIST_16(FL_DEF_FFOR_UNPACK_U16)
#endif /* FL_TIER >= 16 */

/* T=8 always present. */
#define FL_DEF_FFOR_PACK_U8(W)                                                                     \
	static void FL_FNW(_ffor_pack_u8,                                                              \
					   W)(const uint8 *restrict in, uint8 *restrict out, uint8 base)               \
	{                                                                                              \
		const uint8 mask =                                                                         \
			((W) == 8) ? (uint8) UINT8_MAX : (uint8) (((uint8) 1 << ((W) & 7)) - 1);               \
		for (int lane = 0; lane < FL_S8; lane++)                                                   \
		{                                                                                          \
			uint8 tmp = 0, src;                                                                    \
			FL_FFOR_PACK_ALL_8(FL_S8, W)                                                           \
			FL_PACK_FLUSH(8, FL_S8, W, 8)                                                          \
		}                                                                                          \
	}
#define FL_DEF_FFOR_UNPACK_U8(W)                                                                   \
	static void FL_FNW(_ffor_unpack_u8,                                                            \
					   W)(const uint8 *restrict packed, uint8 *restrict out, uint8 base)           \
	{                                                                                              \
		const uint8 mask =                                                                         \
			((W) == 8) ? (uint8) UINT8_MAX : (uint8) (((uint8) 1 << ((W) & 7)) - 1);               \
		for (int lane = 0; lane < FL_S8; lane++)                                                   \
		{                                                                                          \
			uint8 src = packed[lane], tmp;                                                         \
			FL_FFOR_UNPACK_ALL_8(uint8, FL_S8, W)                                                  \
		}                                                                                          \
	}
FL_W_LIST_8(FL_DEF_FFOR_PACK_U8)
FL_W_LIST_8(FL_DEF_FFOR_UNPACK_U8)

/* Per-T FFOR dispatchers */

#if FL_TIER >= 64
static inline void
FL_FN(_ffor_pack_t64)(const uint64 *in, void *out, uint8 w, uint64 base)
{
	switch (w)
	{
#define FL_CASE(W)                                                                                 \
	case W:                                                                                        \
		FL_FNW(_ffor_pack_u64, W)(in, (uint64 *) out, base);                                       \
		break;
		FL_W_LIST_64(FL_CASE)
#undef FL_CASE
	}
}
static inline void
FL_FN(_ffor_unpack_t64)(const void *packed, uint64 *out, uint8 w, uint64 base)
{
	switch (w)
	{
#define FL_CASE(W)                                                                                 \
	case W:                                                                                        \
		FL_FNW(_ffor_unpack_u64, W)((const uint64 *) packed, out, base);                           \
		break;
		FL_W_LIST_64(FL_CASE)
#undef FL_CASE
	}
}
#endif /* FL_TIER >= 64 */

#if FL_TIER >= 32
static inline void
FL_FN(_ffor_pack_t32)(const uint32 *in, void *out, uint8 w, uint32 base)
{
	switch (w)
	{
#define FL_CASE(W)                                                                                 \
	case W:                                                                                        \
		FL_FNW(_ffor_pack_u32, W)(in, (uint32 *) out, base);                                       \
		break;
		FL_W_LIST_32(FL_CASE)
#undef FL_CASE
	}
}
static inline void
FL_FN(_ffor_unpack_t32)(const void *packed, uint32 *out, uint8 w, uint32 base)
{
	switch (w)
	{
#define FL_CASE(W)                                                                                 \
	case W:                                                                                        \
		FL_FNW(_ffor_unpack_u32, W)((const uint32 *) packed, out, base);                           \
		break;
		FL_W_LIST_32(FL_CASE)
#undef FL_CASE
	}
}
#endif /* FL_TIER >= 32 */

#if FL_TIER >= 16
static inline void
FL_FN(_ffor_pack_t16)(const uint16 *in, void *out, uint8 w, uint16 base)
{
	switch (w)
	{
#define FL_CASE(W)                                                                                 \
	case W:                                                                                        \
		FL_FNW(_ffor_pack_u16, W)(in, (uint16 *) out, base);                                       \
		break;
		FL_W_LIST_16(FL_CASE)
#undef FL_CASE
	}
}
static inline void
FL_FN(_ffor_unpack_t16)(const void *packed, uint16 *out, uint8 w, uint16 base)
{
	switch (w)
	{
#define FL_CASE(W)                                                                                 \
	case W:                                                                                        \
		FL_FNW(_ffor_unpack_u16, W)((const uint16 *) packed, out, base);                           \
		break;
		FL_W_LIST_16(FL_CASE)
#undef FL_CASE
	}
}
#endif /* FL_TIER >= 16 */

static inline void
FL_FN(_ffor_pack_t8)(const uint8 *in, void *out, uint8 w, uint8 base)
{
	switch (w)
	{
#define FL_CASE(W)                                                                                 \
	case W:                                                                                        \
		FL_FNW(_ffor_pack_u8, W)(in, (uint8 *) out, base);                                         \
		break;
		FL_W_LIST_8(FL_CASE)
#undef FL_CASE
	}
}
static inline void
FL_FN(_ffor_unpack_t8)(const void *packed, uint8 *out, uint8 w, uint8 base)
{
	switch (w)
	{
#define FL_CASE(W)                                                                                 \
	case W:                                                                                        \
		FL_FNW(_ffor_unpack_u8, W)((const uint8 *) packed, out, base);                             \
		break;
		FL_W_LIST_8(FL_CASE)
#undef FL_CASE
	}
}

/* Public entry points */

extern size_t
FL_FN(_pack_ffor)(const void *values, void *packed, uint32 n, uint8 w, fl_elem_width_t t,
				  uint64 base)
{
	if (w == 0)
	{
		return 0;
	}

	switch (t)
	{
#if FL_TIER >= 64
		case FL_ELEM_W64:
			FL_FN(_ffor_pack_t64)((const uint64 *) values, packed, w, base);
			break;
#endif
#if FL_TIER >= 32
		case FL_ELEM_W32:
			FL_FN(_ffor_pack_t32)((const uint32 *) values, packed, w, (uint32) base);
			break;
#endif
#if FL_TIER >= 16
		case FL_ELEM_W16:
			FL_FN(_ffor_pack_t16)((const uint16 *) values, packed, w, (uint16) base);
			break;
#endif
		case FL_ELEM_W8:
			FL_FN(_ffor_pack_t8)((const uint8 *) values, packed, w, (uint8) base);
			break;
		default:
			Assert(false);
			break;
	}
	return (size_t) FL_FN(_words_needed)(n, w, t) * FL_WORD_BYTES;
}

extern void
FL_FN(_unpack_ffor)(const void *packed, void *values, uint32 n, uint8 w, fl_elem_width_t t,
					uint64 base)
{
	if (w == 0)
	{
		/* we need all elem widths, no matter what the tier is */
		switch (t)
		{
			case FL_ELEM_W64:
			{
				uint64 *out = (uint64 *) values;
				uint64 b = base;
				for (uint32 i = 0; i < n; i++)
				{
					out[i] = b;
				}
				break;
			}
			case FL_ELEM_W32:
			{
				uint32 *out = (uint32 *) values;
				uint32 b = (uint32) base;
				for (uint32 i = 0; i < n; i++)
				{
					out[i] = b;
				}
				break;
			}
			case FL_ELEM_W16:
			{
				uint16 *out = (uint16 *) values;
				uint16 b = (uint16) base;
				for (uint32 i = 0; i < n; i++)
				{
					out[i] = b;
				}
				break;
			}
			case FL_ELEM_W8:
			{
				uint8 *out = (uint8 *) values;
				uint8 b = (uint8) base;
				for (uint32 i = 0; i < n; i++)
				{
					out[i] = b;
				}
				break;
			}
			default:
				Assert(false);
				break;
		}
		return;
	}

	switch (t)
	{
#if FL_TIER >= 64
		case FL_ELEM_W64:
			FL_FN(_ffor_unpack_t64)(packed, (uint64 *) values, w, base);
			break;
#endif
#if FL_TIER >= 32
		case FL_ELEM_W32:
			FL_FN(_ffor_unpack_t32)(packed, (uint32 *) values, w, (uint32) base);
			break;
#endif
#if FL_TIER >= 16
		case FL_ELEM_W16:
			FL_FN(_ffor_unpack_t16)(packed, (uint16 *) values, w, (uint16) base);
			break;
#endif
		case FL_ELEM_W8:
			FL_FN(_ffor_unpack_t8)(packed, (uint8 *) values, w, (uint8) base);
			break;
		default:
			Assert(false);
			break;
	}
}

/* Teardown */

#undef FL_DEF_FFOR_PACK_U64
#undef FL_DEF_FFOR_UNPACK_U64
#undef FL_DEF_FFOR_PACK_U32
#undef FL_DEF_FFOR_UNPACK_U32
#undef FL_DEF_FFOR_PACK_U16
#undef FL_DEF_FFOR_UNPACK_U16
#undef FL_DEF_FFOR_PACK_U8
#undef FL_DEF_FFOR_UNPACK_U8
#undef FL_FN
#undef FL_FNW
#undef FL_PASTE3
#undef FL_PASTE3_
#undef FL_VEC_SIZE
#undef FL_WORD_BYTES
#undef FL_S8
#undef FL_S16
#undef FL_S32
#undef FL_S64
/* Leaves FL_TIER defined; the caller undefs it after the final tier include. */
