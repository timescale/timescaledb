/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * aic/aic_blocks_impl.h -- per-type block reconstruct/broadcast template.
 *
 * Usage (only from aic_blocks.c, at the start of the decompression section):
 *     #define AIC_TY uint16                 // uint16/uint32/uint64
 *     #define AIC_STY int16                 // matching signed type
 *     #include "aic_blocks_impl.h"
 *
 * Emits these functions per parameter set (all static inline):
 *
 *   aic_rle_broadcast_{AIC_TY}
 *   aic_delta_rle_broadcast_{AIC_TY}
 *   aic_dfor_reconstruct_{AIC_TY}
 *   aic_pfor_reconstruct_{AIC_TY}
 */

#if !defined(AIC_TY) || !defined(AIC_STY)
#error "aic_blocks_impl.h requires AIC_TY (uint16/uint32/uint64) and AIC_STY (matching signed type)"
#endif

/* Symbol-name plumbing: two-level paste so AIC_TY expands before ## */
#define AIC_PASTE2_(a, b) a##b
#define AIC_PASTE2(a, b) AIC_PASTE2_(a, b)
#define AIC_TFN(prefix) AIC_PASTE2(prefix, AIC_TY)

static inline void
AIC_TFN(aic_rle_broadcast_)(uint8 *dest, AIC_STY base, uint16 count)
{
	AIC_STY *ptr = (AIC_STY *) dest;
	for (uint16 i = 0; i < count; ++i)
	{
		ptr[i] = base;
	}
}

static inline void
AIC_TFN(aic_delta_rle_broadcast_)(uint8 *dest, AIC_STY base, AIC_STY step, uint16 count)
{
	AIC_TY *ptr = (AIC_TY *) dest;
	AIC_TY b = (AIC_TY) base;
	AIC_TY s = (AIC_TY) step;
	for (uint16 i = 0; i < count; ++i)
	{
		ptr[i] = (AIC_TY) ((uint64) b + (uint64) i * (uint64) s);
	}
}

/* aic_dfor_reconstruct_.. : for in-place reconstruction of the original values
 * from stride-S delta residual values. The zigzag flag indicates whether the
 * elements were zigzag encoded or not.
 */
static inline void
AIC_TFN(aic_dfor_reconstruct_)(uint8 *dest, uint16 S, uint32 n, bool zigzag)
{
	AIC_TY *v = (AIC_TY *) dest;
	if (zigzag)
	{
		for (uint32 i = S; i < n; ++i)
		{
			AIC_TY zz = v[i];
			v[i] = (AIC_TY) (v[i - S] + AIC_TFN(cm_zigzag_decode_)(zz));
		}
	}
	else
	{
		for (uint32 i = S; i < n; ++i)
		{
			v[i] = v[i - S] + v[i];
		}
	}
}

/* aic_pfor_reconstruct_.. : add each exception's high bits back into
 * its body value. positions were validated against the element
 * count at prepare time, so every write lands inside dest. */
static inline void
AIC_TFN(aic_pfor_reconstruct_)(uint8 *dest, const uint8 *exc_area, const uint8 *positions,
							   uint32 n_exc, uint8 b)
{
	AIC_TY *v = (AIC_TY *) dest;
	const AIC_TY *exc = (const AIC_TY *) exc_area;
	for (uint32 e = 0; e < n_exc; ++e)
	{
		v[positions[e]] += (AIC_TY) (exc[e] << b);
	}
}

/* Teardown */
#undef AIC_TFN
#undef AIC_PASTE2
#undef AIC_PASTE2_
#undef AIC_TY
#undef AIC_STY
