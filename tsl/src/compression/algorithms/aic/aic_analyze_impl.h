/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * aic/aic_analyze_impl.h -- per-type analyzer template.
 *
 * Usage (only from aic_analyze.c, at the bottom of the file):
 *     #define AIC_TY uint16                 // uint16/uint32/uint64
 *     #define AIC_STY int16                 // matching signed type
 *     #include "aic_analyze_impl.h"
 *
 * Emits these functions per parameter set:
 *
 *   static inline aic_analyze_minmax_{AIC_TY}
 *   static inline aic_analyze_residuals_both_{AIC_TY}
 *   static inline aic_analyze_residuals_high_{AIC_TY}
 *   static inline aic_analyze_residuals_{AIC_TY}
 *   static inline aic_analyze_delta_range_{AIC_TY}
 *   static inline aic_analyze_zigzag_lo_{AIC_TY}
 *   extern        aic_analyze_{AIC_TY}
 *
 * this file relies on the AICAnalysis struct and the static helpers
 * (aic_decide, aic_dfor_stride, aic_pfor_search, ...) defined above the
 * inclusion point.
 */

#if !defined(AIC_TY) || !defined(AIC_STY)
#error                                                                                             \
	"aic_analyze_impl.h requires AIC_TY (uint16/uint32/uint64) and AIC_STY (matching signed type)"
#endif

/* Symbol-name plumbing: two-level paste so AIC_TY expands before ## */
#define AIC_PASTE2_(a, b) a##b
#define AIC_PASTE2(a, b) AIC_PASTE2_(a, b)
#define AIC_TFN(prefix) AIC_PASTE2(prefix, AIC_TY)

/* type specific analyzer functions */

static inline void
AIC_TFN(aic_analyze_minmax_)(const AIC_STY *buf, uint32 n, AIC_STY *vmin_out, AIC_STY *vmax_out)
{
	AIC_STY vmin = buf[0];
	AIC_STY vmax = buf[0];
	uint32 i;
	for (i = 1; i < n; ++i)
	{
		if (buf[i] < vmin)
		{
			vmin = buf[i];
		}
		if (buf[i] > vmax)
		{
			vmax = buf[i];
		}
	}
	*vmin_out = vmin;
	*vmax_out = vmax;
}

/* calculate the residuals to be used for both DICT and PFOR analysis
 * and also calculate the OR of all residuals and the number of residuals
 * that have the highest bit set */
static inline void
AIC_TFN(aic_analyze_residuals_both_)(const AIC_STY *restrict buf, uint32 n, AIC_STY vmin, uint8 W,
									 uint64 *restrict residuals, uint64 *or_out, uint32 *n_high_out)
{
	uint64 bound = (uint64) 1 << (W - 1);
	uint64 acc_or = 0;
	uint32 n_high = 0;
	uint32 i;
	for (i = 0; i < n; ++i)
	{
		uint64 r = (uint64) (AIC_TY) ((AIC_TY) buf[i] - (AIC_TY) vmin);
		residuals[i] = r;
		acc_or |= r;
		n_high += (r >= bound);
	}
	*or_out = acc_or;
	*n_high_out = n_high;
}

/* calculate the residuals to be used for both DICT and PFOR analysis
 * and also calculate the number of residuals that have the highest bit set */
static inline void
AIC_TFN(aic_analyze_residuals_high_)(const AIC_STY *restrict buf, uint32 n, AIC_STY vmin, uint8 W,
									 uint64 *restrict residuals, uint32 *n_high_out)
{
	uint64 bound = (uint64) 1 << (W - 1);
	uint32 n_high = 0;
	uint32 i;
	for (i = 0; i < n; ++i)
	{
		uint64 r = (uint64) (AIC_TY) ((AIC_TY) buf[i] - (AIC_TY) vmin);
		residuals[i] = r;
		n_high += (r >= bound);
	}
	*n_high_out = n_high;
}

/* calculate the residuals only for the DICT analysis */
static inline void
AIC_TFN(aic_analyze_residuals_)(const AIC_STY *restrict buf, uint32 n, AIC_STY vmin,
								uint64 *restrict residuals)
{
	uint32 i;
	for (i = 0; i < n; ++i)
	{
		residuals[i] = (uint64) (AIC_TY) ((AIC_TY) buf[i] - (AIC_TY) vmin);
	}
}

/* this function collects the strided deltas, calculates the
 * range of the deltas and the range of the bases, and stages
 * the deltas into the result's DFOR buffer. this is used to
 * calculate the DFOR cost and also to stage the deltas for
 * the DFOR encoder. */
static inline void
AIC_TFN(aic_analyze_delta_range_)(const AIC_STY *buf, uint32 n, uint16 S,
								  AIC_TY *restrict deltas_out, int64 *dmin_out, int64 *dmax_out,
								  int64 *bases_min_out, uint8 *w_bases_out)
{
	uint32 n_deltas = n - S;
	AIC_TY d0 = (AIC_TY) ((AIC_TY) buf[S] - (AIC_TY) buf[0]);
	/* min and max for the bases: */
	AIC_TY bmin = (AIC_TY) buf[0];
	AIC_TY bmax = (AIC_TY) buf[0];
	/* min and max for the stride-S deltas: */
	AIC_TY dmin = d0;
	AIC_TY dmax = d0;
	uint32 i;
	/* calculate the base min/max */
	for (i = 1; i < S; ++i)
	{
		if ((AIC_STY) buf[i] < (AIC_STY) bmin)
		{
			bmin = (AIC_TY) buf[i];
		}
		if ((AIC_STY) buf[i] > (AIC_STY) bmax)
		{
			bmax = (AIC_TY) buf[i];
		}
	}
	*bases_min_out = aic_sign_extend_u64((uint64) bmin, (uint8) (sizeof(AIC_TY) * 8));
	*w_bases_out = aic_bit_width_u64((uint64) (AIC_TY) (bmax - bmin));
	/* calculate the stride-S deltas and their min/max */
	deltas_out[0] = d0;
	for (i = 1; i < n_deltas; ++i)
	{
		AIC_TY d = (AIC_TY) ((AIC_TY) buf[S + i] - (AIC_TY) buf[i]);
		deltas_out[i] = d;
		if ((AIC_STY) d < (AIC_STY) dmin)
		{
			dmin = d;
		}
		if ((AIC_STY) d > (AIC_STY) dmax)
		{
			dmax = d;
		}
	}
	*dmin_out = aic_sign_extend_u64((uint64) dmin, (uint8) (sizeof(AIC_TY) * 8));
	*dmax_out = aic_sign_extend_u64((uint64) dmax, (uint8) (sizeof(AIC_TY) * 8));
}

/* zz_lo from the staged stride-S deltas if zigzag is used, this is used
 * for exact costing of the DFOR delta residuals and also the encoder will
 * use this as the base for the zigzag encoded delta residuals. */
static inline void
AIC_TFN(aic_analyze_zigzag_lo_)(const AIC_TY *deltas, uint32 n_deltas, uint64 *zz_lo_out)
{
	AIC_TY zz_min = (AIC_TY) ~(AIC_TY) 0;
	uint32 i;
	for (i = 0; i < n_deltas; ++i)
	{
		AIC_TY zz = AIC_TFN(aic_zigzag_encode_)(deltas[i]);
		zz_min = (zz < zz_min) ? zz : zz_min;
	}
	*zz_lo_out = (uint64) zz_min;
}

/* this is the type specific external API function */
void
AIC_TFN(aic_analyze_)(const AIC_STY *buf, uint32 n, fl_elem_width_t T, AICAnalyzeResult *result)
{
	AICAnalysis a;
	AIC_STY vmin;
	AIC_STY vmax;
	Assert(n >= 1 && n <= AIC_FIXED_BLOCK_MAX_COUNT);
	/* first pass (A) over the data to find the min/max of the values */
	AIC_TFN(aic_analyze_minmax_)(buf, n, &vmin, &vmax);
	a.vmin = (int64) vmin;
	a.vmax = (int64) vmax;
	if (vmin == vmax)
	{
		/* this can only happen if the block doesn't have enough
		 * elements to trigger the RLE mode */
		result->type = AIC_BT_RLE;
		result->cost = 4 + aic_block_header_scalar_len(a.vmin);
		result->W = 0;
		result->vmin = a.vmin;
		return;
	}
	/* store the bitwidth of the residuals (v[i] - vmin) */
	a.W = aic_bit_width_u64((uint64) vmax - (uint64) vmin);
	a.S = aic_dfor_stride(n, T);
	/* we don't need DFOR if we don't have at least a full stride of values */
	a.dfor_valid = (n > (uint32) a.S);
	if (a.dfor_valid)
	{
		AIC_TFN(aic_analyze_delta_range_)
		(buf,
		 n,
		 a.S,
		 result->dfor.deltas.AIC_PASTE2(AIC_TY, _elem),
		 &a.dmin,
		 &a.dmax,
		 &result->dfor.bases_min,
		 &result->dfor.W_bases);
		/* decide if we need zigzag to store the deltas */
		a.use_zigzag = (a.dmin < 0 && a.dmax >= 0);
		if (a.use_zigzag)
		{
			/* do another analysis pass so we have the zz_lo value for exact costing */
			AIC_TFN(aic_analyze_zigzag_lo_)
			(result->dfor.deltas.AIC_PASTE2(AIC_TY, _elem), (uint32) n - a.S, &a.zz_lo);
		}
		/* PFOR trial can be skipped depending on the DFOR savings */
		a.skip_pfor_strong =
			(aic_dfor_bit_savings(a.W, a.dmin, a.dmax) >= aic_pfor_loses_on_dfor_savings(a.W));
	}
	else
	{
		/* if DFOR is not valid we can't favor it over PFOR */
		a.skip_pfor_strong = false;
	}
	/* assume we can skip PFOR based on the residual data. will revise it below */
	a.skip_pfor_main = true;
	/* for both PFOR and DICT we need to collect the residuals (value[i] - vmin) */
	if (!a.skip_pfor_strong)
	{
		/* when PFOR is a viable candidate we can combine the residual
		 * collection with other metrics that allow us to eliminate
		 * the PFOR probe. for all types, we want to collect the
		 * number of elements where the highest bit is set. if more
		 * than half the elements has this, then PFOR can't win */
		uint32 n_high = 0;
		if (sizeof(AIC_TY) == sizeof(uint64))
		{
			/* for 64bit types we also want to collect the bit positions where
			 * any bit is set in the residuals (residual_or), if more than the
			 * empirical threshold bits set, then PFOR can't win on uint64. */
			uint64 residual_or = 0;
			AIC_TFN(aic_analyze_residuals_both_)
			(buf, n, vmin, a.W, result->residuals, &residual_or, &n_high);
			a.skip_pfor_main = (aic_popcount64(residual_or) >= AIC_PFOR_POPCOUNT_THRESHOLD_U64) ||
							   (n_high >= (n / 2));
		}
		else
		{
			AIC_TFN(aic_analyze_residuals_high_)(buf, n, vmin, a.W, result->residuals, &n_high);
			a.skip_pfor_main = (n_high >= (n / 2));
		}
	}
	else if (a.W >= AIC_DICT_MIN_BITWIDTH)
	{
		AIC_TFN(aic_analyze_residuals_)(buf, n, vmin, result->residuals);
	}
	aic_decide(n, T, &a, result);
}

/* Teardown */
#undef AIC_TFN
#undef AIC_PASTE2
#undef AIC_PASTE2_
#undef AIC_TY
#undef AIC_STY
