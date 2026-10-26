/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include "aic_analyze.h"
#include "aic_utils.h"
#include "compression/algorithms/fastlanes/fastlanes.h"
#include "compression/algorithms/fastlanes/fastlanes_types.h"

/* this is the implementation of the analysis, where we decide
 * which encoder to use for a block of data. we try to make the
 * analysis as cheap as possible, so closed form (FOR) and simple
 * (DFOR) analysis takes precedence. based on the data we have
 * cases when we can completely eliminate the more expensive PFOR
 * analysis. the main analysis logic is in aic_decide, and the
 * prerequisite data is collected in the caller aic_analyze_##TY()
 * function.
 *
 * the aic_analyze_{uint16,uint32,uint64}() functions are type dependent
 * and call a few type dependent helpers too. they are emitted from the
 * aic_analyze_impl.h template, included once per type at the bottom of
 * this file. the majority of the logic is type independent and is
 * located in the upper part of this file.
 *
 * the analysis is broken down to 3 passes marked as A-C. in each
 * pass we collect more data and we may skip passes based on the
 * results of the previous pass. in some of the passes we stage
 * data that the encoder will reuse later, which would otherwise
 * be recomputed. the AICAnalysis structure is used to pass the
 * analysis results between the passes and it is local to this
 * file. the final outcome of the analysis is a decision on which
 * encoder to use and the parameters for that encoder. the decision
 * is returned in the AICAnalyzeResult structure, which is passed
 * to the encoder.
 */

/* please read the README.md file for more details. */

/* DICT analysis constants */
#define AIC_DICT_MIN_BITWIDTH 2		   /* DICT can only win at W >=2 */
#define AIC_DICT_ZIGZAG_MIN_BITWIDTH 4 /* delta-coded indices pay off from W > 4 */
#define AIC_DICT_ZIGZAG_MIN_K 4		   /* and from K > 4 */
#define AIC_DICT_HASH_SHIFT 56		   /* bucket = top bits of the Fibonacci product */
#define AIC_DICT_K_FLOOR 2			   /* the probe needs room for at least two distinct keys */
#define AIC_DICT_K_CEIL 128			   /* the probe is limited to 'ceil' distinct keys */
#define AIC_DICT_HASH_SIZE 256		   /* the size of the dictionary hash table */

/* the ht_idx slots are int8 and the sign bit tells if the slot is
 * occupied. this leaves room for the key indexes 0..127, so the K
 * ceiling must not exceed 128. */
StaticAssertDecl(AIC_DICT_K_CEIL <= 128, "int8 ht_idx cannot represent dict key indexes above 127");

/* PFOR analysis constants */
#define AIC_PFOR_POPCOUNT_THRESHOLD_U64 33 /* empirical threshold when PFOR can't win on uint64 */

/*
 * AICAnalysis : the analysis results for a block of n elements of width T.
 *  the analysis is broken down to 3 passes marked as A-C. in each pass we
 *  collect more data and we may skip passes based on the results of the
 *  previous pass.
 */
typedef struct AICAnalysis
{
	/* pass A : goes over all elements and calculates the min, max and the
	 * residual bitwidth: W =  bitwidth(vmax - vmin). these are used across
	 * all encoders and we run this pass unconditionally. */
	int64 vmin;
	int64 vmax;
	uint8 W;

	/* pass B: second pass over the elements at the DFOR stride S,
	 * staging the stride-S deltas into the result's DFOR buffer and
	 * reducing their min and max. these exact signals decide 1) if
	 * we can skip PFOR altogether when the DFOR signal is strong
	 * enough, 2) whether the DFOR delta residual stream packs zigzag
	 * (mixed signs) or raw with a base.
	 *
	 * when n <= S there are no stride-S deltas: dfor_valid is false,
	 * the fields below are undefined.
	 */
	bool dfor_valid;
	uint16 S;
	int64 dmin;
	int64 dmax;
	bool use_zigzag;
	bool skip_pfor_strong;
	/* only needed if use_zigzag is true: */
	uint64 zz_lo;

	/* pass C: DICT and PFOR need residuals[] = value - vmin and
	 * the PFOR decision has two additional conditions based on the
	 * number of entries with the highest bit set (n_high) and the number
	 * of OR'd bit set in the residual values. based on these we may
	 * be able to skip the PFOR analysis.
	 */
	bool skip_pfor_main; /* exception density or bit spread rules PFOR out */
} AICAnalysis;

/*
 * aic_dfor_bit_savings: W - bitwidth(dmax - dmin), DFOR's per-element
 * bit savings over FOR.
 */
static inline int
aic_dfor_bit_savings(uint8 W, int64 dmin, int64 dmax)
{
	return (int) W - (int) aic_bit_width_u64((uint64) dmax - (uint64) dmin);
}

/*
 * aic_pfor_loses_on_dfor_savings : returns the minimum number of bits of
 * savings that DFOR must have over FOR to make PFOR lose.
 */
static inline uint8
aic_pfor_loses_on_dfor_savings(uint8 W)
{
	return (W <= 8) ? 5 : 3;
}

/* aic_dfor_stride : the DFOR stride for a block of n elements of width T. */
static inline uint16
aic_dfor_stride(uint32 n, fl_elem_width_t T)
{
	if (n > 128)
	{
		return (T == FL_ELEM_W64) ? 8 : 4;
	}
	return (uint16) ((uint32) fl_tier_select(n, T) / (uint32) T);
}

/* aic_cost_for: total size of a FOR encoded block including the headers */
static inline int32
aic_cost_for(uint32 n, uint8 W, int64 vmin, fl_elem_width_t T)
{
	return 4 + aic_block_header_scalar_len(vmin) + (int32) fl_result_bytes(n, W, T);
}

/*
 * aic_cost_dfor : calculate the total bytes needed to store the block as DFOR
 * and also return the bitwidth of the delta residuals and the stride length (S).
 */
static inline int32
aic_cost_dfor(uint32 n, const AICAnalysis *a, fl_elem_width_t T, AICDforResult *dfor)
{
	uint32 W_delta_residuals;
	int32 delta_residual_base_len;

	if (!a->dfor_valid)
	{
		return -1;
	}
	if (a->use_zigzag)
	{
		/* figure the largest zigzag value in the stream and calculate
		 * its bitwidth */
		uint64 zz_dmax = (uint64) a->dmax << 1;
		uint64 zz_dmin = ~((uint64) a->dmin << 1);
		uint64 zz_hi = zz_dmax > zz_dmin ? zz_dmax : zz_dmin;
		uint64 zz_lo = a->zz_lo;

		W_delta_residuals = (uint32) aic_bit_width_u64(zz_hi - zz_lo);
		delta_residual_base_len =
			aic_block_header_scalar_len(aic_sign_extend_u64(zz_lo, (uint8) T));
		dfor->delta_residual_base = (int64) zz_lo;
	}
	else
	{
		/* we have the exact size because we did the stride-S analysis previously */
		W_delta_residuals = (uint32) aic_bit_width_u64((uint64) a->dmax - (uint64) a->dmin);
		delta_residual_base_len = aic_block_header_scalar_len(a->dmin);
		dfor->delta_residual_base = a->dmin;
	}
	dfor->S = a->S;
	dfor->zigzag = a->use_zigzag;
	dfor->W_delta_residuals = (uint8) W_delta_residuals;
	return 5 + aic_block_header_scalar_len(dfor->bases_min) +
		   (int32) fl_result_bytes(a->S, dfor->W_bases, T) + delta_residual_base_len +
		   (int32) fl_result_bytes(n - a->S, (uint8) W_delta_residuals, T);
}

/*
 * aic_dict_cap : calculate the largest K (number of dict keys) at
 * which DICT can win against the best_cost using the lower bound of
 * the DICT record size at n and W, and considering the implied
 * cardinality based on W.
 */
static inline int32
aic_dict_cap(uint32 n, uint8 W, int32 best_cost)
{
	int32 budget = best_cost - 5 - (int32) ((2 * n + 7) / 8);
	int32 k = AIC_DICT_K_FLOOR;

	if (budget > 0)
	{
		k = (8 * budget) / (int32) W - 1;
		if (k < AIC_DICT_K_FLOOR)
		{
			k = AIC_DICT_K_FLOOR;
		}
	}
	if (W < 31)
	{
		int32 kcap = 1 << (W - 1);

		if (kcap < k)
		{
			k = kcap;
		}
	}
	if (k > AIC_DICT_K_CEIL)
	{
		k = AIC_DICT_K_CEIL;
	}
	return k;
}

/*
 * aic_dict_probe : calculates the exact cost of the DICT storage and
 * also fills the 'keys' and 'idx' results which the encoder will
 * pack into the block output.
 */
static inline int32
aic_dict_probe(uint32 n, const AICAnalysis *a, fl_elem_width_t T, AICAnalyzeResult *r)
{
	const uint64 *residuals = r->residuals;
	const int32 k_cap = aic_dict_cap(n, a->W, r->cost);

	uint64 ht_keys[AIC_DICT_HASH_SIZE];
	int8 ht_idx[AIC_DICT_HASH_SIZE];
	int32 K = 0;
	uint8 w_idx;
	int32 idx_cost;
	bool zz_idx = false;
	int32 dict_cost;
	uint32 i;

	/* build up the 'keys' and 'idx' arrays */
	Assert(a->W >= AIC_DICT_MIN_BITWIDTH);
	memset(ht_idx, 0xFF, sizeof(ht_idx));
	for (i = 0; i < n; ++i)
	{
		uint64 key = residuals[i];
		uint32 h = (uint32) ((key * UINT64CONST(0x9E3779B97F4A7C15)) >> AIC_DICT_HASH_SHIFT) &
				   (AIC_DICT_HASH_SIZE - 1);

		int32 slot = (int32) ht_idx[h];
		int32 miss = (slot < 0) | (ht_keys[h] != key);
		int32 kidx = miss ? K : slot;

		/* note: keys are rewritten unconditionally, because
		 * the branch makes a measurable regression in performance */
		r->dict.keys[K] = key;
		ht_keys[h] = key;
		ht_idx[h] = (int8) kidx;
		r->dict.idx[i] = (uint8) kidx;
		K += miss;

		if (((i & 7) == 7) && (K > k_cap))
		{
			return -1;
		}
	}
	if (K > k_cap)
	{
		return -1;
	}
	Assert(K >= 2);

	/* check which idx storage is tighter, raw or zigzag+delta */
	w_idx = aic_bit_width_u64((uint64) (K - 1));
	idx_cost = (int32) fl_result_bytes(n, w_idx, FL_ELEM_W8);
	if (a->W > AIC_DICT_ZIGZAG_MIN_BITWIDTH && K > AIC_DICT_ZIGZAG_MIN_K)
	{
		uint64 zz_or = 0;
		uint8 w_zz;
		int32 zz_encoded_cost;

		r->dict.idx_zz[0] = 0;
		for (i = 1; i < n; ++i)
		{
			int32 dlt = (int32) r->dict.idx[i] - (int32) r->dict.idx[i - 1];
			uint32 zz = aic_zigzag_encode_uint32((uint32) dlt);
			r->dict.idx_zz[i] = (uint16) zz;
			zz_or |= (uint64) zz;
		}
		w_zz = aic_bit_width_u64(zz_or);
		zz_encoded_cost = (int32) fl_result_bytes(n, w_zz, FL_ELEM_W16);
		if (zz_encoded_cost < idx_cost)
		{
			idx_cost = zz_encoded_cost;
			w_idx = w_zz;
			zz_idx = true;
		}
	}

	dict_cost = 5 + aic_block_header_scalar_len(a->vmin) +
				(int32) fl_result_bytes((uint32) K, a->W, T) + idx_cost;
	if (dict_cost >= r->cost)
	{
		return -1;
	}
	r->dict.K = (uint16) K;
	r->dict.W_idx = w_idx;
	r->dict.zigzag_idxs = zz_idx;
	return dict_cost;
}

/*
 * aic_pfor_search : determines the 'b' bitwidth threshold at which
 * we store the values with FOR. the values over the 'b' threshold
 * are stored as exceptions. the search uses an 8 bit window relative
 * to the highest bit set and calculates how many values hit each of
 * the 8 bits in the window. these bit counters are cumulative, so
 * when a bit is hit, all the bits below are hit too.
 *
 * this limited search window is chosen for performance reasons. this
 * window can do a fast 'SIMD Within A Register' sweep. the loop also
 * aggregates (OR) the bits set below the search window where the values
 * don't fall into the window, so we can tell what is the next bit set
 * below the window.
 *
 * with the above technique, the algorithm tries to determine the 'b'
 * threshold such that the number of exceptions doesn't exceed
 * AIC_PFOR_MAX_EXCEPTIONS and also, it iterates over the possible
 * choices based on the window and the next bit set, such that the
 * encoded cost is minimized.
 *
 * the result's r->pfor values will hold enough details for the encoder
 * to do the encoding pass.
 */
static inline int32
aic_pfor_search(uint32 n, uint8 W, int64 vmin, fl_elem_width_t T, AICAnalyzeResult *r)
{
	const uint64 *residuals = r->residuals;
	int32 hdr = 5 + aic_block_header_scalar_len(vmin);
	int shift = (W > 8) ? W - 8 : 0;
	uint64 low_or = 0;
	uint64 acc = 0;
	uint32 lim = (n < 255) ? n : 255;
	int exact_exc[8];
	uint8 best_b = W;
	int32 best_cost = r->cost;
	int32 best_exc = 0;
	uint8 p;
	uint32 i;
	int k;
	int j;

	/* 8 bit window pass with the accumulation of low_or bits
	 * so we can tell which is the subsequent bit set outside
	 * the window
	 */
	for (i = 0; i < lim; ++i)
	{
		uint8 t = (uint8) (residuals[i] >> shift);
		uint64 ev;
		uint64 od;

		low_or |= t ? 0 : residuals[i];
		t |= t >> 1;
		t |= t >> 2;
		t |= t >> 4;
		ev = t & 0x55;
		od = t & 0xAA;
		acc += ((ev * UINT64CONST(0x0000040010004001)) | (od * UINT64CONST(0x0002000800200080))) &
			   UINT64CONST(0x0101010101010101);
	}

	/* transfer the results of the window loop into the
	 * exact_exc array. */
	for (k = 0; k < 8; k++)
	{
		exact_exc[k] = (int) ((acc >> (8 * k)) & 0xFF);
	}

	/* because the SWAR loop above can only hold 255
	 * values in acc for the each window bit, we need to
	 * add a manual step for the 256th entry. */
	if (n > 255)
	{
		uint8 t = (uint8) (residuals[255] >> shift);

		low_or |= t ? 0 : residuals[255];
		t |= t >> 1;
		t |= t >> 2;
		t |= t >> 4;
		for (k = 0; k < 8; k++)
		{
			exact_exc[k] += (t >> k) & 1;
		}
	}

	/* loop over the window and check which 'b' candidate
	 * has the best cost, considering the full PFOR encoded
	 * block. when we put values into the exception store
	 * we increase its size, so the bit saving in the main
	 * store causes a loss in the exception store. we need
	 * to balance this. */
	for (j = (W - shift) - 1; j >= 0; j--)
	{
		uint8 b = (uint8) (shift + j);
		int32 cand_exc = exact_exc[j];
		int32 current_cost;

		Assert(cand_exc > 0);
		if (cand_exc > AIC_PFOR_MAX_EXCEPTIONS)
		{
			break;
		}
		current_cost = hdr + (int32) fl_result_bytes(n, b, T) + cand_exc +
					   (int32) fl_result_bytes((uint32) cand_exc, (uint8) (W - b), T);
		if (current_cost < best_cost)
		{
			best_cost = current_cost;
			best_b = b;
			best_exc = cand_exc;
		}
	}

	/* if the search window doesn't exhaust the number of exceptions
	 * we can store, and there is a bit gap (no values stored at these
	 * bits) then we can narrow the 'b' bitwidth until we reach the next
	 * bit set below the window. */
	p = aic_bit_width_u64(low_or);
	if (shift > 0 && (int) p < shift && exact_exc[0] <= AIC_PFOR_MAX_EXCEPTIONS)
	{
		int32 c = hdr + (int32) fl_result_bytes(n, p, T) + exact_exc[0] +
				  (int32) fl_result_bytes((uint32) exact_exc[0], (uint8) (W - p), T);

		if (c < best_cost)
		{
			best_cost = c;
			best_b = p;
			best_exc = exact_exc[0];
		}
	}

	if (best_b >= W)
	{
		return -1;
	}

	/* return cost and update PFOR info */
	r->pfor.b = best_b;
	r->pfor.n_exc = (uint8) best_exc;
	r->pfor.W_exc = (uint8) (W - best_b);
	return best_cost;
}

/*
 * aic_decide : decides which codec to use based on the estimated size
 * of the encoded data. initialize the search with the closed form, easy
 * to calculate FOR cost and then move to the slower calculations and try
 * to eliminate the slower PFOR analysis if possible.
 *
 * the DICT search is also constrained by the best cost so far by limiting
 * the number of keys (K) to the maximum that can still win against the
 * best cost.
 */
static inline void
aic_decide(uint32 n, fl_elem_width_t T, const AICAnalysis *a, AICAnalyzeResult *r)
{
	r->type = AIC_BT_FOR;
	r->W = a->W;
	r->vmin = a->vmin;
	r->cost = aic_cost_for(n, a->W, a->vmin, T);

	{
		/* the DFOR costing is unconditional */
		int32 c = aic_cost_dfor(n, a, T, &r->dfor);

		if (c >= 0 && c < r->cost)
		{
			r->type = AIC_BT_DFOR;
			r->cost = c;
		}
	}

	if (a->W >= AIC_DICT_MIN_BITWIDTH)
	{
		int32 c = aic_dict_probe(n, a, T, r);

		if (c >= 0 && c < r->cost)
		{
			r->type = AIC_BT_DICT;
			r->cost = c;
		}
	}
	if (!a->skip_pfor_strong && !a->skip_pfor_main)
	{
		int32 c = aic_pfor_search(n, a->W, a->vmin, T, r);

		if (c >= 0 && c < r->cost)
		{
			r->type = AIC_BT_PFOR;
			r->cost = c;
		}
	}
}

/* clang format would reorder the headers which is not desired here */
/* clang-format off */

#define AIC_TY uint16
#define AIC_STY int16
#include "aic_analyze_impl.h"

#define AIC_TY uint32
#define AIC_STY int32
#include "aic_analyze_impl.h"

#define AIC_TY uint64
#define AIC_STY int64
#include "aic_analyze_impl.h"

/* clang-format on */
