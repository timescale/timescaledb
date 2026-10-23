/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include "aic_blocks.h"
#include "aic_internal.h"

/*
 * this header contains the declarations for the analyzer code
 * that is responsible for deciding which codec we use to encode
 * a block. (this only runs in NORMAL mode, so RLE and DELTA_RLE
 * is handled earlier. see README.md)
 *
 * `aic_analyze_{uint16,uint32,uint64}(buf, n, T, result)` analyzes
 * the input `buf` that contains `n` elements of the matching signed type
 * and is
 * being encoded with one of the supported fastlanes elem widths
 * `fl_elem_width_t T`. T identifies the input/output
 * element widths, which impacts all encodings through the
 * fastlanes library result format (through the lane widths).
 *
 * the `result`output variable stores the analysis decision and
 * staged data that are byproducts of the analysis. these staged
 * data can be reused in the encoding and thus saves time and
 * effort later.
 */

/* please read the README.md file for more details. */

/* parameters for the DFOR encoder (block type: AIC_BT_DFOR) */
typedef struct AICDforResult
{
	/* delta stride length */
	uint16 S;

	/* the minimum value across all bases */
	int64 bases_min;

	/* the bitwidth of the bases max(base[i] - bases_min) */
	uint8 W_bases;

	/* delta residual packing mode: zigzag or not? */
	bool zigzag;

	/* delta residual stream FOR base */
	int64 delta_residual_base;

	/* delta residual stream bitwidth upper bound */
	uint8 W_delta_residuals;

	/* the pre-calculated stride-S deltas */
	AICIntBuffer deltas;

} AICDforResult;

/* parameters for the DICT encoder (block type: AIC_BT_DICT) */
typedef struct AICDictResult
{
	/* the number of dictionary keys */
	uint16 K;

	/* the bitwidth of the index entries */
	uint8 W_idx;

	/* is the indexes cheaper to store as a zigzag delta stream? */
	bool zigzag_idxs;

	/* keys are stored as (value[i]-vmin) called residuals, in insertion order.
	 * 'vmin' comes from the parent structure: AICAnalyzeResult */
	uint64 keys[AIC_FIXED_BLOCK_MAX_COUNT];

	/* staged the raw index values is a byproduct of the analysis,
	 * we either use this or the zigzag version depending on the analysis
	 * decision (zigzag_idxs)
	 */
	uint8 idx[AIC_FIXED_BLOCK_MAX_COUNT];

	/* staged zigzag index values, similar to the raw index values (idx) */
	uint16 idx_zz[AIC_FIXED_BLOCK_MAX_COUNT];

} AICDictResult;

/* parameters for the PFOR encoder (block type: AIC_BT_PFOR) */
typedef struct AICPforResult
{
	/* the bit threshold at which we store values in the exception stream */
	uint8 b;

	/* the bitwidth of the exception stream */
	uint8 W_exc;

	/* the number of exceptions */
	uint8 n_exc;

} AICPforResult;

/* the result of the analysis. this contains both the analysis
 * decision of which encoder to use and also values produced
 * during analysis that are useful during the encoding.
 */
typedef struct AICAnalyzeResult
{
	/* the analysis outcome */
	AICBlockType type;

	/* the result bytes for the chosen encoder */
	int32 cost;

	/* the bitwidth of the residual stream (value[i] - vmin) */
	uint8 W;

	/* the minimum value across all input values */
	int64 vmin;

	/* the residual values (value[i] - vmin) */
	uint64 residuals[AIC_FIXED_BLOCK_MAX_COUNT];

	AICDforResult dfor;
	AICDictResult dict;
	AICPforResult pfor;

} AICAnalyzeResult;

extern void aic_analyze_uint16(const int16 *buf, uint32 n, fl_elem_width_t T,
							   AICAnalyzeResult *result);
extern void aic_analyze_uint32(const int32 *buf, uint32 n, fl_elem_width_t T,
							   AICAnalyzeResult *result);
extern void aic_analyze_uint64(const int64 *buf, uint32 n, fl_elem_width_t T,
							   AICAnalyzeResult *result);
