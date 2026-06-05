/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * fastlanes/fastlanes_common.h -- shared macros for the FL tier headers.
 *
 * Two pieces every tier needs:
 *
 *   1. Bit-position helpers (FL_SHIFT, FL_WORD, FL_NWORD, FL_SPLITS,
 *      FL_REM, FL_CUR, FL_CUR_SAFE) -- pure (row, W, T) arithmetic.
 *
 *   2. Cascading row-enumeration macros (FL_P2..FL_P64, FL_U2..FL_U64,
 *      FL_FP2..FL_FP64, FL_FU2..FL_FU64) and their typed "ALL"
 *      wrappers. Each tier binds FL_P1 / FL_U1 / FL_FP1 / FL_FU1 to
 *      its own row body before invoking the per-W generator; C
 *      macros are late-bound so the same cascade expands tier-
 *      specific code under each binding.
 */

#pragma once

#include "fastlanes_types.h"

/*
 * Bit-position helpers -- compile-time when (row, W, T) are constants.
 *
 *   SHIFT  bit offset of this row's first bit within its word
 *   WORD   word index containing this row's first bit
 *   NWORD  word index containing the next row's first bit
 *   SPLITS this row crosses a word boundary
 *   REM    bits spilling into the next word
 *   CUR    bits remaining in the current word
 */

#define FL_SHIFT(row, W, T) (((row) * (W)) % (T))
#define FL_WORD(row, W, T) (((row) * (W)) / (T))
#define FL_NWORD(row, W, T) ((((row) + 1) * (W)) / (T))
#define FL_SPLITS(row, W, T) (FL_NWORD(row, W, T) > FL_WORD(row, W, T))
#define FL_REM(row, W, T) ((((row) + 1) * (W)) % (T))
#define FL_CUR(row, W, T) ((W) -FL_REM(row, W, T))

/* MSVC C4293: keep the intermediate non-negative so the warning checker
 * (which runs before constant folding through the mask) sees a small
 * positive value. Equivalent to ((W - FL_REM) mod T). */
#define FL_CUR_SAFE(row, W, T) (((W) + (T) -FL_REM(row, W, T)) & ((unsigned) (T) -1u))

/* Doubling macro expansion cascades. */
#define FL_P2(T, S, W, b) FL_P1(T, S, W, (b)) FL_P1(T, S, W, (b) + 1)
#define FL_P4(T, S, W, b) FL_P2(T, S, W, (b)) FL_P2(T, S, W, (b) + 2)
#define FL_P8(T, S, W, b) FL_P4(T, S, W, (b)) FL_P4(T, S, W, (b) + 4)
#define FL_P16(T, S, W, b) FL_P8(T, S, W, (b)) FL_P8(T, S, W, (b) + 8)
#define FL_P32(T, S, W, b) FL_P16(T, S, W, (b)) FL_P16(T, S, W, (b) + 16)
#define FL_P64(T, S, W, b) FL_P32(T, S, W, (b)) FL_P32(T, S, W, (b) + 32)

#define FL_PACK_ALL_8(S, W) FL_P8(8, S, W, 0)
#define FL_PACK_ALL_16(S, W) FL_P16(16, S, W, 0)
#define FL_PACK_ALL_32(S, W) FL_P32(32, S, W, 0)
#define FL_PACK_ALL_64(S, W) FL_P64(64, S, W, 0)

/* Doubling macro expansion cascades. */
#define FL_U2(TYPE, T, S, W, b) FL_U1(TYPE, T, S, W, (b)) FL_U1(TYPE, T, S, W, (b) + 1)
#define FL_U4(TYPE, T, S, W, b) FL_U2(TYPE, T, S, W, (b)) FL_U2(TYPE, T, S, W, (b) + 2)
#define FL_U8(TYPE, T, S, W, b) FL_U4(TYPE, T, S, W, (b)) FL_U4(TYPE, T, S, W, (b) + 4)
#define FL_U16(TYPE, T, S, W, b) FL_U8(TYPE, T, S, W, (b)) FL_U8(TYPE, T, S, W, (b) + 8)
#define FL_U32(TYPE, T, S, W, b) FL_U16(TYPE, T, S, W, (b)) FL_U16(TYPE, T, S, W, (b) + 16)
#define FL_U64(TYPE, T, S, W, b) FL_U32(TYPE, T, S, W, (b)) FL_U32(TYPE, T, S, W, (b) + 32)

#define FL_UNPACK_ALL_8(TYPE, S, W) FL_U8(TYPE, 8, S, W, 0)
#define FL_UNPACK_ALL_16(TYPE, S, W) FL_U16(TYPE, 16, S, W, 0)
#define FL_UNPACK_ALL_32(TYPE, S, W) FL_U32(TYPE, 32, S, W, 0)
#define FL_UNPACK_ALL_64(TYPE, S, W) FL_U64(TYPE, 64, S, W, 0)

/* Doubling macro expansion cascades. */
#define FL_FP2(T, S, W, b) FL_FP1(T, S, W, (b)) FL_FP1(T, S, W, (b) + 1)
#define FL_FP4(T, S, W, b) FL_FP2(T, S, W, (b)) FL_FP2(T, S, W, (b) + 2)
#define FL_FP8(T, S, W, b) FL_FP4(T, S, W, (b)) FL_FP4(T, S, W, (b) + 4)
#define FL_FP16(T, S, W, b) FL_FP8(T, S, W, (b)) FL_FP8(T, S, W, (b) + 8)
#define FL_FP32(T, S, W, b) FL_FP16(T, S, W, (b)) FL_FP16(T, S, W, (b) + 16)
#define FL_FP64(T, S, W, b) FL_FP32(T, S, W, (b)) FL_FP32(T, S, W, (b) + 32)

#define FL_FFOR_PACK_ALL_8(S, W) FL_FP8(8, S, W, 0)
#define FL_FFOR_PACK_ALL_16(S, W) FL_FP16(16, S, W, 0)
#define FL_FFOR_PACK_ALL_32(S, W) FL_FP32(32, S, W, 0)
#define FL_FFOR_PACK_ALL_64(S, W) FL_FP64(64, S, W, 0)

/* Doubling macro expansion cascades. */
#define FL_FU2(TYPE, T, S, W, b) FL_FU1(TYPE, T, S, W, (b)) FL_FU1(TYPE, T, S, W, (b) + 1)
#define FL_FU4(TYPE, T, S, W, b) FL_FU2(TYPE, T, S, W, (b)) FL_FU2(TYPE, T, S, W, (b) + 2)
#define FL_FU8(TYPE, T, S, W, b) FL_FU4(TYPE, T, S, W, (b)) FL_FU4(TYPE, T, S, W, (b) + 4)
#define FL_FU16(TYPE, T, S, W, b) FL_FU8(TYPE, T, S, W, (b)) FL_FU8(TYPE, T, S, W, (b) + 8)
#define FL_FU32(TYPE, T, S, W, b) FL_FU16(TYPE, T, S, W, (b)) FL_FU16(TYPE, T, S, W, (b) + 16)
#define FL_FU64(TYPE, T, S, W, b) FL_FU32(TYPE, T, S, W, (b)) FL_FU32(TYPE, T, S, W, (b) + 32)

#define FL_FFOR_UNPACK_ALL_8(TYPE, S, W) FL_FU8(TYPE, 8, S, W, 0)
#define FL_FFOR_UNPACK_ALL_16(TYPE, S, W) FL_FU16(TYPE, 16, S, W, 0)
#define FL_FFOR_UNPACK_ALL_32(TYPE, S, W) FL_FU32(TYPE, 32, S, W, 0)
#define FL_FFOR_UNPACK_ALL_64(TYPE, S, W) FL_FU64(TYPE, 64, S, W, 0)
