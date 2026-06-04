/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * fastlanes/fastlanes_template_macros.h -- shared macros for fastlanes_tier_pack_impl.h
 * and fastlanes_tier_ffor_impl.h.
 *
 * On top of fastlanes_common.h this defines:
 *
 *   1. The tier-agnostic row-body macros (FL_PACK_ROW, FL_PACK_FLUSH,
 *      FL_UNPACK_ROW, FL_UNPACK_LOAD and their FFOR counterparts).
 *
 *   2. W-iteration macros (FL_W_LIST_8/16/32/64) that let the
 *      templates enumerate W=1..T via a single token.
 */
#pragma once

#include "fastlanes_common.h"

/*
 * All macros reference local names (in, out, mask, tmp, src, lane,
 * packed, base) belonging to the per-W function they're expanded
 * into.
 */

#define FL_PACK_ROW(T, S, W, row)                                                                  \
	src = in[(row) * (S) + lane] & mask;                                                           \
	tmp |= src << FL_SHIFT(row, W, T);                                                             \
	if (FL_SPLITS(row, W, T))                                                                      \
	{                                                                                              \
		out[FL_WORD(row, W, T) * (S) + lane] = tmp;                                                \
		tmp = (FL_REM(row, W, T) != 0) ? (src >> FL_CUR_SAFE(row, W, T)) : 0;                      \
	}

#define FL_PACK_FLUSH(T, S, W, T_ROWS)                                                             \
	if (!FL_SPLITS((T_ROWS) -1, W, T))                                                             \
	{                                                                                              \
		out[FL_WORD((T_ROWS) -1, W, T) * (S) + lane] = tmp;                                        \
	}

#define FL_UNPACK_LOAD(T, S, W, row)                                                               \
	if (FL_WORD(row, W, T) > FL_WORD((row) -1, W, T) && FL_REM((row) -1, W, T) == 0)               \
	{                                                                                              \
		src = packed[FL_WORD(row, W, T) * (S) + lane];                                             \
	}

#define FL_UNPACK_ROW(TYPE, T, S, W, row)                                                          \
	if (FL_SPLITS(row, W, T))                                                                      \
	{                                                                                              \
		tmp = (src >> FL_SHIFT(row, W, T)) &                                                       \
			  ((FL_CUR(row, W, T) == (T)) ? (TYPE) (~(TYPE) 0) :                                   \
											(((TYPE) 1 << FL_CUR_SAFE(row, W, T)) - 1));           \
		if (FL_REM(row, W, T) != 0)                                                                \
		{                                                                                          \
			src = packed[FL_NWORD(row, W, T) * (S) + lane];                                        \
			tmp |= (src & (((TYPE) 1 << FL_REM(row, W, T)) - 1)) << FL_CUR_SAFE(row, W, T);        \
		}                                                                                          \
	}                                                                                              \
	else                                                                                           \
	{                                                                                              \
		tmp = (src >> FL_SHIFT(row, W, T)) & mask;                                                 \
	}                                                                                              \
	out[(row) * (S) + lane] = tmp;

#define FL_FFOR_PACK_ROW(T, S, W, row)                                                             \
	src = (in[(row) * (S) + lane] - base) & mask;                                                  \
	tmp |= src << FL_SHIFT(row, W, T);                                                             \
	if (FL_SPLITS(row, W, T))                                                                      \
	{                                                                                              \
		out[FL_WORD(row, W, T) * (S) + lane] = tmp;                                                \
		tmp = (FL_REM(row, W, T) != 0) ? (src >> FL_CUR_SAFE(row, W, T)) : 0;                      \
	}

#define FL_FFOR_UNPACK_ROW(TYPE, T, S, W, row)                                                     \
	if (FL_SPLITS(row, W, T))                                                                      \
	{                                                                                              \
		tmp = (src >> FL_SHIFT(row, W, T)) &                                                       \
			  ((FL_CUR(row, W, T) == (T)) ? (TYPE) (~(TYPE) 0) :                                   \
											(((TYPE) 1 << FL_CUR_SAFE(row, W, T)) - 1));           \
		if (FL_REM(row, W, T) != 0)                                                                \
		{                                                                                          \
			src = packed[FL_NWORD(row, W, T) * (S) + lane];                                        \
			tmp |= (src & (((TYPE) 1 << FL_REM(row, W, T)) - 1)) << FL_CUR_SAFE(row, W, T);        \
		}                                                                                          \
	}                                                                                              \
	else                                                                                           \
	{                                                                                              \
		tmp = (src >> FL_SHIFT(row, W, T)) & mask;                                                 \
	}                                                                                              \
	out[(row) * (S) + lane] = tmp + base;

/* (2) W-iteration X-macros */
/* clang-format off */

#define FL_W_LIST_8(X)                                                                             \
	X(1) X(2) X(3) X(4) X(5) X(6) X(7) X(8)

/* Flat (no recursion) so MSVC's preprocessor doesn't stack four nested
 * FL_W_LIST_N expansion frames while iterating the kernel definers. */

#define FL_W_LIST_16(X)                                                                            \
	X(1)  X(2)  X(3)  X(4)  X(5)  X(6)  X(7)  X(8)                                                 \
	X(9)  X(10) X(11) X(12) X(13) X(14) X(15) X(16)

#define FL_W_LIST_32(X)                                                                            \
	X(1)  X(2)  X(3)  X(4)  X(5)  X(6)  X(7)  X(8)                                                 \
	X(9)  X(10) X(11) X(12) X(13) X(14) X(15) X(16)                                                \
	X(17) X(18) X(19) X(20) X(21) X(22) X(23) X(24)                                                \
	X(25) X(26) X(27) X(28) X(29) X(30) X(31) X(32)

#define FL_W_LIST_64(X)                                                                            \
	X(1)  X(2)  X(3)  X(4)  X(5)  X(6)  X(7)  X(8)                                                 \
	X(9)  X(10) X(11) X(12) X(13) X(14) X(15) X(16)                                                \
	X(17) X(18) X(19) X(20) X(21) X(22) X(23) X(24)                                                \
	X(25) X(26) X(27) X(28) X(29) X(30) X(31) X(32)                                                \
	X(33) X(34) X(35) X(36) X(37) X(38) X(39) X(40)                                                \
	X(41) X(42) X(43) X(44) X(45) X(46) X(47) X(48)                                                \
	X(49) X(50) X(51) X(52) X(53) X(54) X(55) X(56)                                                \
	X(57) X(58) X(59) X(60) X(61) X(62) X(63) X(64)

/* clang-format on */
