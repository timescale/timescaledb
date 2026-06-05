/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * fastlanes/fastlanes_types.h -- shared enum types for the FastLanes pack/unpack layer.
 *
 *   fl_elem_width_t  -- input element width T (8, 16, 32, 64 bits)
 *   fl_tier_width_t  -- selected tier (FL8..FL256)
 */
#pragma once

/* Element width T in bits. */
typedef enum
{
	FL_ELEM_W8 = 8,
	FL_ELEM_W16 = 16,
	FL_ELEM_W32 = 32,
	FL_ELEM_W64 = 64,
} fl_elem_width_t;

/* Selected FL tier. */
typedef enum
{
	FL_TIER_W8 = 8,
	FL_TIER_W16 = 16,
	FL_TIER_W32 = 32,
	FL_TIER_W64 = 64,
	FL_TIER_W128 = 128,
	FL_TIER_W256 = 256,
} fl_tier_width_t;
