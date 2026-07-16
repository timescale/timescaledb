/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * fastlanes/fastlanes_sizing.c -- tier selection + public sizing API.
 *
 * Owns the library's "root" surface that both pack and FFOR call into:
 *
 *   fl_tier_select        chooses the internal tier for an (N, T)
 *                         pair, deterministic and W-independent.
 *   fl_required_bytes     buffer size to allocate (worst-case N).
 *   fl_result_bytes       truncated byte count for a specific N.
 *   fl_alignment          required buffer alignment.
 *   fl_input_count        number of input elements the kernel reads.
 *   fl_input_bytes        byte size of the input buffer.
 *
 * Per-tier sizing helpers (fl{T}_required_bytes, ...) are
 * instantiated as file-local static functions by looping
 * fastlanes_tier_sizing.h over the six tiers. Each public entry
 * point switches on fl_tier_select(N, T) and forwards to the
 * matching static.
 *
 * The pack and FFOR object files link against fl_tier_select only
 * -- one extern call per public fl_pack / fl_unpack /
 * fl_pack_ffor / fl_unpack_ffor invocation, outside any inner
 * loop.
 */

#include <postgres.h>

#include "fastlanes.h"

#define FL_TIER 8
#include "fastlanes_tier_sizing.h"
#undef FL_TIER

#define FL_TIER 16
#include "fastlanes_tier_sizing.h"
#undef FL_TIER

#define FL_TIER 32
#include "fastlanes_tier_sizing.h"
#undef FL_TIER

#define FL_TIER 64
#include "fastlanes_tier_sizing.h"
#undef FL_TIER

#define FL_TIER 128
#include "fastlanes_tier_sizing.h"
#undef FL_TIER

#define FL_TIER 256
#include "fastlanes_tier_sizing.h"
#undef FL_TIER

/* Public sizing entry points */

size_t
fl_required_bytes(uint32 n, uint8 w, fl_elem_width_t t)
{
	switch (fl_tier_select(n, t))
	{
		case FL_TIER_W8:
			return fl8_required_bytes(w, t);
		case FL_TIER_W16:
			return fl16_required_bytes(w, t);
		case FL_TIER_W32:
			return fl32_required_bytes(w, t);
		case FL_TIER_W64:
			return fl64_required_bytes(w, t);
		case FL_TIER_W128:
			return fl128_required_bytes(w, t);
		case FL_TIER_W256:
			return fl256_required_bytes(w, t);
	}
	Assert(false);
	return 0;
}

size_t
fl_alignment(uint32 n, fl_elem_width_t t)
{
	switch (fl_tier_select(n, t))
	{
		case FL_TIER_W8:
			return fl8_alignment();
		case FL_TIER_W16:
			return fl16_alignment();
		case FL_TIER_W32:
			return fl32_alignment();
		case FL_TIER_W64:
			return fl64_alignment();
		case FL_TIER_W128:
			return fl128_alignment();
		case FL_TIER_W256:
			return fl256_alignment();
	}
	Assert(false);
	return 0;
}

uint32
fl_input_count(uint32 n, fl_elem_width_t t)
{
	switch (fl_tier_select(n, t))
	{
		case FL_TIER_W8:
			return fl8_input_count();
		case FL_TIER_W16:
			return fl16_input_count();
		case FL_TIER_W32:
			return fl32_input_count();
		case FL_TIER_W64:
			return fl64_input_count();
		case FL_TIER_W128:
			return fl128_input_count();
		case FL_TIER_W256:
			return fl256_input_count();
	}
	Assert(false);
	return 0;
}

size_t
fl_input_bytes(uint32 n, fl_elem_width_t t)
{
	switch (fl_tier_select(n, t))
	{
		case FL_TIER_W8:
			return fl8_input_bytes(t);
		case FL_TIER_W16:
			return fl16_input_bytes(t);
		case FL_TIER_W32:
			return fl32_input_bytes(t);
		case FL_TIER_W64:
			return fl64_input_bytes(t);
		case FL_TIER_W128:
			return fl128_input_bytes(t);
		case FL_TIER_W256:
			return fl256_input_bytes(t);
	}
	Assert(false);
	return 0;
}
