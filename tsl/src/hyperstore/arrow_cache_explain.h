/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include <nodes/parsenodes.h>

void _arrow_cache_explain_init(void);

struct DecompressCacheStats
{
	size_t hits;
	size_t misses;
	size_t evictions;
	size_t decompressions;
	size_t decompress_calls;
};

extern bool decompress_cache_print;
extern struct DecompressCacheStats decompress_cache_stats;

#define DECOMPRESS_CACHE_STATS_INCREMENT(FIELD)                                                    \
	if (decompress_cache_print)                                                                    \
	decompress_cache_stats.FIELD++

extern bool tsl_process_explain_def(DefElem *opt);
