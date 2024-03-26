/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifndef COMPRESSION_ARROW_CACHE_EXPLAIN_H_
#define COMPRESSION_ARROW_CACHE_EXPLAIN_H_

#include <postgres.h>

#include <nodes/parsenodes.h>

void _arrow_cache_explain_init(void);

extern bool decompress_cache_print;
extern size_t decompress_cache_hits;
extern size_t decompress_cache_misses;
extern size_t decompress_cache_decompress_count;

extern bool tsl_process_explain_def(DefElem *opt);

#endif /* COMPRESSION_ARROW_CACHE_EXPLAIN_H_ */
