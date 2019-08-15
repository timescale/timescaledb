/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_COMPRESSION_UTILS_H
#define TIMESCALEDB_TSL_COMPRESSION_UTILS_H

extern Datum tsl_compress_chunk(PG_FUNCTION_ARGS);
extern Datum tsl_decompress_chunk(PG_FUNCTION_ARGS);

#endif // TIMESCALEDB_TSL_COMPRESSION_UTILS_H
