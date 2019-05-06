/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_COMPRESSION_UTILS_H
#define TIMESCALEDB_TSL_COMPRESSION_UTILS_H

#include <postgres.h>

static inline uint32
float_get_bits(float in)
{
	uint32 out;
	StaticAssertStmt(sizeof(float) == sizeof(uint32), "float is not IEEE double wide float");
	/* yes, this is the correct way to extract the bits of a floating point number in C */
	memcpy(&out, &in, sizeof(uint32));
	return out;
}

static inline float
bits_get_float(uint32 bits)
{
	float out;
	StaticAssertStmt(sizeof(float) == sizeof(uint32), "float is not IEEE double wide float");
	/* yes, this is the correct way to extract the bits of a floating point number in C */
	memcpy(&out, &bits, sizeof(uint32));
	return out;
}

static inline uint64
double_get_bits(double in)
{
	uint64 out;
	StaticAssertStmt(sizeof(uint64) == sizeof(double), "double is not IEEE double wide float");
	/* yes, this is the correct way to extract the bits of a floating point number in C */
	memcpy(&out, &in, sizeof(uint64));
	return out;
}

static inline double
bits_get_double(uint64 bits)
{
	double out;
	StaticAssertStmt(sizeof(uint64) == sizeof(double), "double is not IEEE double wide float");
	/* yes, this is the correct way to extract the bits of a floating point number in C */
	memcpy(&out, &bits, sizeof(double));
	return out;
}

#endif
