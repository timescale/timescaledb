/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*-------------------------------------------------------------------------
 *
 * erand48.c from PostgreSQL < 15. See the comments in compat.h.
 *
 * This file supplies pg_erand48() and related functions, which except
 * for the names are just like the POSIX-standard erand48() family.
 * (We don't supply the full set though, only the ones we have found use
 * for in Postgres.  In particular, we do *not* implement lcong48(), so
 * that there is no need for the multiplier and addend to be variable.)
 *
 * We used to test for an operating system version rather than
 * unconditionally using our own, but (1) some versions of Cygwin have a
 * buggy erand48() that always returns zero and (2) as of 2011, glibc's
 * erand48() is strangely coded to be almost-but-not-quite thread-safe,
 * which doesn't matter for the backend but is important for pgbench.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 *
 * Portions Copyright (c) 1993 Martin Birgmeier
 * All rights reserved.
 *
 * You may redistribute unmodified or modified versions of this source
 * code provided that the above copyright notice and this and the
 * following conditions are retained.
 *
 * This software is provided ``as is'', and comes with no warranties
 * of any kind. I shall in no event be liable for anything that happens
 * to anyone/anything when using this software.
 *
 * IDENTIFICATION
 *	  src/port/erand48.c
 *
 *-------------------------------------------------------------------------
 */

#include "c.h"

#include <math.h>
#include "compat.h"

/* These values are specified by POSIX */
#define RAND48_MULT UINT64CONST(0x0005deece66d)
#define RAND48_ADD UINT64CONST(0x000b)

/* POSIX specifies 0x330e's use in srand48, but the other bits are arbitrary */
#define RAND48_SEED_0 (0x330e)
#define RAND48_SEED_1 (0xabcd)
#define RAND48_SEED_2 (0x1234)

static unsigned short _rand48_seed[3] = { RAND48_SEED_0, RAND48_SEED_1, RAND48_SEED_2 };

/*
 * Advance the 48-bit value stored in xseed[] to the next "random" number.
 *
 * Also returns the value of that number --- without masking it to 48 bits.
 * If caller uses the result, it must mask off the bits it wants.
 */
static uint64
_dorand48(unsigned short xseed[3])
{
	/*
	 * We do the arithmetic in uint64; any type wider than 48 bits would work.
	 */
	uint64 in;
	uint64 out;

	in = (uint64) xseed[2] << 32 | (uint64) xseed[1] << 16 | (uint64) xseed[0];

	out = in * RAND48_MULT + RAND48_ADD;

	xseed[0] = out & 0xFFFF;
	xseed[1] = (out >> 16) & 0xFFFF;
	xseed[2] = (out >> 32) & 0xFFFF;

	return out;
}

/*
 * Generate a random floating-point value using caller-supplied state.
 * Values are uniformly distributed over the interval [0.0, 1.0).
 */
double
ts_erand48(unsigned short xseed[3])
{
	uint64 x = _dorand48(xseed);

	return ldexp((double) (x & UINT64CONST(0xFFFFFFFFFFFF)), -48);
}

/*
 * Generate a random non-negative integral value using internal state.
 * Values are uniformly distributed over the interval [0, 2^31).
 */
long
ts_lrand48(void)
{
	uint64 x = _dorand48(_rand48_seed);

	return (x >> 17) & UINT64CONST(0x7FFFFFFF);
}

/*
 * Generate a random signed integral value using caller-supplied state.
 * Values are uniformly distributed over the interval [-2^31, 2^31).
 */
long
ts_jrand48(unsigned short xseed[3])
{
	uint64 x = _dorand48(xseed);

	return (int32)((x >> 16) & UINT64CONST(0xFFFFFFFF));
}

/*
 * Initialize the internal state using the given seed.
 *
 * Per POSIX, this uses only 32 bits from "seed" even if "long" is wider.
 * Hence, the set of possible seed values is smaller than it could be.
 * Better practice is to use caller-supplied state and initialize it with
 * random bits obtained from a high-quality source of random bits.
 *
 * Note: POSIX specifies a function seed48() that allows all 48 bits
 * of the internal state to be set, but we don't currently support that.
 */
void
ts_srand48(long seed)
{
	_rand48_seed[0] = RAND48_SEED_0;
	_rand48_seed[1] = (unsigned short) seed;
	_rand48_seed[2] = (unsigned short) (seed >> 16);
}
