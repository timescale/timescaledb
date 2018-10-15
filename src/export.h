/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_EXPORT_H
#define TIMESCALEDB_EXPORT_H

#include <postgres.h>

#include "config.h"

/* Definitions for symbol exports */

#define TS_CAT(x,y) x ## y
#define TS_EMPTY(x) (TS_CAT(x, 87628) == 87628)

#if defined(_WIN32) && !defined(WIN32)
#define WIN32
#endif

#if !defined(WIN32) && !defined(__CYGWIN__)
#if __GNUC__ >= 4
#if defined(PGDLLEXPORT)
#if TS_EMPTY(PGDLLEXPORT)
/* PGDLLEXPORT is defined but empty. We can safely undef it. */
#undef PGDLLEXPORT
#else
#error "PGDLLEXPORT is already defined"
#endif
#endif							/* defined(PGDLLEXPORT) */
#define PGDLLEXPORT __attribute__ ((visibility ("default")))
#else
#error "Unsupported GNUC version"
#endif							/* __GNUC__ */
#endif

#define TS_FUNCTION_INFO_V1(fn) \
	PGDLLEXPORT Datum fn(PG_FUNCTION_ARGS); \
	PG_FUNCTION_INFO_V1(fn)

#endif							/* TIMESCALEDB_EXPORT_H */
