/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_EXPORT_H
#define TIMESCALEDB_EXPORT_H

#include <postgres.h>

#include "config.h"

/* Definitions for symbol exports */

#if defined(_WIN32) && !defined(WIN32)
#define WIN32
#endif

/*
 * PGDLLEXPORT is defined as en empty macro until PG15.
 * Since PG16, a macro HAVE_VISIBILITY_ATTRIBUTE is defined if the compiler has
 * support for visibility attribute and the PGDLLEXPORT macro is defined as the
 * same. So, skip redefining PGDLLEXPORT if HAVE_VISIBILITY_ATTRIBUTE is defined.
 * If not, undef the empty PGDLLEXPORT macro and redefine it properly.
 */
#if !defined(WIN32) && !defined(__CYGWIN__) && !defined(HAVE_VISIBILITY_ATTRIBUTE)
#if __GNUC__ >= 4
/* PGDLLEXPORT is defined but will be empty. Redefine it. */
#undef PGDLLEXPORT
#define PGDLLEXPORT __attribute__((visibility("default")))
#else
#error "Unsupported GNUC version"
#endif /* __GNUC__ */
#endif

/*
 * On windows, symbols shared across modules have to be marked "export" in the
 * main TimescaleDb module and "import" in the submodule. Since we want to use the
 * same headers, we TSDLLEXPORT functions as "export" in the main module and
 * "import" in submodules.
 */
#ifndef TS_SUBMODULE
/* In the core timescaledb TSDLLEXPORT is export */
#define TSDLLEXPORT PGDLLEXPORT

#elif defined(PGDLLIMPORT)
/* In submodules it works as imports */
#define TSDLLEXPORT PGDLLIMPORT

#else
/* If there is no IMPORT defined, it's a nop */
#define TSDLLEXPORT

#endif

#define TS_FUNCTION_INFO_V1(fn)                                                                    \
	PGDLLEXPORT Datum fn(PG_FUNCTION_ARGS);                                     \
	PG_FUNCTION_INFO_V1(fn)

#endif /* TIMESCALEDB_EXPORT_H */
