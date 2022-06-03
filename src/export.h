/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_EXPORT_H
#define TIMESCALEDB_EXPORT_H

#include <postgres.h>

#include "config.h"

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
