/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_DEBUG_POINT_H_
#define TIMESCALEDB_DEBUG_POINT_H_

#include <postgres.h>
#include "export.h"

extern TSDLLEXPORT void ts_debug_point_wait(const char *name, bool blocking);
extern TSDLLEXPORT void ts_debug_point_raise_error_if_enabled(const char *name);

#ifdef TS_DEBUG

#define DEBUG_WAITPOINT(NAME) ts_debug_point_wait((NAME), true)
#define DEBUG_RETRY_WAITPOINT(NAME) ts_debug_point_wait((NAME), false)
#define DEBUG_ERROR_INJECTION(NAME) ts_debug_point_raise_error_if_enabled((NAME))

#else

#define DEBUG_WAITPOINT(NAME)
#define DEBUG_RETRY_WAITPOINT(NAME)
#define DEBUG_ERROR_INJECTION(NAME)

#endif

#endif /* TIMESCALEDB_DEBUG_POINT_H_ */
