/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include "export.h"

extern TSDLLEXPORT void ts_debug_point_wait(const char *name, bool blocking);
extern TSDLLEXPORT void ts_debug_point_raise_error_if_enabled(const char *name);
extern TSDLLEXPORT void ts_debug_point_raise_error_oneshot(const char *name);
extern TSDLLEXPORT bool ts_debug_point_is_enabled(const char *name);

#ifdef TS_DEBUG

#define DEBUG_WAITPOINT(NAME) ts_debug_point_wait((NAME), true)
#define DEBUG_RETRY_WAITPOINT(NAME) ts_debug_point_wait((NAME), false)
#define DEBUG_ERROR_INJECTION(NAME) ts_debug_point_raise_error_if_enabled((NAME))
#define DEBUG_ERROR_INJECTION_ONESHOT(NAME) ts_debug_point_raise_error_oneshot((NAME))
/*
 * Predicate variant: true when the named debug point is enabled.  Unlike the
 * error-injection macros this does not ereport, so the caller can branch into
 * a simulated failure path (e.g. an out-of-memory fall-back) and exercise the
 * recovery code rather than aborting the transaction.
 */
#define DEBUG_INJECTION_ENABLED(NAME) ts_debug_point_is_enabled((NAME))

#else

#define DEBUG_WAITPOINT(NAME)
#define DEBUG_RETRY_WAITPOINT(NAME)
#define DEBUG_ERROR_INJECTION(NAME)
#define DEBUG_ERROR_INJECTION_ONESHOT(NAME)
#define DEBUG_INJECTION_ENABLED(NAME) false

#endif
