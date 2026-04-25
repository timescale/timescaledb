/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include "ts_observ_defs.h"

/*
 * Event API: write multiple KV pairs as one event (shared VTS).
 *
 * All tuples in the `kvs` array are written as a single logical event:
 * they share the same virtual timestamp. If the observability feature is
 * disabled or the segment is not available, the call is a silent no-op.
 */
extern TSDLLEXPORT void ts_observ_emit(const TsObservKV *kvs, int count);
