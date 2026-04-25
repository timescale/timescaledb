/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#pragma once

#include "ts_observ_defs.h"
#include "ts_observ_keys.h"

/*
 * Aggregate implementation for observability metrics. This is a simple, in-memory
 * process-local implementation that supports a fixed number of concurrent aggregates.
 * The implementation doesn't need to be lock-protected since the aggregation happens
 * in a single process and is not concurrent. The API is designed to be explicit, that
 * is the start and finish of an aggregate must be marked by the caller.
 *
 * The implementation supports a maximum of TS_OBSERV_MAX_AGG_SLOTS concurrent aggregates
 * and a maximum of TS_OBSERV_MAX_AGG_DESCRIPTORS aggregate descriptors per aggregate. The
 * descriptors define which keys are being aggregated and how, and the slots hold the actual
 * state of each aggregate. For instance a single `ts_observ_agg_add` call may update multiple
 * aggregates, one min, one max, etc., and each of those aggregates may have multiple
 * descriptors for different keys.
 */

#define TS_OBSERV_MAX_AGG_SLOTS 16384
#define TS_OBSERV_MAX_AGG_DESCRIPTORS 32

/* Aggregation type enum — see ts_observ_agg_describe() */
typedef enum TsObservAggType
{
	TS_OBSERV_AGG_SUM,
	TS_OBSERV_AGG_MIN,
	TS_OBSERV_AGG_MAX,
	TS_OBSERV_AGG_SUMSQ,
	TS_OBSERV_AGG_LAST,
	TS_OBSERV_AGG_FIRST,
	TS_OBSERV_AGG_AVG,
	TS_OBSERV_AGG_STDDEV,
} TsObservAggType;

typedef struct TsObservAggID
{
	uint32 id;
} TsObservAggID;

#define TS_OBSERV_AGG_INVALID_ID ((TsObservAggID){ .id = UINT32_MAX })

extern TSDLLEXPORT TsObservAggID ts_observ_agg_start(const TsObservKV *identity_kvs,
													 int num_identity);
extern TSDLLEXPORT void ts_observ_agg_describe(TsObservAggID id, uint16 key_id,
											   TsObservAggType agg_type);
extern TSDLLEXPORT void ts_observ_agg_add(TsObservAggID id, double value);
extern TSDLLEXPORT void ts_observ_agg_finish(TsObservAggID id);
extern TSDLLEXPORT void ts_observ_agg_cancel(TsObservAggID id);
extern TSDLLEXPORT void ts_observ_agg_add_kv(const TsObservAggID agg_id, const TsObservKV kv);
