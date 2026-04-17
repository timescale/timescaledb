/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include "export.h"
#include <port/atomics.h>

/*
 * The fundamental unit of storage: 16 bytes.
 *
 * id layout: [virtual_timestamp(54 bits) | key_id(10 bits)]
 * virtual_timestamp: [us_offset(48 bits) | sub_us_counter(6 bits)]
 *
 * The timestamp has a double role, it provides the link between the metric values and
 * also provides an anchor in wall time. The timestamp itself is an offset in microsecond
 * from the initialization of the observability system. Because there is a possibility of
 * having multiple observations in the same microsecond, we have a sub-microsecond counter to
 * differentiate between them. If the 6 bits is not enough, we will drift the microseconds, so
 * there is a possibility to have a slight inaccuracy in the timestamp.
 *
 * The key_id is an index into the key registry, which maps to the actual metric name and labels.
 *
 * See ts_observ_keys.[ch] for more details.
 */
typedef struct TsObservTuple
{
	uint64 id;
	double value;
} TsObservTuple;

StaticAssertDecl(sizeof(TsObservTuple) == 16, "TsObservTuple must be 16 bytes");

/* Bit layout constants */
#define TS_OBSERV_KEY_BITS 10
#define TS_OBSERV_VTS_BITS 54
#define TS_OBSERV_SUB_US_BITS 6

#define TS_OBSERV_KEY_MASK ((UINT64CONST(1) << TS_OBSERV_KEY_BITS) - 1)
#define TS_OBSERV_SUB_US_MASK ((UINT64CONST(1) << TS_OBSERV_SUB_US_BITS) - 1)

/* Construct / decompose the id field */
static inline uint64
ts_observ_make_id(uint64 virtual_ts, uint16 key_id)
{
	return (virtual_ts << TS_OBSERV_KEY_BITS) | ((uint64) key_id & TS_OBSERV_KEY_MASK);
}

static inline uint64
ts_observ_get_virtual_ts(uint64 id)
{
	return id >> TS_OBSERV_KEY_BITS;
}

static inline uint16
ts_observ_get_key_id(uint64 id)
{
	return (uint16) (id & TS_OBSERV_KEY_MASK);
}

static inline uint64
ts_observ_virtual_ts_to_us(uint64 virtual_ts)
{
	return virtual_ts >> TS_OBSERV_SUB_US_BITS;
}

/* KV pair for the Event API (key_id + value, without timestamp) */
typedef struct TsObservKV
{
	uint16 key_id;
	double value;
} TsObservKV;
