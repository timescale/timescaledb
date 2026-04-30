/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include "ts_observ_defs.h"

/*
 * This file defines the key registry. In the in-memory store, we only store the
 * numeric Key ids, and the key registry provides the mapping between the key id and
 * the human readable key name and description. The key registry may grow over time
 * as new metrics are added, but previous keys should never be removed or changed, as
 * they provide compatibility between timescaledb versions.
 */

/* Key dictionary entry */
typedef struct TsObservKeyDef
{
	uint16 key_id;
	const char *name;
	const char *description;
} TsObservKeyDef;

/* Well-known key IDs */
#define TS_KEY_UNUSED 0 /* reserved/unused */
#define TS_KEY_EVENT_TYPE 1
#define TS_KEY_RELID 2
#define TS_KEY_COMPRESS_RELID 3
/* System provided keys for aggregated events */
#define TS_KEY_AGG_COUNT 4
#define TS_KEY_AGG_START 5
#define TS_KEY_AGG_END 6
/* Aggregate output keys for batch rows */
#define TS_KEY_BATCH_ROWS_SUM 7
#define TS_KEY_BATCH_ROWS_MIN 8
#define TS_KEY_BATCH_ROWS_MAX 9
#define TS_KEY_BATCH_ROWS_AVG 10
#define TS_KEY_BATCH_ROWS_STDDEV 11
/* Aggregate output keys for batch sizes */
#define TS_KEY_BATCH_BYTES_SUM 12
#define TS_KEY_BATCH_BYTES_MIN 13
#define TS_KEY_BATCH_BYTES_MAX 14
#define TS_KEY_BATCH_BYTES_AVG 15
#define TS_KEY_BATCH_BYTES_STDDEV 16
/* */
#define TS_KEY_HYPERTABLE_ID 17
#define TS_KEY_IS_COLUMNAR_SCAN 18
#define TS_KEY_CMD_TYPE 19
/* stats from decompress_batches_stats */
#define TS_KEY_BATCHES_DELETED 20
#define TS_KEY_BATCHES_DECOMPRESSED 21
#define TS_KEY_BATCHES_SCANNED 22
#define TS_KEY_BATCHES_CHECKED_BY_BLOOM 23
#define TS_KEY_BATCHES_PRUNED_BY_BLOOM 24
#define TS_KEY_BATCHES_WITHOUT_BLOOM 25
#define TS_KEY_BLOOM_FALSE_POSITIVES 26
#define TS_KEY_DECOMPRESSED_TUPLES 27
#define TS_KEY_DELETED_TUPLES 28
#define TS_KEY_BATCHES_FILTERED_COMPRESSED 29
#define TS_KEY_BATCHES_FILTERED_DECOMPRESSED 30

#define TS_OBSERV_MAX_KEYS 1024 /* 10-bit key_id space */

/* Event types */
#define TS_EVENT_COMPRESS 1
#define TS_EVENT_DECOMPRESS 2
#define TS_EVENT_DML_STATS 3

/* Lookup functions */
extern TSDLLEXPORT const TsObservKeyDef *ts_observ_key_by_id(uint16 key_id);
extern TSDLLEXPORT const TsObservKeyDef *ts_observ_key_by_name(const char *name, int namelen);
extern TSDLLEXPORT uint16 ts_observ_key_name_to_id(const char *name, int namelen);
extern TSDLLEXPORT int ts_observ_key_count(void);
