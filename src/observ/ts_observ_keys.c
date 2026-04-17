/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <string.h>
#include "ts_observ_keys.h"

/*
 * Note that the key IDs in the registry must be the same as their position in the
 * key dictionary array below. This is for efficient lookup of the key definitions by ID.
 */
static const TsObservKeyDef key_dictionary[] = {
	{ TS_KEY_UNUSED, "unused", "Unused key" },
	{ TS_KEY_EVENT_TYPE, "event_type", "Type of event" },
	{ TS_KEY_RELID, "relid", "Relation ID" },
	{ TS_KEY_COMPRESS_RELID, "compress_relid", "Compressed Relation ID" },
	{ TS_KEY_AGG_COUNT, "agg_count", "Number of aggregated observations" },
	{ TS_KEY_AGG_START, "agg_start", "Aggregation start time" },
	{ TS_KEY_AGG_END, "agg_end", "Aggregation end time" },
	{ TS_KEY_BATCH_ROWS_SUM, "batch_rows_sum", "Sum of rows per batch" },
	{ TS_KEY_BATCH_ROWS_MIN, "batch_rows_min", "Min rows per batch" },
	{ TS_KEY_BATCH_ROWS_MAX, "batch_rows_max", "Max rows per batch" },
	{ TS_KEY_BATCH_ROWS_AVG, "batch_rows_avg", "Average rows per batch" },
	{ TS_KEY_BATCH_ROWS_STDDEV, "batch_rows_stddev", "Standard deviation of rows per batch" },
	{ TS_KEY_BATCH_BYTES_SUM, "batch_bytes_sum", "Sum of bytes per batch" },
	{ TS_KEY_BATCH_BYTES_MIN, "batch_bytes_min", "Min bytes per batch" },
	{ TS_KEY_BATCH_BYTES_MAX, "batch_bytes_max", "Max bytes per batch" },
	{ TS_KEY_BATCH_BYTES_AVG, "batch_bytes_avg", "Average bytes per batch" },
	{ TS_KEY_BATCH_BYTES_STDDEV, "batch_bytes_stddev", "Standard deviation of bytes per batch" },
	{ TS_KEY_HYPERTABLE_ID, "hypertable_id", "Hypertable ID" },
	{ TS_KEY_IS_COLUMNAR_SCAN, "is_columnar_scan", "Whether the event is from a columnar scan" },
	{ TS_KEY_CMD_TYPE, "cmd_type", "Command type (the value of the CmdType enum)" },
	{ TS_KEY_BATCHES_DELETED,
	  "batches_deleted",
	  "Number of batches deleted without decompression" },
	{ TS_KEY_BATCHES_DECOMPRESSED, "batches_decompressed", "Number of batches decompressed" },
	{ TS_KEY_BATCHES_SCANNED, "batches_scanned", "Number of batches scanned" },
	{ TS_KEY_BATCHES_CHECKED_BY_BLOOM,
	  "batches_checked_by_bloom",
	  "Number of batches checked by bloom filter" },
	{ TS_KEY_BATCHES_PRUNED_BY_BLOOM,
	  "batches_pruned_by_bloom",
	  "Number of batches pruned by bloom filter" },
	{ TS_KEY_BATCHES_WITHOUT_BLOOM,
	  "batches_without_bloom",
	  "Number of batches without bloom filter" },
	{ TS_KEY_BLOOM_FALSE_POSITIVES,
	  "bloom_false_positives",
	  "Number of bloom filter false positives" },
	{ TS_KEY_DECOMPRESSED_TUPLES, "decompressed_tuples", "Number of tuples decompressed" },
	{ TS_KEY_DELETED_TUPLES, "deleted_tuples", "Number of tuples deleted by direct batch delete" },
	{ TS_KEY_BATCHES_FILTERED_COMPRESSED,
	  "batches_filtered_compressed",
	  "Number of batches filtered by compression" },
	{ TS_KEY_BATCHES_FILTERED_DECOMPRESSED,
	  "batches_filtered_decompressed",
	  "Number of batches filtered by decompression" },
};

#define KEY_DICT_SIZE (sizeof(key_dictionary) / sizeof(key_dictionary[0]))
StaticAssertDecl(KEY_DICT_SIZE <= TS_OBSERV_MAX_KEYS,
				 "Key dictionary size exceeds maximum key ID space");

const TsObservKeyDef *
ts_observ_key_by_id(uint16 key_id)
{
	if (key_id >= KEY_DICT_SIZE)
		return NULL;

	const TsObservKeyDef *def = &key_dictionary[key_id];
	/* Ensure the key_id matches the dictionary position */
	Assert(def->key_id == key_id);
	return def;
}

const TsObservKeyDef *
ts_observ_key_by_name(const char *name, int namelen)
{
	for (int i = 0; i < (int) KEY_DICT_SIZE; i++)
	{
		if (strlen(key_dictionary[i].name) == (size_t) namelen &&
			memcmp(key_dictionary[i].name, name, namelen) == 0)
			return &key_dictionary[i];
	}
	return NULL;
}

uint16
ts_observ_key_name_to_id(const char *name, int namelen)
{
	const TsObservKeyDef *def = ts_observ_key_by_name(name, namelen);
	if (def != NULL)
		return def->key_id;
	return 0;
}

int
ts_observ_key_count(void)
{
	return (int) KEY_DICT_SIZE;
}
