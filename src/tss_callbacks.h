/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <stdbool.h>
#include <stdint.h>

/* Keep the legacy declarations below aligned with the external provider. */
#define TSS_CALLBACKS_VAR_NAME "tss_callbacks"
#define TSS_CALLBACKS_VERSION INT64_C(1)

struct BufferUsage;
struct WalUsage;

typedef void (*tss_store_hook_type)(const char *query, int query_location, int query_len,
									uint64_t query_id, uint64_t total_time, uint64_t rows,
									const struct BufferUsage *bufusage,
									const struct WalUsage *walusage);
typedef bool (*tss_enabled_hook_type)(int level);

typedef struct TSSCallbacks
{
	int64_t version_num;
	tss_store_hook_type tss_store_hook;
	tss_enabled_hook_type tss_enabled;
} TSSCallbacks;

extern bool ts_is_tss_enabled(void);
extern void ts_begin_tss_store_callback(void);
extern void ts_end_tss_store_callback(const char *query, int query_location, int query_len,
									  uint64 query_id, uint64 rows);
