/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#define TSS_CALLBACKS_VAR_NAME "tss_callbacks"
#define TSS_CALLBACKS_VERSION 1

/* ts_stat_statements -> pgss_store */
typedef void (*tss_store_hook_type)(const char *query, int query_location, int query_len,
									uint64 query_id, uint64 total_time, uint64 rows,
									const BufferUsage *bufusage, const WalUsage *walusage);
/* ts_stat_statements -> pgss_enabled */
typedef bool (*tss_enabled_hook_type)(int level);

/* ts_stat_statements callbacks */
typedef struct TSSCallbacks
{
	uint32_t version_num;
	tss_store_hook_type tss_store_hook;
	tss_enabled_hook_type tss_enabled_hook_type;
} TSSCallbacks;

extern void ts_begin_tss_store_callback(void);
extern void ts_end_tss_store_callback(const char *query, int query_location, int query_len,
									  uint64 query_id, uint64 rows);
