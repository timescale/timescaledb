/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#pragma once

#include <postgres.h>

#include <replication/logical.h>
#include <utils/hsearch.h>

/*
 * Invalidation log cache entry.
 *
 * This structure is deliberately distinct from InvalidationEntry in
 * invalidation_record.h, even though they contain similar information.
 *
 * The reason is that this structure might add more data that is being tracked
 * as part of a transaction that should not be present in the record being
 * sent back from the plugin. If we need to add fields here, it would affect
 * the format of the records sent back, and we don't what that to happen even
 * accidentally.
 */
typedef struct InvalidationCacheEntry
{
	Oid hypertable_relid;
	int64 lowest_modified;
	int64 highest_modified;
} InvalidationCacheEntry;

/*
 * Context for the invalidation execution.
 */
typedef struct InvalidationsContext
{
	LogicalDecodingContext *ctx;
	ReorderBufferTXN *txn;
} InvalidationsContext;

typedef void ProcessInvalidationFunction(InvalidationCacheEntry *, bool, InvalidationsContext *);

extern HTAB *invalidation_cache_create(MemoryContext mcxt);
extern void invalidation_cache_destroy(HTAB *cache);
extern void invalidation_cache_record_entry(HTAB *cache, Oid hypertable_relid, int64 value);
extern void invalidation_cache_foreach_entry(HTAB *cache, ProcessInvalidationFunction func,
											 InvalidationsContext *args);
