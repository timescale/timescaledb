/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

#include <access/htup.h>
#include <replication/logical.h>
#include <utils/hsearch.h>

/*
 * Hypertable invalidation log cache entry.
 *
 * We use the relid of the hypertable rather than the hypertable id to keep
 * processing fast and also avoid linking dependencies on the timescaledb
 * extension. The translation from hypertable relid to hypertable id will be
 * done on the receiving end before writing the records to the materialization
 * log.
 *
 * The lowest and highest modified values are still in microseconds since the
 * epoch, but the libraries for this do not require any dynamic linking so we
 * can just build the plugin with these files directly.
 */
typedef struct HypertableInvalidationCacheEntry
{
	Oid hypertable_relid;
	int64 lowest_modified_value;
	int64 greatest_modified_value;
} HypertableInvalidationCacheEntry;

/*
 * Context for the invalidation execution.
 */
typedef struct InvalidationsContext
{
	LogicalDecodingContext *ctx;
	ReorderBufferTXN *txn;
	TupleTableSlot *slot;
} InvalidationsContext;

typedef void ProcessInvalidationFunction(HypertableInvalidationCacheEntry *, bool,
										 InvalidationsContext *);

extern HTAB *invalidation_cache_create(MemoryContext mcxt);
extern void invalidation_cache_destroy(HTAB *cache);
extern void invalidation_cache_write_record(HTAB *cache, Oid hypertable_relid, int64 value);
extern void invalidation_cache_foreach_record(HTAB *cache, ProcessInvalidationFunction func,
											  InvalidationsContext *args);
