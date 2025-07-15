/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#pragma once

#include <postgres.h>

#include <utils/multirangetypes.h>
#include <utils/typcache.h>

#include "continuous_aggs/invalidation.h"
#include "ts_catalog/continuous_agg.h"

/*
 * Multi invalidations information.
 *
 * Information about the hypertable being processed is stored here, including
 * partitioning column type and list of continuous aggregates associated with the
 * hypertable.
 */
typedef struct MultiInvalidationEntry
{
	/* Key fields */
	int32 hypertable_id;

	/* Value fields */
	Oid dimtype; /* Partition column dimension type */
	List *caggs; /* Materialization table ids for associated caggs */
} MultiInvalidationEntry;

/*
 * Multi-hypertable invalidation ranges for a continuous aggregate.
 *
 * Information necessary for building the multiranges for the continuous
 * aggregate is stored here, including the bucket function and information
 * about the continuous aggregate.
 *
 * Note that the ranges are stored in internal time type (microseconds since
 * Epoch). This is what we write to the materialization log.
 *
 * The type cache entry need to contain the element type, the range type, and
 * the multirange type, which means that it has to be an entry for the
 * multirange type.
 *
 * Note that range types are varlen objects, so we have to store an array of
 * pointers to range types, not an array of range types.
 */
typedef struct MultiInvalidationRangeEntry
{
	/* Key fields */
	int32 materialization_id; /* Materialization table for continuous aggregate */

	/* Value fields */
	int ranges_alloc;	   /* Number of range entries allocated in array */
	int ranges_count;	   /* Number of range entries in array */
	RangeType **ranges;	   /* Array of pointers to collected invalidation ranges */
	MemoryContext context; /* Memory context for the ranges in this entry */
	ContinuousAggBucketFunction *bucket_function;
} MultiInvalidationRangeEntry;

/*
 * State for multi-hypertable invalidations.
 *
 * This state lives for the duration of the processing, including the entries
 * in the hash tables.
 *
 * When the high work memory limit is exceeded, range entries will get written
 * to the materialization invalidation log until we get below the low memory
 * limit after which it will proceed to add entries.
 *
 * The reason for a high and low working memory limit is to avoid
 * flip-flopping between writing to materialization log and adding ranges when
 * we're close to the high working memory mark.
 *
 * The type cache entry need to contain the element type, the range type, and
 * the multirange type, which means that it has to be an entry for the
 * multirange type.
 */
typedef struct MultiInvalidationState
{
	MemoryContext hash_context; /* Memory context for stored data */
	MemoryContext work_context; /* Memory context for short-lived processing data */
	TypeCacheEntry *typecache;	/* Typecache entry for multirange type */
	Size high_work_mem;			/* High limit on the working memory (in bytes) */
	Size low_work_mem;			/* Low limit on the working memory (in bytes) */
	HTAB *hypertables;			/* Cache for information about hypertables being processed */
	HTAB *ranges;				/* Cache with ranges for all continuous aggregates */
	Relation logrel;			/* Materialization invalidation log table being written */
	NameData slot_name;			/* Slot name used while processing invalidations */
} MultiInvalidationState;

extern void multi_invalidation_state_init(MultiInvalidationState *state, const char *slot_name,
										  Size low_work_mem, Size high_work_mem);
extern void multi_invalidation_state_cleanup(MultiInvalidationState *state);
extern void multi_invalidation_flush_ranges(MultiInvalidationState *state);
extern void multi_invalidation_range_add(MultiInvalidationState *state,
										 const Invalidation *invalidation);
extern void multi_invalidation_range_write_all(MultiInvalidationState *state);
extern void multi_invalidation_move_invalidations(MultiInvalidationState *state);
extern MultiInvalidationEntry *
multi_invalidation_state_hypertable_entry_get_for_update(MultiInvalidationState *state,
														 int32 hypertable_id);
extern void multi_invalidation_process_hypertable_log(List *hypertables);
extern Datum
continuous_agg_process_multi_hypertable_invalidations(PG_FUNCTION_ARGS);
