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
 * Multi-hypertable invalidations information.
 *
 * Information about the hypertable being processed is stored here, including
 * partitioning column type and list of continuous aggregates associated with the
 * hypertable.
 *
 * The type cache entry need to contain the element type, the range type, and
 * the multirange type, which means that it has to be an entry for the
 * multirange type.
 */
typedef struct MultiHypertableInvalidationEntry
{
	/* Key fields */
	int32 hypertable_id;

	/* Value fields */
	Oid dimtype;			  /* Partition column dimension type */
	TypeCacheEntry *typcache; /* Type cache entry for the multirange type of the partition type. */
	List *caggs;			  /* Materialization table ids for associated caggs */
} MultiHypertableInvalidationEntry;

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
typedef struct MultiHypertableInvalidationRangeEntry
{
	int32 materialization_id; /* Materialization table for continuous aggregate */
	TypeCacheEntry *typcache; /* Type cache entry for the multirange type. */
	size_t ranges_alloc;	  /* Number of range entries allocated in array*/
	size_t ranges_count;	  /* Number of range entries in array */
	RangeType **ranges;		  /* Array of pointers to collected invalidation ranges */
	ContinuousAggsBucketFunction *bucket_function;
} MultiHypertableInvalidationRangeEntry;

/*
 * State for multi-hypertable invalidations.
 *
 * This state lives for the duration of the processing, including the entries
 * in the hash tables.
 */
typedef struct MultiHypertableInvalidationState
{
	MemoryContext mcxt; /* Memory context for both hash tables */
	HTAB *hypertables;	/* Cache for information about hypertables being processed */
	HTAB *ranges;		/* Cache with ranges for all continuous aggregates */
	Relation logrel;	/* Materialization invalidation log table being written */
} MultiHypertableInvalidationState;

extern void multi_invalidation_state_init(MultiHypertableInvalidationState *state,
										  MemoryContext mcxt);
extern void multi_invalidation_state_cleanup(MultiHypertableInvalidationState *state);
extern void multi_invalidation_add_range(MultiHypertableInvalidationState *state,
										 const Invalidation *invalidation);
extern void multi_invalidation_write_range_entry(MultiHypertableInvalidationState *state,
												 MultiHypertableInvalidationRangeEntry *entry);
extern void multi_invalidation_write_all(MultiHypertableInvalidationState *state);
extern void multi_invalidation_move_invalidations(MultiHypertableInvalidationState *state);
extern MultiHypertableInvalidationEntry *
multi_invalidation_state_get_or_set_hypertable_entry(MultiHypertableInvalidationState *state,
													 int32 hypertable_id);
