/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <access/htup_details.h>
#include <executor/execdesc.h>
#include <executor/tuptable.h>
#include <nodes/parsenodes.h>
#include <nodes/pathnodes.h>
#include <tcop/dest.h>
#include <utils/timestamp.h>

#include "hypertable.h"

extern bool ts_should_deferred_chunk_scan(const Query *query, const Hypertable *ht);
extern void ts_deferred_chunk_scan_add_path(PlannerInfo *root, RelOptInfo *rel,
											const Hypertable *ht);
extern void _deferred_chunk_scan_init(void);

/*
 * DeferredChunkAppendState is the execution state for a deferred chunk append scan.
 *
 * This is exported in the header to make it easier for ts_stat_statements to
 * access the chunks_scanned counter.
 */
typedef struct DeferredChunkAppendState
{
	CustomScanState css;

	/* Do not change the position of chunks_scanned, it is used by ts_stat_statements */
	int chunks_scanned; /* chunks opened total */

	bool ordered;
	bool descending;
	int push_limit;
	char *where_clause;
	char *order_by;

	int num_fetch_attnos;
	AttrNumber *fetch_attnos;

	int32 hypertable_id;
	int32 primary_dimension_id;
	int64 last_range_start;			/* ordered: range_start of the last slice returned */
	TimestampTz last_creation_time; /* unordered: creation_time of the last chunk returned */
	int32 last_id;					/* unordered: id of the last chunk (creation_time tiebreak) */
	bool have_last;					/* whether the last_* resume key is set */

	int batch_size;			  /* rows per ExecutorRun batch */
	QueryDesc *cur_qd;		  /* executor for the current chunk, or NULL */
	List *chunk_qds;		  /* under ANALYZE, every visited chunk's executor, kept
							   * alive so EXPLAIN can print its actual plan */
	DestReceiver *dest;		  /* receiver that copies fetched rows into chunk_mcxt */
	MemoryContext chunk_mcxt; /* holds the current batch's tuples */
	HeapTuple *cur_tuples;	  /* current batch (batch_size long), in chunk_mcxt */
	TupleDesc cur_tupdesc;	  /* per-chunk query row type (node-lifetime copy) */
	uint64 cur_nrows;
	uint64 cur_row;
	TupleTableSlot *scan_slot; /* virtual tuple in hypertable row type, before projection */
} DeferredChunkAppendState;
