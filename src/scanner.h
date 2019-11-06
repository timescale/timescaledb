/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_SCANNER_H
#define TIMESCALEDB_SCANNER_H

#include <postgres.h>
#include <access/genam.h>
#include <access/relscan.h>
#include <utils/fmgroids.h>
#include <access/heapam.h>
#include <nodes/lockoptions.h>
#include <utils.h>

#include "compat.h"
#include "export.h"

/* Tuple information passed on to handlers when scanning for tuples. */
typedef struct TupleInfo
{
	Relation scanrel;
	/* TODO in PG12+ we should not materialize a HeapTuple unless needed */
	HeapTuple tuple;
	TupleDesc desc;
#if PG12
	TupleTableSlot *slot;
#endif
	/* return index tuple if it was requested -- only for index scans */
	IndexTuple ituple;
	TupleDesc ituple_desc;

	/*
	 * If the user requested a tuple lock, the result of the lock is passed on
	 * in lockresult.
	 */
	TM_Result lockresult;
	int count;

	/*
	 * The memory context (optionally) set initially in the ScannerCtx. This
	 * can be used to allocate data on in the tuple handle function.
	 */
	MemoryContext mctx;
} TupleInfo;

typedef enum ScanTupleResult
{
	SCAN_DONE,
	SCAN_CONTINUE
} ScanTupleResult;

typedef enum ScanFilterResult
{
	SCAN_EXCLUDE,
	SCAN_INCLUDE
} ScanFilterResult;

typedef ScanTupleResult (*tuple_found_func)(TupleInfo *ti, void *data);
typedef ScanFilterResult (*tuple_filter_func)(TupleInfo *ti, void *data);
typedef void (*postscan_func)(int num_tuples, void *data);

typedef struct ScannerCtx
{
	Oid table;
	Oid index;
	ScanKey scankey;
	int nkeys, norderbys, limit; /* Limit on number of tuples to return. 0 or
								  * less means no limit */
	bool want_itup;
	LOCKMODE lockmode;
	MemoryContext result_mctx; /* The memory context to allocate the result
								* on */
	struct
	{
		LockTupleMode lockmode;
		LockWaitPolicy waitpolicy;
		bool enabled;
	} tuplock;
	ScanDirection scandirection;
	void *data; /* User-provided data passed on to filter()
				 * and tuple_found() */

	/*
	 * Optional handler called before a scan starts, but relation locks are
	 * acquired.
	 */
	void (*prescan)(void *data);

	/*
	 * Optional handler called after a scan finishes and before relation locks
	 * are released. Passes on the number of tuples found.
	 */
	void (*postscan)(int num_tuples, void *data);

	/*
	 * Optional handler to filter tuples. Should return SCAN_INCLUDE for
	 * tuples that should be passed on to tuple_found, or SCAN_EXCLUDE
	 * otherwise.
	 */
	ScanFilterResult (*filter)(TupleInfo *ti, void *data);

	/*
	 * Handler for found tuples. Should return SCAN_CONTINUE to continue the
	 * scan or SCAN_DONE to finish without scanning further tuples.
	 */
	ScanTupleResult (*tuple_found)(TupleInfo *ti, void *data);
} ScannerCtx;

/* Performs an index scan or heap scan and returns the number of matching
 * tuples. */
extern TSDLLEXPORT int ts_scanner_scan(ScannerCtx *ctx);
extern TSDLLEXPORT bool ts_scanner_scan_one(ScannerCtx *ctx, bool fail_if_not_found,
											char *item_type);

/*
 * Internal types and functions below.
 *
 * The below functions and types are really only exposed for the scan_iterator.
 * The type definitions are needed for struct embedding and the functions are needed
 * for iteration.
 */
typedef union ScanDesc
{
	IndexScanDesc index_scan;
	TableScanDesc heap_scan;
} ScanDesc;
/*
 * InternalScannerCtx is the context passed to Scanner functions.
 * It holds a pointer to the user-given ScannerCtx as well as
 * internal state used during scanning. Should not be used outside scanner.c
 * but is embedded in ScanIterator.
 */
typedef struct InternalScannerCtx
{
	Relation tablerel, indexrel;
	TupleInfo tinfo;
	ScanDesc scan;
	ScannerCtx *sctx;
	bool closed;
} InternalScannerCtx;

extern TSDLLEXPORT void ts_scanner_start_scan(ScannerCtx *ctx, InternalScannerCtx *ictx);

extern TSDLLEXPORT void ts_scanner_end_scan(ScannerCtx *ctx, InternalScannerCtx *ictx);
extern TSDLLEXPORT TupleInfo *ts_scanner_next(ScannerCtx *ctx, InternalScannerCtx *ictx);

#endif /* TIMESCALEDB_SCANNER_H */
