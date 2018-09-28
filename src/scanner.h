/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_SCANNER_H
#define TIMESCALEDB_SCANNER_H

#include <postgres.h>
#include <access/relscan.h>
#include <utils/fmgroids.h>
#include <access/heapam.h>
#include <nodes/lockoptions.h>

/* Tuple information passed on to handlers when scanning for tuples. */
typedef struct TupleInfo
{
	Relation	scanrel;
	HeapTuple	tuple;
	TupleDesc	desc;
	/* return index tuple if it was requested -- only for index scans */
	IndexTuple	ituple;
	TupleDesc	ituple_desc;

	/*
	 * If the user requested a tuple lock, the result of the lock is passed on
	 * in lockresult.
	 */
	HTSU_Result lockresult;
	int			count;

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

typedef ScanTupleResult (*tuple_found_func) (TupleInfo *ti, void *data);
typedef ScanFilterResult (*tuple_filter_func) (TupleInfo *ti, void *data);

typedef struct ScannerCtx
{
	Oid			table;
	Oid			index;
	ScanKey		scankey;
	int			nkeys,
				norderbys,
				limit;			/* Limit on number of tuples to return. 0 or
								 * less means no limit */
	bool		want_itup;
	LOCKMODE	lockmode;
	MemoryContext result_mctx;	/* The memory context to allocate the result
								 * on */
	struct
	{
		LockTupleMode lockmode;
		LockWaitPolicy waitpolicy;
		bool		enabled;
	}			tuplock;
	ScanDirection scandirection;
	void	   *data;			/* User-provided data passed on to filter()
								 * and tuple_found() */

	/*
	 * Optional handler called before a scan starts, but relation locks are
	 * acquired.
	 */
	void		(*prescan) (void *data);

	/*
	 * Optional handler called after a scan finishes and before relation locks
	 * are released. Passes on the number of tuples found.
	 */
	void		(*postscan) (int num_tuples, void *data);

	/*
	 * Optional handler to filter tuples. Should return SCAN_INCLUDE for
	 * tuples that should be passed on to tuple_found, or SCAN_EXCLUDE
	 * otherwise.
	 */
	ScanFilterResult (*filter) (TupleInfo *ti, void *data);

	/*
	 * Handler for found tuples. Should return SCAN_CONTINUE to continue the
	 * scan or SCAN_DONE to finish without scanning further tuples.
	 */
	ScanTupleResult (*tuple_found) (TupleInfo *ti, void *data);
} ScannerCtx;

/* Performs an index scan or heap scan and returns the number of matching
 * tuples. */
int			scanner_scan(ScannerCtx *ctx);
bool		scanner_scan_one(ScannerCtx *ctx, bool fail_if_not_found, char *item_type);


#endif							/* TIMESCALEDB_SCANNER_H */
