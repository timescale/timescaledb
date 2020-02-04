/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/relscan.h>
#include <access/xact.h>
#include <storage/lmgr.h>
#include <storage/bufmgr.h>
#include <utils/rel.h>

#include "compat.h"
#include "scanner.h"

enum ScannerType
{
	ScannerTypeHeap,
	ScannerTypeIndex,
};

/*
 * Scanner can implement both index and heap scans in a single interface.
 */
typedef struct Scanner
{
	Relation (*openheap)(InternalScannerCtx *ctx);
	ScanDesc (*beginscan)(InternalScannerCtx *ctx);
	bool (*getnext)(InternalScannerCtx *ctx);
	void (*endscan)(InternalScannerCtx *ctx);
	void (*closeheap)(InternalScannerCtx *ctx);
} Scanner;

/* Functions implementing heap scans */
static Relation
heap_scanner_open(InternalScannerCtx *ctx)
{
	ctx->tablerel = table_open(ctx->sctx->table, ctx->sctx->lockmode);
	return ctx->tablerel;
}

static ScanDesc
heap_scanner_beginscan(InternalScannerCtx *ctx)
{
	ScannerCtx *sctx = ctx->sctx;

	ctx->scan.heap_scan = table_beginscan(ctx->tablerel, SnapshotSelf, sctx->nkeys, sctx->scankey);
	return ctx->scan;
}

static bool
heap_scanner_getnext(InternalScannerCtx *ctx)
{
	ctx->tinfo.tuple = heap_getnext(ctx->scan.heap_scan, ctx->sctx->scandirection);
	return HeapTupleIsValid(ctx->tinfo.tuple);
}

static void
heap_scanner_endscan(InternalScannerCtx *ctx)
{
	heap_endscan(ctx->scan.heap_scan);
}

static void
heap_scanner_close(InternalScannerCtx *ctx)
{
	table_close(ctx->tablerel, ctx->sctx->lockmode);
}

/* Functions implementing index scans */
static Relation
index_scanner_open(InternalScannerCtx *ctx)
{
	ctx->tablerel = table_open(ctx->sctx->table, ctx->sctx->lockmode);
	ctx->indexrel = index_open(ctx->sctx->index, ctx->sctx->lockmode);
	return ctx->indexrel;
}

static ScanDesc
index_scanner_beginscan(InternalScannerCtx *ctx)
{
	ScannerCtx *sctx = ctx->sctx;

	ctx->scan.index_scan =
		index_beginscan(ctx->tablerel, ctx->indexrel, SnapshotSelf, sctx->nkeys, sctx->norderbys);
	ctx->scan.index_scan->xs_want_itup = ctx->sctx->want_itup;
	index_rescan(ctx->scan.index_scan, sctx->scankey, sctx->nkeys, NULL, sctx->norderbys);
	return ctx->scan;
}

static bool
index_scanner_getnext(InternalScannerCtx *ctx)
{
	bool success;
#if PG12_LT
	ctx->tinfo.tuple = index_getnext(ctx->scan.index_scan, ctx->sctx->scandirection);
	success = HeapTupleIsValid(ctx->tinfo.tuple);
#else /* TODO we should not materialize a HeapTuple unless needed */
	success = index_getnext_slot(ctx->scan.index_scan, ctx->sctx->scandirection, ctx->tinfo.slot);
	if (success)
		ctx->tinfo.tuple = ExecFetchSlotHeapTuple(ctx->tinfo.slot, false, NULL);
#endif

	ctx->tinfo.ituple = ctx->scan.index_scan->xs_itup;
	ctx->tinfo.ituple_desc = ctx->scan.index_scan->xs_itupdesc;
	return success;
}

static void
index_scanner_endscan(InternalScannerCtx *ctx)
{
	index_endscan(ctx->scan.index_scan);
}

static void
index_scanner_close(InternalScannerCtx *ctx)
{
	table_close(ctx->tablerel, ctx->sctx->lockmode);
	index_close(ctx->indexrel, ctx->sctx->lockmode);
}

/*
 * Two scanners by type: heap and index scanners.
 */
static Scanner scanners[] = {
	[ScannerTypeHeap] = {
		.openheap = heap_scanner_open,
		.beginscan = heap_scanner_beginscan,
		.getnext = heap_scanner_getnext,
		.endscan = heap_scanner_endscan,
		.closeheap = heap_scanner_close,
	},
	[ScannerTypeIndex] = {
		.openheap = index_scanner_open,
		.beginscan = index_scanner_beginscan,
		.getnext = index_scanner_getnext,
		.endscan = index_scanner_endscan,
		.closeheap = index_scanner_close,
	}
};

static inline Scanner *
scanner_ctx_get_scanner(ScannerCtx *ctx)
{
	if (OidIsValid(ctx->index))
		return &scanners[ScannerTypeIndex];
	else
		return &scanners[ScannerTypeHeap];
}

/*
 * Perform either a heap or index scan depending on the information in the
 * ScannerCtx. ScannerCtx must be setup by caller with the proper information
 * for the scan, including filters and callbacks for found tuples.
 *
 * Return the number of tuples that where found.
 */
TSDLLEXPORT void
ts_scanner_start_scan(ScannerCtx *ctx, InternalScannerCtx *ictx)
{
	TupleDesc tuple_desc;
	Scanner *scanner;

	ictx->sctx = ctx;
	ictx->closed = false;

	scanner = scanner_ctx_get_scanner(ctx);

	scanner->openheap(ictx);
	scanner->beginscan(ictx);

	tuple_desc = RelationGetDescr(ictx->tablerel);

	ictx->tinfo.scanrel = ictx->tablerel;
	ictx->tinfo.desc = tuple_desc;
	ictx->tinfo.mctx = ctx->result_mctx == NULL ? CurrentMemoryContext : ctx->result_mctx;
#if PG12 /* TODO we should not materialize a HeapTuple unless needed */
	ictx->tinfo.slot = MakeSingleTupleTableSlot(tuple_desc, &TTSOpsBufferHeapTuple);
#endif

	/* Call pre-scan handler, if any. */
	if (ctx->prescan != NULL)
		ctx->prescan(ctx->data);
}

static inline bool
ts_scanner_limit_reached(ScannerCtx *ctx, InternalScannerCtx *ictx)
{
	return ctx->limit > 0 && ictx->tinfo.count >= ctx->limit;
}

TSDLLEXPORT void
ts_scanner_end_scan(ScannerCtx *ctx, InternalScannerCtx *ictx)
{
	Scanner *scanner = scanner_ctx_get_scanner(ictx->sctx);

	if (ictx->closed)
		return;

	/* Call post-scan handler, if any. */
	if (ictx->sctx->postscan != NULL)
		ictx->sctx->postscan(ictx->tinfo.count, ictx->sctx->data);

	scanner->endscan(ictx);
	scanner->closeheap(ictx);
#if PG12
	ExecDropSingleTupleTableSlot(ictx->tinfo.slot);
#endif
	ictx->closed = true;
}

TSDLLEXPORT TupleInfo *
ts_scanner_next(ScannerCtx *ctx, InternalScannerCtx *ictx)
{
	Scanner *scanner = scanner_ctx_get_scanner(ctx);
	bool is_valid = ts_scanner_limit_reached(ctx, ictx) ? false : scanner->getnext(ictx);

	while (is_valid)
	{
		if (ctx->filter == NULL || ctx->filter(&ictx->tinfo, ctx->data) == SCAN_INCLUDE)
		{
			ictx->tinfo.count++;

			if (ctx->tuplock.enabled)
			{
				Buffer buffer;
				TM_FailureData hufd;

				ictx->tinfo.lockresult = heap_lock_tuple(ictx->tablerel,
														 ictx->tinfo.tuple,
														 GetCurrentCommandId(false),
														 ctx->tuplock.lockmode,
														 ctx->tuplock.waitpolicy,
														 false,
														 &buffer,
														 &hufd);

				/*
				 * A tuple lock pins the underlying buffer, so we need to
				 * unpin it.
				 */
				ReleaseBuffer(buffer);
			}

			/* stop at a valid tuple */
			return &ictx->tinfo;
		}
		is_valid = ts_scanner_limit_reached(ctx, ictx) ? false : scanner->getnext(ictx);
	}

	ts_scanner_end_scan(ctx, ictx);

	return NULL;
}

/*
 * Perform either a heap or index scan depending on the information in the
 * ScannerCtx. ScannerCtx must be setup by caller with the proper information
 * for the scan, including filters and callbacks for found tuples.
 *
 * Return the number of tuples that where found.
 */
TSDLLEXPORT int
ts_scanner_scan(ScannerCtx *ctx)
{
	InternalScannerCtx ictx = { 0 };
	TupleInfo *tinfo;

	for (ts_scanner_start_scan(ctx, &ictx); (tinfo = ts_scanner_next(ctx, &ictx));)
	{
		/* Call tuple_found handler. Abort the scan if the handler wants us to */
		if (ctx->tuple_found != NULL && ctx->tuple_found(tinfo, ctx->data) == SCAN_DONE)
		{
			ts_scanner_end_scan(ctx, &ictx);
			break;
		}
	}

	return ictx.tinfo.count;
}

TSDLLEXPORT bool
ts_scanner_scan_one(ScannerCtx *ctx, bool fail_if_not_found, char *item_type)
{
	int num_found = ts_scanner_scan(ctx);

	ctx->limit = 2;

	switch (num_found)
	{
		case 0:
			if (fail_if_not_found)
			{
				elog(ERROR, "%s not found", item_type);
			}
			return false;
		case 1:
			return true;
		default:
			elog(ERROR, "more than one %s found", item_type);
			return false;
	}
}
