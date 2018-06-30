#include <postgres.h>
#include <access/relscan.h>
#include <access/xact.h>
#include <storage/lmgr.h>
#include <storage/bufmgr.h>
#include <utils/rel.h>
#include <utils/tqual.h>

#include "scanner.h"

typedef union ScanDesc
{
	IndexScanDesc index_scan;
	HeapScanDesc heap_scan;
} ScanDesc;

/*
 * InternalScannerCtx is the context passed to Scanner functions.
 * It holds a pointer to the user-given ScannerCtx as well as
 * internal state used during scanning.
 */
typedef struct InternalScannerCtx
{
	Relation	tablerel,
				indexrel;
	TupleInfo	tinfo;
	ScanDesc	scan;
	ScannerCtx *sctx;
} InternalScannerCtx;

/*
 * Scanner can implement both index and heap scans in a single interface.
 */
typedef struct Scanner
{
	Relation	(*openheap) (InternalScannerCtx *ctx);
	ScanDesc	(*beginscan) (InternalScannerCtx *ctx);
	bool		(*getnext) (InternalScannerCtx *ctx);
	void		(*endscan) (InternalScannerCtx *ctx);
	void		(*closeheap) (InternalScannerCtx *ctx);
} Scanner;

/* Functions implementing heap scans */
static Relation
heap_scanner_open(InternalScannerCtx *ctx)
{
	ctx->tablerel = heap_open(ctx->sctx->table, ctx->sctx->lockmode);
	return ctx->tablerel;
}

static ScanDesc
heap_scanner_beginscan(InternalScannerCtx *ctx)
{
	ScannerCtx *sctx = ctx->sctx;

	ctx->scan.heap_scan = heap_beginscan(ctx->tablerel, SnapshotSelf,
										 sctx->nkeys, sctx->scankey);
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
	heap_close(ctx->tablerel, ctx->sctx->lockmode);
}

/* Functions implementing index scans */
static Relation
index_scanner_open(InternalScannerCtx *ctx)
{
	ctx->tablerel = heap_open(ctx->sctx->table, ctx->sctx->lockmode);
	ctx->indexrel = index_open(ctx->sctx->index, ctx->sctx->lockmode);
	return ctx->indexrel;
}

static ScanDesc
index_scanner_beginscan(InternalScannerCtx *ctx)
{
	ScannerCtx *sctx = ctx->sctx;

	ctx->scan.index_scan = index_beginscan(ctx->tablerel, ctx->indexrel,
										   SnapshotSelf, sctx->nkeys,
										   sctx->norderbys);
	ctx->scan.index_scan->xs_want_itup = ctx->sctx->want_itup;
	index_rescan(ctx->scan.index_scan, sctx->scankey,
				 sctx->nkeys, NULL, sctx->norderbys);
	return ctx->scan;
}

static bool
index_scanner_getnext(InternalScannerCtx *ctx)
{
	ctx->tinfo.tuple = index_getnext(ctx->scan.index_scan, ctx->sctx->scandirection);
	ctx->tinfo.ituple = ctx->scan.index_scan->xs_itup;
	ctx->tinfo.ituple_desc = ctx->scan.index_scan->xs_itupdesc;
	return HeapTupleIsValid(ctx->tinfo.tuple);
}

static void
index_scanner_endscan(InternalScannerCtx *ctx)
{
	index_endscan(ctx->scan.index_scan);
}

static void
index_scanner_close(InternalScannerCtx *ctx)
{
	heap_close(ctx->tablerel, ctx->sctx->lockmode);
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

/*
 * Perform either a heap or index scan depending on the information in the
 * ScannerCtx. ScannerCtx must be setup by caller with the proper information
 * for the scan, including filters and callbacks for found tuples.
 *
 * Return the number of tuples that where found.
 */
int
scanner_scan(ScannerCtx *ctx)
{
	TupleDesc	tuple_desc;
	bool		is_valid;
	Scanner    *scanner;

	InternalScannerCtx ictx = {
		.sctx = ctx,
	};

	if (OidIsValid(ctx->index))
		scanner = &scanners[ScannerTypeIndex];
	else
		scanner = &scanners[ScannerTypeHeap];

	scanner->openheap(&ictx);
	scanner->beginscan(&ictx);

	tuple_desc = RelationGetDescr(ictx.tablerel);

	ictx.tinfo.scanrel = ictx.tablerel;
	ictx.tinfo.desc = tuple_desc;
	ictx.tinfo.mctx = ctx->result_mctx == NULL ? CurrentMemoryContext : ctx->result_mctx;

	/* Call pre-scan handler, if any. */
	if (ctx->prescan != NULL)
		ctx->prescan(ctx->data);

	is_valid = scanner->getnext(&ictx);

	while (is_valid)
	{
		if (ctx->filter == NULL || ctx->filter(&ictx.tinfo, ctx->data))
		{
			ictx.tinfo.count++;

			if (ctx->tuplock.enabled)
			{
				Buffer		buffer;
				HeapUpdateFailureData hufd;

				ictx.tinfo.lockresult = heap_lock_tuple(ictx.tablerel, ictx.tinfo.tuple,
														GetCurrentCommandId(false),
														ctx->tuplock.lockmode,
														ctx->tuplock.waitpolicy,
														false, &buffer, &hufd);

				/*
				 * A tuple lock pins the underlying buffer, so we need to
				 * unpin it.
				 */
				ReleaseBuffer(buffer);
			}

			/* Abort the scan if the handler wants us to */
			if (ctx->tuple_found != NULL && !ctx->tuple_found(&ictx.tinfo, ctx->data))
				break;
		}

		/* Check if limit is reached */
		if (ctx->limit > 0 && ictx.tinfo.count >= ctx->limit)
			break;

		is_valid = scanner->getnext(&ictx);
	}

	/* Call post-scan handler, if any. */
	if (ctx->postscan != NULL)
		ctx->postscan(ictx.tinfo.count, ctx->data);

	scanner->endscan(&ictx);
	scanner->closeheap(&ictx);

	return ictx.tinfo.count;
}

bool
scanner_scan_one(ScannerCtx *ctx, bool fail_if_not_found, char *item_type)
{
	int			num_found = scanner_scan(ctx);

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
