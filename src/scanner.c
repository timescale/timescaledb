#include <postgres.h>
#include <access/relscan.h>
#include <utils/rel.h>
#include <utils/tqual.h>

#include "scanner.h"

typedef union ScanDesc {
	IndexScanDesc index_scan;
	HeapScanDesc heap_scan;
} ScanDesc;

/*
 * InternalScannerCtx is the context passed to Scanner functions.
 * It holds a pointer to the user-given ScannerCtx as well as
 * internal state used during scanning.
 */
typedef struct InternalScannerCtx {
	Relation tablerel, indexrel;
	ScanDesc scan;
	ScannerCtx *sctx;
} InternalScannerCtx;

/*
 * Scanner can implement both index and heap scans in a single interface.
 */
typedef struct Scanner {
	Relation (*open)(InternalScannerCtx *ctx);
	ScanDesc (*beginscan)(InternalScannerCtx *ctx);
	HeapTuple (*getnext)(InternalScannerCtx *ctx);
	void (*endscan)(InternalScannerCtx *ctx);
	void (*close)(InternalScannerCtx *ctx);
} Scanner;

/* Functions implementing heap scans */
static Relation heap_scanner_open(InternalScannerCtx *ctx)
{
	ctx->tablerel = heap_open(ctx->sctx->table, ctx->sctx->lockmode);
	return ctx->tablerel;
}

static ScanDesc heap_scanner_beginscan(InternalScannerCtx *ctx)
{
	ScannerCtx *sctx = ctx->sctx;
	ctx->scan.heap_scan = heap_beginscan(ctx->tablerel, SnapshotSelf,
										 sctx->nkeys, sctx->scankey);
	return ctx->scan;
}

static HeapTuple heap_scanner_getnext(InternalScannerCtx *ctx)
{
	return heap_getnext(ctx->scan.heap_scan, ctx->sctx->scandirection);
}

static void heap_scanner_endscan(InternalScannerCtx *ctx)
{
	heap_endscan(ctx->scan.heap_scan);
}

static void heap_scanner_close(InternalScannerCtx *ctx)
{
	heap_close(ctx->tablerel, ctx->sctx->lockmode);
}

/* Functions implementing index scans */
static Relation index_scanner_open(InternalScannerCtx *ctx)
{
	ctx->tablerel = heap_open(ctx->sctx->table, ctx->sctx->lockmode);
	ctx->indexrel = index_open(ctx->sctx->index, ctx->sctx->lockmode);
	return ctx->indexrel;
}

static ScanDesc index_scanner_beginscan(InternalScannerCtx *ctx)
{
	ScannerCtx *sctx = ctx->sctx;
	ctx->scan.index_scan = index_beginscan(ctx->tablerel, ctx->indexrel,
										   SnapshotSelf, sctx->nkeys,
										   sctx->norderbys);
	index_rescan(ctx->scan.index_scan, sctx->scankey,
				 sctx->nkeys, NULL, sctx->norderbys);
	return ctx->scan;
}

static HeapTuple index_scanner_getnext(InternalScannerCtx *ctx)
{
	return index_getnext(ctx->scan.index_scan, ctx->sctx->scandirection);
}

static void index_scanner_endscan(InternalScannerCtx *ctx)
{
	index_endscan(ctx->scan.index_scan);
}

static void index_scanner_close(InternalScannerCtx *ctx)
{
	heap_close(ctx->tablerel, ctx->sctx->lockmode);
	index_close(ctx->indexrel, ctx->sctx->lockmode);
}

/*
 * Two scanners by type: heap and index scanners.
 */
static Scanner scanners[] = {
	[ScannerTypeHeap] = {
		.open = heap_scanner_open,
		.beginscan = heap_scanner_beginscan,
		.getnext = heap_scanner_getnext,
		.endscan = heap_scanner_endscan,
		.close = heap_scanner_close,
	},
	[ScannerTypeIndex] = {
		.open = index_scanner_open,
		.beginscan = index_scanner_beginscan,
		.getnext = index_scanner_getnext,
		.endscan = index_scanner_endscan,
		.close = index_scanner_close,
	}
};

/*
 * Perform either a heap or index scan depending on the information in the
 * ScannerCtx. ScannerCtx must be setup by caller with the proper information
 * for the scan, including filters and callbacks for found tuples.
 *
 * Return the number of tuples that where found.
 */
int scanner_scan(ScannerCtx *ctx)
{
	HeapTuple tuple;
	TupleDesc tuple_desc;
	int num_tuples = 0;
	Scanner *scanner = &scanners[ctx->scantype];
	InternalScannerCtx ictx = {
		.sctx = ctx,
	};

	scanner->open(&ictx);
	scanner->beginscan(&ictx);

	tuple_desc = RelationGetDescr(ictx.tablerel);

	tuple = scanner->getnext(&ictx);

	while (HeapTupleIsValid(tuple))
	{
		if (ctx->filter == NULL || ctx->filter(tuple, tuple_desc, ctx->data))
		{
			ctx->tuple_found(tuple, tuple_desc, ctx->data);
			num_tuples++;
		}

		tuple = scanner->getnext(&ictx);
	}

	scanner->endscan(&ictx);
	scanner->close(&ictx);

	return num_tuples;
}
