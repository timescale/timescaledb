/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/relscan.h>
#include <access/xact.h>
#include <access/htup_details.h>
#include <executor/tuptable.h>
#include <storage/lmgr.h>
#include <storage/bufmgr.h>
#include <storage/procarray.h>
#include <utils/rel.h>
#include <utils/palloc.h>
#include <utils/snapmgr.h>

#include "scanner.h"

enum ScannerType
{
	ScannerTypeTable,
	ScannerTypeIndex,
};

/*
 * Scanner can implement both index and heap scans in a single interface.
 */
typedef struct Scanner
{
	Relation (*openscan)(ScannerCtx *ctx);
	ScanDesc (*beginscan)(ScannerCtx *ctx);
	bool (*getnext)(ScannerCtx *ctx);
	void (*rescan)(ScannerCtx *ctx);
	void (*endscan)(ScannerCtx *ctx);
	void (*closescan)(ScannerCtx *ctx);
} Scanner;

/* Functions implementing heap scans */
static Relation
table_scanner_open(ScannerCtx *ctx)
{
	ctx->tablerel = table_open(ctx->table, ctx->lockmode);
	return ctx->tablerel;
}

static ScanDesc
table_scanner_beginscan(ScannerCtx *ctx)
{
	ctx->internal.scan.table_scan =
		table_beginscan(ctx->tablerel, ctx->snapshot, ctx->nkeys, ctx->scankey);

	return ctx->internal.scan;
}

static bool
table_scanner_getnext(ScannerCtx *ctx)
{
	bool success = table_scan_getnextslot(ctx->internal.scan.table_scan,
										  ForwardScanDirection,
										  ctx->internal.tinfo.slot);

	return success;
}

static void
table_scanner_rescan(ScannerCtx *ctx)
{
	table_rescan(ctx->internal.scan.table_scan, ctx->scankey);
}

static void
table_scanner_endscan(ScannerCtx *ctx)
{
	table_endscan(ctx->internal.scan.table_scan);
}

static void
table_scanner_close(ScannerCtx *ctx)
{
	LOCKMODE lockmode = (ctx->flags & SCANNER_F_KEEPLOCK) ? NoLock : ctx->lockmode;

	table_close(ctx->tablerel, lockmode);
}

/* Functions implementing index scans */
static Relation
index_scanner_open(ScannerCtx *ctx)
{
	ctx->tablerel = table_open(ctx->table, ctx->lockmode);
	ctx->indexrel = index_open(ctx->index, ctx->lockmode);
	return ctx->indexrel;
}

static ScanDesc
index_scanner_beginscan(ScannerCtx *ctx)
{
	InternalScannerCtx *ictx = &ctx->internal;

	ictx->scan.index_scan =
		index_beginscan(ctx->tablerel, ctx->indexrel, ctx->snapshot, ctx->nkeys, ctx->norderbys);
	ictx->scan.index_scan->xs_want_itup = ctx->want_itup;
	index_rescan(ictx->scan.index_scan, ctx->scankey, ctx->nkeys, NULL, ctx->norderbys);
	return ictx->scan;
}

static bool
index_scanner_getnext(ScannerCtx *ctx)
{
	InternalScannerCtx *ictx = &ctx->internal;
	bool success;

	success = index_getnext_slot(ictx->scan.index_scan, ctx->scandirection, ictx->tinfo.slot);
	ictx->tinfo.ituple = ictx->scan.index_scan->xs_itup;
	ictx->tinfo.ituple_desc = ictx->scan.index_scan->xs_itupdesc;

	return success;
}

static void
index_scanner_rescan(ScannerCtx *ctx)
{
	index_rescan(ctx->internal.scan.index_scan, ctx->scankey, ctx->nkeys, NULL, ctx->norderbys);
}

static void
index_scanner_endscan(ScannerCtx *ctx)
{
	index_endscan(ctx->internal.scan.index_scan);
}

static void
index_scanner_close(ScannerCtx *ctx)
{
	LOCKMODE lockmode = (ctx->flags & SCANNER_F_KEEPLOCK) ? NoLock : ctx->lockmode;
	index_close(ctx->indexrel, ctx->lockmode);
	table_close(ctx->tablerel, lockmode);
}

/*
 * Two scanners by type: heap and index scanners.
 */
static Scanner scanners[] = {
	[ScannerTypeTable] = {
		.openscan = table_scanner_open,
		.beginscan = table_scanner_beginscan,
		.getnext = table_scanner_getnext,
		.rescan = table_scanner_rescan,
		.endscan = table_scanner_endscan,
		.closescan = table_scanner_close,
	},
	[ScannerTypeIndex] = {
		.openscan = index_scanner_open,
		.beginscan = index_scanner_beginscan,
		.getnext = index_scanner_getnext,
		.rescan = index_scanner_rescan,
		.endscan = index_scanner_endscan,
		.closescan = index_scanner_close,
	}
};

static inline Scanner *
scanner_ctx_get_scanner(ScannerCtx *ctx)
{
	if (OidIsValid(ctx->index))
		return &scanners[ScannerTypeIndex];
	else
		return &scanners[ScannerTypeTable];
}

TSDLLEXPORT void
ts_scanner_rescan(ScannerCtx *ctx, const ScanKey scankey)
{
	Scanner *scanner = scanner_ctx_get_scanner(ctx);

	/* If scankey is NULL, the existing scan key was already updated or the
	 * old should be reused */
	if (NULL != scankey)
		memcpy(ctx->scankey, scankey, sizeof(*ctx->scankey));

	scanner->rescan(ctx);
}

static void
prepare_scan(ScannerCtx *ctx)
{
	ctx->internal.ended = false;
	ctx->internal.registered_snapshot = false;

	if (ctx->snapshot == NULL)
	{
		/*
		 * We use SnapshotSelf by default, for historical reasons mostly, but
		 * we probably want to move to an MVCC snapshot as the default. The
		 * difference is that a Self snapshot is an "instant" snapshot and can
		 * see its own changes. More importantly, however, unlike an MVCC
		 * snapshot, a Self snapshot is not subject to the strictness of
		 * SERIALIZABLE isolation mode.
		 *
		 * This is important in case of, e.g., concurrent chunk creation by
		 * two transactions; we'd like a transaction to use a new chunk as
		 * soon as the creating transaction commits, so that there aren't
		 * multiple transactions creating the same chunk and all but one fails
		 * with a conflict. However, under SERIALIZABLE mode a transaction is
		 * only allowed to read data from transactions that were committed
		 * prior to transaction start. This means that two or more
		 * transactions that create the same chunk must have all but the first
		 * committed transaction fail.
		 *
		 * Therefore, we probably want to exempt internal bookkeeping metadata
		 * from full SERIALIZABLE semantics (at least in the case of chunk
		 * creation), or otherwise the INSERT behavior will be different for
		 * hypertables compared to regular tables under SERIALIZABLE
		 * mode.
		 */
		ctx->snapshot = RegisterSnapshot(GetSnapshotData(SnapshotSelf));
		ctx->internal.registered_snapshot = true;
	}
}

TSDLLEXPORT Relation
ts_scanner_open(ScannerCtx *ctx)
{
	Scanner *scanner = scanner_ctx_get_scanner(ctx);

	Assert(NULL == ctx->tablerel);
	prepare_scan(ctx);

	return scanner->openscan(ctx);
}

/*
 * Start either a heap or index scan depending on the information in the
 * ScannerCtx. ScannerCtx must be setup by caller with the proper information
 * for the scan, including filters and callbacks for found tuples.
 */
TSDLLEXPORT void
ts_scanner_start_scan(ScannerCtx *ctx)
{
	InternalScannerCtx *ictx = &ctx->internal;
	Scanner *scanner;
	TupleDesc tuple_desc;

	if (ictx->started)
	{
		Assert(!ictx->ended);
		Assert(ctx->tablerel);
		Assert(OidIsValid(ctx->table));
		return;
	}

	if (ctx->tablerel == NULL)
	{
		Assert(NULL == ctx->indexrel);
		ts_scanner_open(ctx);
	}
	else
	{
		/*
		 * Relations already opened by caller: Only need to prepare the scan
		 * and set relation Oids so that the scanner knows which scanner
		 * implementation to use. Respect the auto-closing behavior set by the
		 * user, which is to auto close if unspecified.
		 */
		prepare_scan(ctx);
		ctx->table = RelationGetRelid(ctx->tablerel);

		if (NULL != ctx->indexrel)
			ctx->index = RelationGetRelid(ctx->indexrel);
	}

	scanner = scanner_ctx_get_scanner(ctx);
	scanner->beginscan(ctx);

	tuple_desc = RelationGetDescr(ctx->tablerel);

	ictx->tinfo.scanrel = ctx->tablerel;
	ictx->tinfo.mctx = ctx->result_mctx == NULL ? CurrentMemoryContext : ctx->result_mctx;
	ictx->tinfo.slot = MakeSingleTupleTableSlot(tuple_desc, table_slot_callbacks(ctx->tablerel));

	/* Call pre-scan handler, if any. */
	if (ctx->prescan != NULL)
		ctx->prescan(ctx->data);

	ictx->started = true;
}

static inline bool
ts_scanner_limit_reached(ScannerCtx *ctx)
{
	return ctx->limit > 0 && ctx->internal.tinfo.count >= ctx->limit;
}

static void
scanner_cleanup(ScannerCtx *ctx)
{
	InternalScannerCtx *ictx = &ctx->internal;

	if (ictx->registered_snapshot)
	{
		UnregisterSnapshot(ctx->snapshot);
		ctx->snapshot = NULL;
	}

	if (NULL != ictx->tinfo.slot)
	{
		ExecDropSingleTupleTableSlot(ictx->tinfo.slot);
		ictx->tinfo.slot = NULL;
	}
}

TSDLLEXPORT void
ts_scanner_end_scan(ScannerCtx *ctx)
{
	InternalScannerCtx *ictx = &ctx->internal;
	Scanner *scanner = scanner_ctx_get_scanner(ctx);

	if (ictx->ended)
		return;

	/* Call post-scan handler, if any. */
	if (ctx->postscan != NULL)
		ctx->postscan(ictx->tinfo.count, ctx->data);

	scanner->endscan(ctx);
	scanner_cleanup(ctx);
	ictx->ended = true;
	ictx->started = false;
}

TSDLLEXPORT void
ts_scanner_close(ScannerCtx *ctx)
{
	Scanner *scanner = scanner_ctx_get_scanner(ctx);

	Assert(ctx->internal.ended);

	if (NULL != ctx->tablerel)
	{
		scanner->closescan(ctx);
		ctx->tablerel = NULL;
		ctx->indexrel = NULL;
	}
}

TSDLLEXPORT TupleInfo *
ts_scanner_next(ScannerCtx *ctx)
{
	InternalScannerCtx *ictx = &ctx->internal;
	Scanner *scanner = scanner_ctx_get_scanner(ctx);
	bool is_valid = ts_scanner_limit_reached(ctx) ? false : scanner->getnext(ctx);

	while (is_valid)
	{
		if (ctx->filter == NULL || ctx->filter(&ictx->tinfo, ctx->data) == SCAN_INCLUDE)
		{
			ictx->tinfo.count++;

			if (ctx->tuplock)
			{
				TupleTableSlot *slot = ictx->tinfo.slot;

				Assert(ctx->snapshot);
				ictx->tinfo.lockresult = table_tuple_lock(ctx->tablerel,
														  &(slot->tts_tid),
														  ctx->snapshot,
														  slot,
														  GetCurrentCommandId(false),
														  ctx->tuplock->lockmode,
														  ctx->tuplock->waitpolicy,
														  ctx->tuplock->lockflags,
														  &ictx->tinfo.lockfd);
			}

			/* stop at a valid tuple */
			return &ictx->tinfo;
		}
		is_valid = ts_scanner_limit_reached(ctx) ? false : scanner->getnext(ctx);
	}

	if (!(ctx->flags & SCANNER_F_NOEND))
		ts_scanner_end_scan(ctx);

	if (!(ctx->flags & SCANNER_F_NOEND_AND_NOCLOSE))
		ts_scanner_close(ctx);

	return NULL;
}

/*
 * Perform either a heap or index scan depending on the information in the
 * ScannerCtx. ScannerCtx must be setup by caller with the proper information
 * for the scan, including filters and callbacks for found tuples.
 *
 * Return the number of tuples that were found.
 */
TSDLLEXPORT int
ts_scanner_scan(ScannerCtx *ctx)
{
	TupleInfo *tinfo;

	MemSet(&ctx->internal, 0, sizeof(ctx->internal));

	for (ts_scanner_start_scan(ctx); (tinfo = ts_scanner_next(ctx));)
	{
		/* Call tuple_found handler. Abort the scan if the handler wants us to */
		if (ctx->tuple_found != NULL && ctx->tuple_found(tinfo, ctx->data) == SCAN_DONE)
		{
			if (!(ctx->flags & SCANNER_F_NOEND))
				ts_scanner_end_scan(ctx);

			if (!(ctx->flags & SCANNER_F_NOEND_AND_NOCLOSE))
				ts_scanner_close(ctx);
			break;
		}
	}

	return ctx->internal.tinfo.count;
}

TSDLLEXPORT bool
ts_scanner_scan_one(ScannerCtx *ctx, bool fail_if_not_found, const char *item_type)
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

ItemPointer
ts_scanner_get_tuple_tid(TupleInfo *ti)
{
	return &ti->slot->tts_tid;
}

HeapTuple
ts_scanner_fetch_heap_tuple(const TupleInfo *ti, bool materialize, bool *should_free)
{
	return ExecFetchSlotHeapTuple(ti->slot, materialize, should_free);
}

TupleDesc
ts_scanner_get_tupledesc(const TupleInfo *ti)
{
	return ti->slot->tts_tupleDescriptor;
}

void *
ts_scanner_alloc_result(const TupleInfo *ti, Size size)
{
	return MemoryContextAllocZero(ti->mctx, size);
}
