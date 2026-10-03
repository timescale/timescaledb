/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <commands/explain.h>
#include <nodes/execnodes.h>
#include <nodes/pathnodes.h>
#include <nodes/plannodes.h>

#include "columnar_scan.h"
#include "ts_catalog/compression_settings.h"

typedef struct BatchSeekState BatchSeekState;

/* Found once per chunk while planning, see batch_seek_chunk_share(). */
typedef struct BatchSeekRange
{
	bool checked;
	bool valid;
	Datum lo;
	Datum hi;
} BatchSeekRange;

extern bool batch_seek_column_supported(CompressionSettings *settings, Oid chunk_relid,
										AttrNumber chunk_attno);
extern void batch_seek_index_costing_begin(const CompressionInfo *info);
extern void batch_seek_index_costing_end(const CompressionInfo *info);
extern bool batch_seek_path_costed(PlannerInfo *root, const CompressionInfo *info,
								   Path *compressed_path);
extern bool batch_seek_cost_path(PlannerInfo *root, const CompressionInfo *info,
								 Path *compressed_path, Path *seek_path);
extern List *batch_seek_plan_create(const ColumnarScanPath *dcpath, Path *compressed_path,
									Plan *compressed_scan, Expr **seek_expr);

extern BatchSeekState *batch_seek_begin(CustomScanState *parent, List *private, Expr *seek_expr,
										Plan *compressed_scan, Oid chunk_relid, EState *estate,
										int eflags);
extern TupleTableSlot *batch_seek_next(BatchSeekState *state);
extern void batch_seek_rescan(BatchSeekState *state, Bitmapset *chgParam);
extern void batch_seek_end(BatchSeekState *state);
extern void batch_seek_explain(BatchSeekState *state, List *ancestors, ExplainState *es);
