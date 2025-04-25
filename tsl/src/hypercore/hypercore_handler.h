/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <access/attnum.h>
#include <access/tableam.h>
#include <access/xact.h>
#include <fmgr.h>
#include <nodes/pathnodes.h>

#include "hypertable.h"

typedef enum HypercoreScanOptions
{
	/* Normal scan options stretch to 9th bit. Start at bit 15 out of 32 to be
	 * safe. */
	SO_HYPERCORE_SKIP_COMPRESSED = 1 << 15,
} HypercoreScanOptions;

extern void hypercore_set_analyze_relid(Oid relid);
extern const TableAmRoutine *hypercore_routine(void);
extern void hypercore_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Hypertable *ht);
extern void hypercore_alter_access_method_begin(Oid relid, bool to_other_am);
extern void hypercore_alter_access_method_finish(Oid relid, bool to_other_am);
extern Datum hypercore_handler(PG_FUNCTION_ARGS);
extern void hypercore_xact_event(XactEvent event, void *arg);
extern bool hypercore_set_truncate_compressed(bool onoff);
extern void hypercore_scan_set_skip_compressed(TableScanDesc scan, bool skip);
extern void hypercore_skip_compressed_data_for_relation(Oid relid);
extern int hypercore_decompress_update_segment(Relation relation, const ItemPointer ctid,
											   TupleTableSlot *slot, Snapshot snapshot,
											   ItemPointer new_ctid);

typedef struct ColumnCompressionSettings
{
	/* Attribute name in the non-compressed relation */
	NameData attname;
	/* Attribute number in non-compressed relation */
	AttrNumber attnum;
	/* Attribute number in the compressed relation */
	AttrNumber cattnum;
	/* For orderby columns, these are the attribute numbers of the the min/max
	 * metadata columns. */
	AttrNumber cattnum_min;
	AttrNumber cattnum_max;
	/* Attribute type */
	Oid typid;
	bool is_orderby;
	bool is_segmentby;
	bool is_dropped;
} ColumnCompressionSettings;

/*
 * Compression info cache struct for access method.
 *
 * This struct is cached in a relcache entry's rd_amcache pointer and needs to
 * have a structure that can be palloc'ed in a single memory chunk.
 */
typedef struct HypercoreInfo
{
	Oid compressed_relid; /* Relid of compressed relation */
	int num_columns;
	AttrNumber count_cattno; /* Attribute number of count column in
							  * compressed rel */
	/* Per-column information follows. */
	ColumnCompressionSettings columns[FLEXIBLE_ARRAY_MEMBER];
} HypercoreInfo;

#define REL_IS_HYPERCORE(rel) ((rel)->rd_tableam == hypercore_routine())

extern HypercoreInfo *RelationGetHypercoreInfo(Relation rel);

static inline bool
hypercore_column_has_minmax(const ColumnCompressionSettings *column)
{
	if (AttributeNumberIsValid(column->cattnum_min))
	{
		/* Both min and max should always be set together */
		Assert(AttributeNumberIsValid(column->cattnum_max));
		return true;
	}

	return false;
}
