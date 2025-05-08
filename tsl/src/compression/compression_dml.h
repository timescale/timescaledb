/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <access/skey.h>
#include <nodes/nodes.h>

#include "compression.h"
#include "ts_catalog/compression_settings.h"

typedef struct tuple_filtering_constraints
{
	/*
	 * All key column heap attribute numbers on uncompressed chunk.
	 * We shouldn't be dealing with system columns so no need to
	 * add/subtract FirstLowInvalidHeapAttributeNumber from these.
	 */
	Bitmapset *key_columns;
	/*
	 * The covered flag is set to true if we have a single constraint that is covered
	 * by all the columns present in the Bitmapset.
	 */
	bool covered;
	/* further fields only valid when covered is true */
	OnConflictAction on_conflict;
	Oid index_relid; /* used for better error messages */
	bool nullsnotdistinct;
	bool vectorized_filtering;
} tuple_filtering_constraints;

typedef bool(BatchMatcher)(RowDecompressor *decompressor, ScanKeyData *scankeys, int num_scankeys,
						   tuple_filtering_constraints *constraints, bool *skip_current_tuple);

bool slot_key_test(TupleTableSlot *slot, ScanKey skey);

ScanKeyData *build_mem_scankeys_from_slot(Oid ht_relid, CompressionSettings *settings,
										  Relation out_rel,
										  tuple_filtering_constraints *constraints,
										  TupleTableSlot *slot, int *num_scankeys);
ScanKeyData *build_index_scankeys(Relation index_rel, List *index_filters, int *num_scankeys);
ScanKeyData *build_index_scankeys_using_slot(Oid hypertable_relid, Relation in_rel,
											 Relation out_rel, Bitmapset *key_columns,
											 TupleTableSlot *slot, Relation *result_index_rel,
											 Bitmapset **index_columns, int *num_scan_keys);
ScanKeyData *build_heap_scankeys(Oid hypertable_relid, Relation in_rel, Relation out_rel,
								 CompressionSettings *settings, Bitmapset *key_columns,
								 Bitmapset **null_columns, TupleTableSlot *slot, int *num_scankeys);
ScanKeyData *build_update_delete_scankeys(Relation in_rel, List *heap_filters, int *num_scankeys,
										  Bitmapset **null_columns, bool *delete_only);
bool decompress_batch_for_value(const CompressionSettings *csettings, AttrNumber attnum,
								Datum value);
