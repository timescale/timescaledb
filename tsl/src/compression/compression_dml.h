/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <access/skey.h>

#include "ts_catalog/compression_settings.h"

ScanKeyData *build_scankeys_for_uncompressed(Oid ht_relid, CompressionSettings *settings,
											 Relation out_rel, Bitmapset *key_columns,
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
										  Bitmapset **null_columns);
int create_segment_filter_scankey(Relation in_rel, char *segment_filter_col_name,
								  StrategyNumber strategy, Oid subtype, ScanKeyData *scankeys,
								  int num_scankeys, Bitmapset **null_columns, Datum value,
								  bool is_null_check, bool is_array_op);
