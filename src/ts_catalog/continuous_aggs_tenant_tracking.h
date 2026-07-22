/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <utils/relcache.h>

#include "export.h"

/*
 * Catalog access for _timescaledb_catalog.continuous_aggs_tenant_tracking.
 */

extern TSDLLEXPORT void ts_cagg_tenant_tracking_insert_only(Relation rel, int32 hypertable_id,
															text *tenant_id, int64 min_timestamp,
															int64 max_timestamp, int32 seqnum);

extern TSDLLEXPORT void ts_cagg_tenant_tracking_insert(int32 hypertable_id, const char *tenant_id,
													   int tenant_id_len, int64 min_timestamp,
													   int64 max_timestamp, int32 seqnum);

/*
 * Streaming insert for the refresh hot path: open the relation once
 * (_begin), feed rows one at a time straight from their source -- e.g. the
 * shared-memory tenant tracker's quiesced generation, read in place with no
 * intermediate array -- then close once (_end).  The relation stays open and
 * the catalog owner role assumed for the whole span, so the per-cagg
 * serialization lock is held for as short a time as possible.  Each row's
 * tenant_id text is copied into the tuple by _insert_row; the caller need
 * not keep its own copy.  The inserter state is opaque and allocated by
 * _begin; _end frees it.
 */
typedef struct CaggTenantTrackingInserter CaggTenantTrackingInserter;

extern TSDLLEXPORT CaggTenantTrackingInserter *
ts_cagg_tenant_tracking_insert_begin(int32 hypertable_id, int32 seqnum);

extern TSDLLEXPORT void ts_cagg_tenant_tracking_insert_row(CaggTenantTrackingInserter *inserter,
														   const char *tenant_id, int tenant_id_len,
														   int64 min_timestamp,
														   int64 max_timestamp);

extern TSDLLEXPORT void ts_cagg_tenant_tracking_insert_end(CaggTenantTrackingInserter *inserter);

extern TSDLLEXPORT void ts_cagg_tenant_tracking_delete_by_hypertable_id(int32 hypertable_id);

/*
 * Whether at least one tenant-tracking row exists for the given hypertable and
 * seqnum. A seqnum with no row (e.g invalid generation or no tracking entries
 * for that generation) returns false, so the refresh falls back to a full
 * refresh.
 */
extern TSDLLEXPORT bool ts_cagg_tenant_tracking_exists(int32 hypertable_id, int32 seqnum);

/*
 * Highest seqnum among a hypertable's tenant-tracking rows, or 0 if it has none.
 * Uses the (hypertable_id, seqnum) index for a backward limit-1 seek.
 */
extern TSDLLEXPORT int32 ts_cagg_tenant_tracking_max_seqnum(int32 hypertable_id);
