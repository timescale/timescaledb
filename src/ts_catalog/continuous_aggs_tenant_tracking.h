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
 *
 * Rows are flushed here from the shared-memory per-tenant invalidation tracker
 * during a continuous aggregate refresh (Transaction 2).  tenant_id holds the
 * canonical text form of the tenant key; min/max_timestamp are internal time
 * values.  The "invalid marker" row (tenant_id/min/max NULL) signals that tenant tracking
 * for that seqnum is incomplete, forcing a fall back to the full invalidation
 * log.
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

extern TSDLLEXPORT void ts_cagg_tenant_tracking_insert_invalid_marker(int32 hypertable_id,
																	  int32 seqnum);

extern TSDLLEXPORT void ts_cagg_tenant_tracking_delete_by_hypertable_id(int32 hypertable_id);

/*
 * Whether at least one real (non-marker) tenant-tracking row exists for the
 * given hypertable and seqnum. A seqnum with only an invalid-marker row (or no
 * row) returns false, so the refresh falls back to a full refresh.
 */
extern TSDLLEXPORT bool ts_cagg_tenant_tracking_exists(int32 hypertable_id, int32 seqnum);
