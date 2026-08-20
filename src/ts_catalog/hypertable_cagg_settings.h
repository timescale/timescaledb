/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include "export.h"
#include "ts_catalog/catalog.h"

/* Whether typid is allowed as a granular-refresh tracking column. */
extern TSDLLEXPORT bool ts_tenant_type_is_supported(Oid typid);

/*
 * Catalog access for _timescaledb_catalog.hypertable_cagg_settings.
 *
 * Per-hypertable settings for granular refresh of continuous
 * aggregates. Row existence means granular refresh is configured for the
 * hypertable.
 */

extern TSDLLEXPORT bool ts_hypertable_cagg_settings_get(int32 hypertable_id,
														FormData_hypertable_cagg_settings *form);
extern TSDLLEXPORT void
ts_hypertable_cagg_settings_insert(const FormData_hypertable_cagg_settings *form);
extern TSDLLEXPORT void ts_hypertable_cagg_settings_delete(int32 hypertable_id);
extern TSDLLEXPORT bool ts_hypertable_cagg_settings_get_tenant_tracking_window(int32 hypertable_id,
																			   int64 *window_start,
																			   int64 *window_end);
extern TSDLLEXPORT bool
ts_hypertable_cagg_settings_get_tenant_tracking_column(int32 hypertable_id,
													   const char **column_name);
