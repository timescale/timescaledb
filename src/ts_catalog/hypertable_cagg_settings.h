/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include "export.h"

/*
 * Catalog access for _timescaledb_catalog.hypertable_cagg_settings.
 *
 * Per-hypertable settings for granular refresh of continuous
 * aggregates. Row existence means granular refresh is configured for the
 * hypertable.
 */

extern TSDLLEXPORT void ts_hypertable_cagg_settings_delete(int32 hypertable_id);
