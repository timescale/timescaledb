/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include "scanner.h"
#include "ts_catalog/catalog.h"
#include "export.h"
#include "config.h"

extern ScanTupleResult ts_bgw_policy_delete_row_only_tuple_found(TupleInfo *ti, void *const data);

extern void ts_bgw_policy_delete_by_hypertable_id(int32 hypertable_id);
