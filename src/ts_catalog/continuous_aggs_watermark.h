/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include "export.h"
#include "hypertable.h"

extern TSDLLEXPORT void ts_cagg_watermark_delete_by_mat_hypertable_id(int32 mat_hypertable_id);
extern TSDLLEXPORT void ts_cagg_watermark_insert(Hypertable *mat_ht, int64 watermark,
												 bool watermark_isnull);
extern TSDLLEXPORT void ts_cagg_watermark_update(Hypertable *mat_ht, int64 watermark,
												 bool watermark_isnull, bool force_update);

extern TSDLLEXPORT int64 ts_cagg_watermark_get(int32 hypertable_id);
