/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_JSONB_UTILS_H
#define TIMESCALEDB_JSONB_UTILS_H

#include <utils/datetime.h>
#include <utils/json.h>
#include <utils/jsonb.h>

#include "export.h"

extern TSDLLEXPORT void ts_jsonb_add_bool(JsonbParseState *state, const char *key, bool boolean);
extern TSDLLEXPORT void ts_jsonb_add_str(JsonbParseState *state, const char *key,
										 const char *value);
extern TSDLLEXPORT void ts_jsonb_add_interval(JsonbParseState *state, const char *key,
											  Interval *value);
extern TSDLLEXPORT void ts_jsonb_add_int32(JsonbParseState *state, const char *key,
										   const int32 value);
extern TSDLLEXPORT void ts_jsonb_add_int64(JsonbParseState *state, const char *key,
										   const int64 value);
extern TSDLLEXPORT void ts_jsonb_add_numeric(JsonbParseState *state, const char *key,
											 const Numeric value);

extern void ts_jsonb_add_value(JsonbParseState *state, const char *key, JsonbValue *value);

extern TSDLLEXPORT char *ts_jsonb_get_str_field(Jsonb *jsonb, const char *key);
extern TSDLLEXPORT Interval *ts_jsonb_get_interval_field(Jsonb *jsonb, const char *key);
extern TSDLLEXPORT TimestampTz ts_jsonb_get_time_field(Jsonb *jsonb, const char *key,
													   bool *field_found);
extern TSDLLEXPORT int32 ts_jsonb_get_int32_field(Jsonb *json, const char *key, bool *field_found);
extern TSDLLEXPORT int64 ts_jsonb_get_int64_field(Jsonb *json, const char *key, bool *field_found);

#endif /* TIMESCALEDB_JSONB_UTILS_H */
