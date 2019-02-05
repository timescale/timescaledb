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
extern void ts_jsonb_add_value(JsonbParseState *state, const char *key, JsonbValue *value);

extern TSDLLEXPORT text *ts_jsonb_get_text_field(Jsonb *json, text *field_name);
extern TSDLLEXPORT char *ts_jsonb_get_str_field(Jsonb *license, text *field_name);
extern TSDLLEXPORT TimestampTz ts_jsonb_get_time_field(Jsonb *license, text *field_name,
													   bool *field_found);

#endif /* TIMESCALEDB_JSONB_UTILS_H */
