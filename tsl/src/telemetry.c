/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>

#include "telemetry.h"

#include <utils/builtins.h>

#include <jsonb_utils.h>

#include "license.h"

#define LICENSE_EDITION_KEY "edition"
#define LICENSE_EDITION_COMMUNITY "community"
#define LICENSE_EDITION_ENTERPRISE "enterprise"

#define LICENSE_KIND_KEY "kind"
#define LICENSE_ID_KEY "id"
#define LICENSE_START_TIME_KEY "start_time"
#define LICENSE_END_TIME_KEY "end_time"

void
tsl_telemetry_add_license_info(JsonbParseState *parseState)
{
	if (!license_enterprise_enabled())
		ts_jsonb_add_str(parseState, LICENSE_EDITION_KEY, LICENSE_EDITION_COMMUNITY);
	else
	{
		char *start_time = DatumGetCString(
			DirectFunctionCall1(timestamptz_out, TimestampTzGetDatum(license_start_time())));
		char *end_time = DatumGetCString(
			DirectFunctionCall1(timestamptz_out, TimestampTzGetDatum(license_end_time())));

		ts_jsonb_add_str(parseState, LICENSE_EDITION_KEY, LICENSE_EDITION_ENTERPRISE);
		ts_jsonb_add_str(parseState, LICENSE_KIND_KEY, license_kind_str());
		ts_jsonb_add_str(parseState, LICENSE_ID_KEY, license_id_str());
		ts_jsonb_add_str(parseState, LICENSE_START_TIME_KEY, start_time);
		ts_jsonb_add_str(parseState, LICENSE_END_TIME_KEY, end_time);
	}
}
