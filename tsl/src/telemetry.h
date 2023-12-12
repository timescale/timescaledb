/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <utils/jsonb.h>

#include "telemetry/telemetry.h"

void tsl_telemetry_add_info(JsonbParseState **parse_state);
