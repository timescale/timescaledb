/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

/*
 * Common functions, variables, and constants for working with policy
 * configurations.
 */

#include <postgres.h>
#include <utils/jsonb.h>

#define POLICY_CONFIG_KEY_HYPERTABLE_ID "hypertable_id"

extern int32 policy_config_get_hypertable_id(const Jsonb *config);
