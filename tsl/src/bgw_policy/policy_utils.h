/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include "job.h"

const Dimension *get_open_dimension_for_hypertable(const Hypertable *ht, bool fail_if_not_found);
bool policy_get_verbose_log(const Jsonb *config);
