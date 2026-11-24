/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include <utils/selfuncs.h>

extern bool ts_get_variable_range(PlannerInfo *root, VariableStatData *vardata, Oid sortop,
								  Datum *min, Datum *max);
