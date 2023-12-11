/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

typedef struct TsFdwRelInfo TsFdwRelInfo;

extern bool is_builtin(Oid objectId);
extern bool is_shippable(Oid objectId, Oid classId, TsFdwRelInfo *fpinfo);
