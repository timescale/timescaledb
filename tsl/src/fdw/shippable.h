/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_FDW_SHIPPABLE_H
#define TIMESCALEDB_TSL_FDW_SHIPPABLE_H

#include <postgres.h>

typedef struct TsFdwRelationInfo TsFdwRelationInfo;

extern bool is_builtin(Oid objectId);
extern bool is_shippable(Oid objectId, Oid classId, TsFdwRelationInfo *fpinfo);

#endif /* TIMESCALEDB_TSL_FDW_SHIPPABLE_H */
