/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef _TIMESCALEDB_TSL_HYPERTABLE_H
#define _TIMESCALEDB_TSL_HYPERTABLE_H

#include <hypertable.h>

#include "catalog.h"

extern void hypertable_make_distributed(Hypertable *ht, ArrayType *servers);

List *hypertable_assign_servers(int32 hypertable_id, List *servers);

#endif /* _TIMESCALEDB_TSL_HYPERTABLE_H */
