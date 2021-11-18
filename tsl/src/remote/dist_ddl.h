/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_REMOTE_DIST_DDL_H
#define TIMESCALEDB_TSL_REMOTE_DIST_DDL_H

#include <process_utility.h>

extern void dist_ddl_state_init(void);
extern void dist_ddl_state_reset(void);
extern void dist_ddl_start(ProcessUtilityArgs *args);
extern void dist_ddl_end(EventTriggerData *command);
extern void dist_ddl_drop(List *dropped_objects);

#endif /* TIMESCALEDB_TSL_REMOTE_DIST_DDL_H */
