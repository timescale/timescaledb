/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_PROCESS_UTILITY_H
#define TIMESCALEDB_TSL_PROCESS_UTILITY_H

#include <process_utility.h>

extern void _tsl_process_utility_init(void);
extern void _tsl_process_utility_fini(void);

extern void tsl_ddl_command_start(ProcessUtilityArgs *args);
extern void tsl_ddl_command_end(EventTriggerData *command);
extern void tsl_sql_drop(List *dropped_objects);
extern void tsl_process_altertable_cmd(Hypertable *ht, const AlterTableCmd *cmd);
extern void tsl_process_rename_cmd(Oid relid, Cache *hcache, const RenameStmt *stmt);

#endif /* TIMESCALEDB_TSL_PROCESS_UTILITY_H */
