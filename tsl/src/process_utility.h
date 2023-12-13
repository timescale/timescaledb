/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <process_utility.h>

extern void tsl_process_altertable_cmd(Hypertable *ht, const AlterTableCmd *cmd);
extern void tsl_process_rename_cmd(Oid relid, Cache *hcache, const RenameStmt *stmt);
