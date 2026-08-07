/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <tcop/utility.h>

#include "extension_constants.h"

extern char *ts_loader_extension_version(void);

extern bool ts_loader_extension_exists(void);

extern void ts_loader_extension_check(void);

/*
 * Rendezvous between the shared-preload loader and the versioned extension for
 * ProcessUtility. The loader installs a permanent ProcessUtility hook at
 * preload time so that TimescaleDB remains last in the call chain when it is
 * listed first in shared_preload_libraries. The versioned extension publishes
 * its real handler here instead of becoming the head of the hook chain.
 */
typedef struct TsProcessUtilityRendezvous
{
	ProcessUtility_hook_type versioned_hook;
	ProcessUtility_hook_type prev_hook;
} TsProcessUtilityRendezvous;

/* WaitLatch expects a long, so make sure to cast the value */
/* Default value for timescaledb.launcher_poll_time */
#ifdef TS_DEBUG
#define BGW_LAUNCHER_POLL_TIME_MS 10
#else
#define BGW_LAUNCHER_POLL_TIME_MS 60000
#endif

/* GUC to control launcher timeout */
extern int ts_guc_bgw_launcher_poll_time;
