/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <fmgr.h>

extern void _hypercore_proxy_init(void);
extern Datum hypercore_proxy_handler(PG_FUNCTION_ARGS);
