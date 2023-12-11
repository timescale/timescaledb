/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <nodes/plannodes.h>

#include "fdw/scan_exec.h"
#include "remote/async.h"

extern Node *data_node_scan_state_create(CustomScan *cscan);
