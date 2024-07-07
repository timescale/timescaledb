/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <replication/message.h>

#include "guc.h"

/*
 * Utils to insert markers into the WAL log which demarcate compression
 * and decompression operations.
 * The primary purpose is to be able to discern between user-driven DML
 * operations (caused by statements which INSERT/UPDATE/DELETE data), and
 * compression-driven DML (moving data to/from compressed chunks).
 */

#define COMPRESSION_MARKER_START "::timescaledb-compression-start"
#define COMPRESSION_MARKER_END "::timescaledb-compression-end"
#define DECOMPRESSION_MARKER_START "::timescaledb-decompression-start"
#define DECOMPRESSION_MARKER_END "::timescaledb-decompression-end"

static inline bool
is_compression_wal_markers_enabled()
{
	return ts_guc_enable_compression_wal_markers && XLogLogicalInfoActive();
}

static inline void
write_logical_replication_msg_compression_start()
{
	if (is_compression_wal_markers_enabled())
	{
		LogLogicalMessageCompat(COMPRESSION_MARKER_START, "", 0, true, true);
	}
}

static inline void
write_logical_replication_msg_compression_end()
{
	if (is_compression_wal_markers_enabled())
	{
		LogLogicalMessageCompat(COMPRESSION_MARKER_END, "", 0, true, true);
	}
}

static inline void
write_logical_replication_msg_decompression_start()
{
	if (is_compression_wal_markers_enabled())
	{
		LogLogicalMessageCompat(DECOMPRESSION_MARKER_START, "", 0, true, true);
	}
}

static inline void
write_logical_replication_msg_decompression_end()
{
	if (is_compression_wal_markers_enabled())
	{
		LogLogicalMessageCompat(DECOMPRESSION_MARKER_END, "", 0, true, true);
	}
}
