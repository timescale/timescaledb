/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_TELEMETRY_REPLICATION_H
#define TIMESCALEDB_TELEMETRY_REPLICATION_H

#include <postgres.h>

#include "utils.h"

typedef struct ReplicationInfo
{
	bool got_num_wal_senders;
	int32 num_wal_senders;

	bool got_is_wal_receiver;
	bool is_wal_receiver;
} ReplicationInfo;

extern ReplicationInfo ts_telemetry_replication_info_gather(void);

#endif /* TIMESCALEDB_TELEMETRY_REPLICATION_H */
