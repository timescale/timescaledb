
/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#ifndef TIMESCALEDB_LOADER_FUNCTION_TELEMETRY_H
#define TIMESCALEDB_LOADER_FUNCTION_TELEMETRY_H

#define RENDEZVOUS_FUNCTION_TELEMENTRY "ts_function_telemetry"
#define FN_TELEMETRY_LWLOCK_TRANCHE_NAME "ts_fn_telemetry_lwlock_tranche"

typedef struct FnTelemetryRendezvous
{
	LWLock *lock;
	HTAB *function_counts;
} FnTelemetryRendezvous;

typedef struct FnTelemetryHashEntry
{
	Oid key;
	pg_atomic_uint64 count;
} FnTelemetryHashEntry;

extern void ts_function_telemetry_shmem_startup(void);

extern void ts_function_telemetry_shmem_alloc(void);

#endif /* TIMESCALEDB_LOADER_FUNCTION_TELEMETRY_H */
