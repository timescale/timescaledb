
/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <fmgr.h>

#include <port/atomics.h>
#include <storage/lwlock.h>
#include <storage/shmem.h>

#include "loader/function_telemetry.h"

// Function telemetry hash table size. Sized to be large enough that we're
// unlikely to run out of entries, but small enough that it won't have a
// noticeable impact.
#define FN_TELEMETRY_HASH_SIZE 10000

static FnTelemetryRendezvous rendezvous;

void
ts_function_telemetry_shmem_startup()
{
	FnTelemetryRendezvous **rendezvous_ptr;
	HASHCTL hash_info;
	HTAB *function_telemetry_hash;
	LWLock **lock;
	bool found;

	// NOTE: dshash would be better once it's stable
	hash_info.keysize = sizeof(Oid);
	hash_info.entrysize = sizeof(FnTelemetryHashEntry);

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	/*
	 * GetNamedLWLockTranche must only be run once on windows, otherwise it
	 * segfaults. Since the shmem_startup_hook is run on every backend, we use
	 * a ShmemInitStruct to detect if this function has been called before.
	 */
	lock = ShmemInitStruct("fn_telemetry_detect_first_run", sizeof(LWLock *), &found);
	if (!found)
		*lock = &(GetNamedLWLockTranche(FN_TELEMETRY_LWLOCK_TRANCHE_NAME))->lock;

	function_telemetry_hash = ShmemInitHash("timescaledb function telemetry hash",
											FN_TELEMETRY_HASH_SIZE,
											FN_TELEMETRY_HASH_SIZE,
											&hash_info,
											HASH_ELEM | HASH_BLOBS);
	LWLockRelease(AddinShmemInitLock);

	rendezvous.lock = *lock;
	rendezvous.function_counts = function_telemetry_hash;

	rendezvous_ptr =
		(FnTelemetryRendezvous **) find_rendezvous_variable(RENDEZVOUS_FUNCTION_TELEMENTRY);
	*rendezvous_ptr = &rendezvous;
}

void
ts_function_telemetry_shmem_alloc()
{
	Size size = hash_estimate_size(FN_TELEMETRY_HASH_SIZE, sizeof(FnTelemetryHashEntry));
	RequestAddinShmemSpace(add_size(size, sizeof(LWLock *)));
	RequestNamedLWLockTranche(FN_TELEMETRY_LWLOCK_TRANCHE_NAME, 1);
}
