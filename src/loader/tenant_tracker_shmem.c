/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * Loader-side setup of the per-tenant invalidation aggregator's shared memory.
 */

#include <postgres.h>
#include <fmgr.h>
#include <lib/dshash.h>
#include <miscadmin.h>
#include <storage/lwlock.h>
#include <storage/shmem.h>
#include <utils/dsa.h>

#include "compat/compat.h"
#include "loader/tenant_tracker_shmem.h"

void
ts_tenant_tracker_shmem_alloc(void)
{
	/* Control block + the in-place DSA region (DSA grows via DSM beyond this). */
	RequestAddinShmemSpace(
		add_size(MAXALIGN(sizeof(TenantTrackerControl)), TENANT_TRACKER_DSA_INIT_SIZE));
}

void
ts_tenant_tracker_shmem_startup(void)
{
	bool found;
	TenantTrackerControl *control;
	void **rendezvous;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	control =
		ShmemInitStruct(TENANT_TRACKER_CONTROL_SHMEM_NAME, sizeof(TenantTrackerControl), &found);

	if (!found)
	{
		bool place_found;
		void *place = ShmemInitStruct(TENANT_TRACKER_DSA_SHMEM_NAME,
									  TENANT_TRACKER_DSA_INIT_SIZE,
									  &place_found);
		dsa_area *area;
		dshash_table *map;

		/* One tranche for the map machinery (DSA + dshash internal locks), one
		 * for the trackers' partition locks. */
#if PG19_GE
		/* PG19 merged tranche registration into LWLockNewTrancheId(name); the
		 * name is stored in shared memory and is visible to every backend. */
		control->map_tranche_id = LWLockNewTrancheId(TENANT_TRACKER_MAP_TRANCHE_NAME);
		control->tracker_tranche_id = LWLockNewTrancheId(TENANT_TRACKER_PARTITION_TRANCHE_NAME);
#else
		control->map_tranche_id = LWLockNewTrancheId();
		control->tracker_tranche_id = LWLockNewTrancheId();
#endif
		control->dsa_place = place;

		dshash_parameters params = tenant_tracker_map_params(control->map_tranche_id);

		/* Create the DSA in place and pin it .
		 * it must live for the postmaster's lifetime. */
		area =
			dsa_create_in_place(place, TENANT_TRACKER_DSA_INIT_SIZE, control->map_tranche_id, NULL);
		dsa_pin(area);

		map = dshash_create(area, &params, NULL);
		control->map_handle = dshash_get_hash_table_handle(map);
	}

	LWLockRelease(AddinShmemInitLock);

#if PG19_LT
	/*
	 * Register the tranche names in every backend.  Under EXEC_BACKEND this hook
	 * runs per backend (so each registers); under the fork model it runs once in
	 * the postmaster and children inherit the registration.  The ids are already
	 * set in the control block by the creating run above.
	 *
	 * PG19 stores the name in shared memory at LWLockNewTrancheId(name) time, so
	 * no per-backend registration is needed and LWLockRegisterTranche was removed.
	 */
	LWLockRegisterTranche(control->map_tranche_id, TENANT_TRACKER_MAP_TRANCHE_NAME);
	LWLockRegisterTranche(control->tracker_tranche_id, TENANT_TRACKER_PARTITION_TRANCHE_NAME);
#endif

	/* Publish so the tsl aggregation code can find it (mirrors loader/lwlocks.c). */
	rendezvous = find_rendezvous_variable(RENDEZVOUS_TENANT_TRACKER);
	*rendezvous = control;
}
