/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <lib/dshash.h>
#include <utils/dsa.h>

/*
 * Shared-memory control structures for the per-tenant invalidation aggregator.
 *
 * Trackers (one per hypertable) are allocated on demand from a DSA area and
 * found through a dshash keyed by (database, hypertable id).  Only a tiny fixed control
 * block lives in the main shared segment; it is created by the loader and published
 * via a rendezvous variable.  The aggregation logic -- and the per-tracker layout itself
 * -- lives in tsl/src/continuous_aggs/tenant_tracker.{c,h}
 */

/* Shared-memory object names and the rendezvous variable. */
#define TENANT_TRACKER_CONTROL_SHMEM_NAME "ts_tenant_tracker_control"
#define TENANT_TRACKER_DSA_SHMEM_NAME "ts_tenant_tracker_dsa"
#define RENDEZVOUS_TENANT_TRACKER "ts_tenant_tracker"

/*
 * Size of the in-place DSA region (segment 0).
 * Must be large enough that dshash_create's initial allocation fits here, so the
 * postmaster never has to create a DSM segment at startup (forbidden).DSA allocates
 * in 64 KB superblocks (one per size class); dshash_create touches ~3 classes
 * (control, buckets, entries) -> ~192 KB minimum.  256 KB covers that with
 * headroom (and keeps the whole map in segment 0).  Trackers are large and are
 * allocated later by backends, which grow the DSA via DSM normally.
 */
#define TENANT_TRACKER_DSA_INIT_SIZE ((Size) 256 * 1024)

/* LWLock tranche names: one for the map machinery (DSA + dshash internal locks),
 * one for the trackers' partition locks. */
#define TENANT_TRACKER_MAP_TRANCHE_NAME "ts_tenant_tracker_map"
#define TENANT_TRACKER_PARTITION_TRANCHE_NAME "ts_tenant_tracker_partition"

/*
 * dshash key: (database, hypertable_id) -> dsa_pointer of its TenantTracking.
 *
 * The tracker map is process-global (one per postmaster, shared by every
 * database), but hypertable ids are assigned by a per-database sequence, so two
 * databases can hold the same id.  Keying by id alone would let them collide on
 * one shared tracker (one database's DML landing in the other's tracker).  The
 * database_id qualifier makes the key unique across databases.
 *
 * Both fields are 4 bytes so the struct is exactly 8 bytes with no padding
 * holes -- important because the map uses dshash_memcmp/dshash_memhash over the
 * whole key_size, and padding bytes would carry indeterminate contents.
 */
typedef struct TenantMapKey
{
	Oid database_id;	 /* MyDatabaseId of the tracked hypertable */
	int32 hypertable_id; /* TS raw hypertable id (per-database) */
} TenantMapKey;

/* dshash_memcmp/memhash run over the whole key_size, so padding must not exist. */
StaticAssertDecl(sizeof(TenantMapKey) == 8, "TenantMapKey must be 8 bytes with no padding");

/* dshash entry: TenantMapKey -> dsa_pointer of its TenantTracking. */
typedef struct TenantMapEntry
{
	TenantMapKey key; /* dshash key (must be first) */
	dsa_pointer tracker;
} TenantMapEntry;

/*
 * Canonical dshash parameters for the tracker map, shared by the loader (create)
 * and tsl (attach) sides so they cannot drift; a mismatch corrupts the attach.
 */
static inline dshash_parameters
tenant_tracker_map_params(int tranche_id)
{
	dshash_parameters params = {
		.key_size = sizeof(TenantMapKey),
		.entry_size = sizeof(TenantMapEntry),
		.compare_function = dshash_memcmp,
		.hash_function = dshash_memhash,
#if PG_VERSION_NUM >= 170000
		/* copy_function was added in PG17; PG16 dshash always uses memcpy. */
		.copy_function = dshash_memcpy,
#endif
		.tranche_id = tranche_id,
	};
	return params;
}

/*
 * Tiny fixed control block in the main shared segment.  Holds what a backend
 * needs to reach the dynamic structures: the dshash handle, the two LWLock
 * tranche ids, and a pointer to the in-place DSA region (valid in every backend
 * because the main segment maps at the same address everywhere).
 */
typedef struct TenantTrackerControl
{
	int map_tranche_id;				/* DSA + dshash internal locks */
	int tracker_tranche_id;			/* tracker partition locks */
	dshash_table_handle map_handle; /* dshash_attach handle */
	void *dsa_place;				/* in-place DSA region (main-shmem pointer) */
} TenantTrackerControl;

/* Loader-side shared memory lifecycle (called from the loader hooks). */
extern void ts_tenant_tracker_shmem_alloc(void);
extern void ts_tenant_tracker_shmem_startup(void);
