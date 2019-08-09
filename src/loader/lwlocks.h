/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#ifndef TIMESCALEDB_LOADER_LWLOCKS_H
#define TIMESCALEDB_LOADER_LWLOCKS_H

#define RENDEZVOUS_CHUNK_APPEND_LWLOCK "ts_chunk_append_lwlock"

void ts_lwlocks_shmem_startup(void);
void ts_lwlocks_shmem_alloc(void);

#endif /* TIMESCALEDB_LOADER_LWLOCKS_H */
