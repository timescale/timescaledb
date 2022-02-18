/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_CHUNK_SCAN_H
#define TIMESCALEDB_CHUNK_SCAN_H

#include <postgres.h>

#include "hypertable.h"

extern Chunk **ts_chunk_scan_by_constraints(const Hyperspace *hs, const List *dimension_vecs,
											LOCKMODE chunk_lockmode, unsigned int *numchunks);

#endif /* TIMESCALEDB_CHUNK_SCAN_H */
