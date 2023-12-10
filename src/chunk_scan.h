/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include "hypertable.h"

extern Chunk **ts_chunk_scan_by_chunk_ids(const Hyperspace *hs, const List *chunk_ids,
										  unsigned int *num_chunks);
