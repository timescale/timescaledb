/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_CHUNK_APPEND_EXPLAIN_H
#define TIMESCALEDB_CHUNK_APPEND_EXPLAIN_H

#include <postgres.h>
#include <commands/explain.h>
#include <nodes/execnodes.h>

void ts_chunk_append_explain(CustomScanState *node, List *ancestors, ExplainState *es);

#endif /* TIMESCALEDB_CHUNK_APPEND_EXPLAIN_H */
