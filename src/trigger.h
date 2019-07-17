/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_TRIGGER_H
#define TIMESCALEDB_TRIGGER_H

#include <postgres.h>
#include <catalog/pg_trigger.h>
#include "hypertable.h"
#include "chunk.h"

#define trigger_is_chunk_trigger(trigger)                                                          \
	((trigger) != NULL && TRIGGER_FOR_ROW((trigger)->tgtype) && !(trigger)->tgisinternal &&        \
	 strcmp((trigger)->tgname, INSERT_BLOCKER_NAME) != 0)

extern void ts_trigger_create_on_chunk(Oid trigger_oid, char *chunk_schema_name,
									   char *chunk_table_name);
extern TSDLLEXPORT void ts_trigger_create_all_on_chunk(Hypertable *ht, Chunk *chunk);
extern bool ts_relation_has_transition_table_trigger(Oid relid);

#endif /* TIMESCALEDB_TRIGGER_H */
