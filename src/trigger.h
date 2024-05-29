/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include "chunk.h"
#include "hypertable.h"
#include <catalog/pg_trigger.h>

#define trigger_is_chunk_trigger(trigger)                                                          \
	((trigger) != NULL && TRIGGER_FOR_ROW((trigger)->tgtype) && !(trigger)->tgisinternal &&        \
	 strcmp((trigger)->tgname, INSERT_BLOCKER_NAME) != 0)

extern void ts_trigger_create_on_chunk(Oid trigger_oid, const char *chunk_schema_name,
									   const char *chunk_table_name);
extern TSDLLEXPORT void ts_trigger_create_all_on_chunk(const Chunk *chunk);
extern bool ts_relation_has_transition_table_trigger(Oid relid);
