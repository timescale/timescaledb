/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_TRIGGER_H
#define TIMESCALEDB_TRIGGER_H

#include <postgres.h>
#include <catalog/pg_trigger.h>
#include "hypertable.h"
#include "chunk.h"

#define trigger_is_chunk_trigger(trigger) \
	((trigger) != NULL && TRIGGER_FOR_ROW((trigger)->tgtype) && !(trigger)->tgisinternal && strcmp((trigger)->tgname, INSERT_BLOCKER_NAME) != 0)

extern Trigger *trigger_by_name(Oid relid, const char *name, bool missing_ok);
extern void trigger_create_on_chunk(Oid trigger_oid, char *chunk_schema_name, char *chunk_table_name);
extern void trigger_create_all_on_chunk(Hypertable *ht, Chunk *chunk);
extern bool relation_has_transition_table_trigger(Oid relid);

#endif							/* TIMESCALEDB_TRIGGER_H */
