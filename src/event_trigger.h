/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_EVENT_TRIGGER_H
#define TIMESCALEDB_EVENT_TRIGGER_H

#include <postgres.h>
#include <nodes/pg_list.h>

typedef enum EventTriggerDropType
{
	EVENT_TRIGGER_DROP_TABLE_CONSTRAINT,
	EVENT_TRIGGER_DROP_INDEX,
	EVENT_TRIGGER_DROP_TABLE,
	EVENT_TRIGGER_DROP_VIEW,
	EVENT_TRIGGER_DROP_SCHEMA,
	EVENT_TRIGGER_DROP_TRIGGER
} EventTriggerDropType;

typedef struct EventTriggerDropObject
{
	EventTriggerDropType type;
} EventTriggerDropObject;

typedef struct EventTriggerDropTableConstraint
{
	EventTriggerDropObject obj;
	char *constraint_name;
	char *schema;
	char *table;
} EventTriggerDropTableConstraint;

typedef struct EventTriggerDropIndex
{
	EventTriggerDropObject obj;
	char *index_name;
	char *schema;
} EventTriggerDropIndex;

typedef struct EventTriggerDropTable
{
	EventTriggerDropObject obj;
	char *table_name;
	char *schema;
} EventTriggerDropTable;

typedef struct EventTriggerDropView
{
	EventTriggerDropObject obj;
	char *view_name;
	char *schema;
} EventTriggerDropView;

typedef struct EventTriggerDropSchema
{
	EventTriggerDropObject obj;
	char *schema;
} EventTriggerDropSchema;

typedef struct EventTriggerDropTrigger
{
	EventTriggerDropObject obj;
	char *trigger_name;
	char *schema;
	char *table;
} EventTriggerDropTrigger;

extern List *ts_event_trigger_dropped_objects(void);
extern List *ts_event_trigger_ddl_commands(void);
extern void _event_trigger_init(void);
extern void _event_trigger_fini(void);

#endif /* TIMESCALEDB_EVENT_TRIGGER_H */
