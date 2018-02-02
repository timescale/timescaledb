#ifndef TIMESCALEDB_EVENT_TRIGGER_H
#define TIMESCALEDB_EVENT_TRIGGER_H

#include <postgres.h>
#include <nodes/pg_list.h>

typedef enum EventTriggerDropType
{
	EVENT_TRIGGER_DROP_TABLE_CONSTRAINT,
	EVENT_TRIGGER_DROP_INDEX,
} EventTriggerDropType;

typedef struct EventTriggerDropObject
{
	EventTriggerDropType type;
} EventTriggerDropObject;

typedef struct EventTriggerDropTableConstraint
{
	EventTriggerDropObject obj;
	char	   *constraint_name;
	char	   *schema;
	char	   *table;
} EventTriggerDropTableConstraint;

typedef struct EventTriggerDropIndex
{
	EventTriggerDropObject obj;
	char	   *index_name;
	char	   *schema;
} EventTriggerDropIndex;

extern List *event_trigger_dropped_objects(void);
extern List *event_trigger_ddl_commands(void);
extern void _event_trigger_init(void);
extern void _event_trigger_fini(void);

#endif							/* TIMESCALEDB_EVENT_TRIGGER_H */
