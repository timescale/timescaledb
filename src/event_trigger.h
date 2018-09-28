#ifndef TIMESCALEDB_EVENT_TRIGGER_H
#define TIMESCALEDB_EVENT_TRIGGER_H

#include <postgres.h>
#include <nodes/pg_list.h>

typedef enum EventTriggerDropType EventTriggerDropType;
typedef struct EventTriggerDropIndex EventTriggerDropIndex;
typedef struct EventTriggerDropObject EventTriggerDropObject;
typedef struct EventTriggerDropSchema EventTriggerDropSchema;
typedef struct EventTriggerDropTable EventTriggerDropTable;
typedef struct EventTriggerDropTableConstraint EventTriggerDropTableConstraint;
typedef struct EventTriggerDropTrigger EventTriggerDropTrigger;

enum EventTriggerDropType
{
	EVENT_TRIGGER_DROP_TABLE_CONSTRAINT,
	EVENT_TRIGGER_DROP_INDEX,
	EVENT_TRIGGER_DROP_TABLE,
	EVENT_TRIGGER_DROP_SCHEMA,
	EVENT_TRIGGER_DROP_TRIGGER
};

struct EventTriggerDropObject
{
	EventTriggerDropType type;
};

struct EventTriggerDropTableConstraint
{
	EventTriggerDropObject obj;
	char	   *constraint_name;
	char	   *schema;
	char	   *table;
};

struct EventTriggerDropIndex
{
	EventTriggerDropObject obj;
	char	   *index_name;
	char	   *schema;
};

struct EventTriggerDropTable
{
	EventTriggerDropObject obj;
	char	   *table_name;
	char	   *schema;
};

struct EventTriggerDropSchema
{
	EventTriggerDropObject obj;
	char	   *schema;
};

struct EventTriggerDropTrigger
{
	EventTriggerDropObject obj;
	char	   *trigger_name;
	char	   *schema;
	char	   *table;
};

extern List *event_trigger_dropped_objects(void);
extern List *event_trigger_ddl_commands(void);
extern void _event_trigger_init(void);
extern void _event_trigger_fini(void);

#endif							/* TIMESCALEDB_EVENT_TRIGGER_H */
