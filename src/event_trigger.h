#ifndef TIMESCALEDB_EVENT_TRIGGER_H
#define TIMESCALEDB_EVENT_TRIGGER_H

#include <postgres.h>
#include <nodes/pg_list.h>

extern List *event_trigger_ddl_commands(void);
extern void _event_trigger_init(void);
extern void _event_trigger_fini(void);

#endif   /* TIMESCALEDB_EVENT_TRIGGER_H */
