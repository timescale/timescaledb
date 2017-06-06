#ifndef TIMESCALEDB_EXECUTOR_H
#define TIMESCALEDB_EXECUTOR_H
#include <postgres.h>

extern void _executor_init(void);
extern void _executor_fini(void);

extern void executor_add_number_tuples_processed(uint64 count);
extern uint64 executor_get_additional_tuples_processed(void);

extern void executor_level_enter(void);
extern void executor_level_exit(void);
#endif   /* TIMESCALEDB_EXECUTOR_H */
