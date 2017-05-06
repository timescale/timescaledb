#ifndef TIMESCALEDB_EXECUTOR_H
#define TIMESCALEDB_EXECUTOR_H
#include <postgres.h>

extern void _executor_init(void);
extern void _executor_fini(void);

extern void executor_add_number_tuples_processed(uint64 count);

#endif   /* TIMESCALEDB_EXECUTOR_H */
