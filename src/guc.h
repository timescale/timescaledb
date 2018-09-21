#ifndef TIMESCALEDB_GUC_H
#define TIMESCALEDB_GUC_H

#include <postgres.h>

extern bool telemetry_on(void);

extern bool guc_disable_optimizations;
extern bool guc_optimize_non_hypertables;
extern bool guc_constraint_aware_append;
extern bool guc_restoring;
extern int	guc_max_open_chunks_per_insert;
extern int	guc_max_cached_chunks_per_hypertable;
extern int	guc_telemetry_level;

void		_guc_init(void);
void		_guc_fini(void);

#endif							/* TIMESCALEDB_GUC_H */
