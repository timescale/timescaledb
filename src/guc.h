#ifndef TIMESCALEDB_GUC_H
#define TIMESCALEDB_GUC_H
#include <postgres.h>

extern bool guc_disable_optimizations;
extern bool guc_optimize_non_hypertables;

void		_guc_init(void);
void		_guc_fini(void);

#endif   /* TIMESCALEDB_GUC_H */
