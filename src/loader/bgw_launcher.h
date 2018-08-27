
#ifndef TIMESCALEDB_BGW_LAUNCHER_H
#define TIMESCALEDB_BGW_LAUNCHER_H

#include <postgres.h>

extern void bgw_cluster_launcher_register(void);

/*called by postmaster at launcher bgw startup*/
extern void bgw_cluster_launcher_main(void);
extern void bgw_db_scheduler_entrypoint(Oid db_id);



#endif							/* TIMESCALEDB_BGW_LAUNCHER_H */
