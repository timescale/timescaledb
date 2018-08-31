#ifndef TIMESCALEDB_BGW_LAUNCHER_INTERFACE_H
#define TIMESCALEDB_BGW_LAUNCHER_INTERFACE_H

#include <postgres.h>

extern bool bgw_worker_reserve(void);
extern void bgw_worker_release(void);
extern int	bgw_num_unreserved(void);
extern int	bgw_loader_api_version(void);
extern void bgw_check_loader_api_version(void);
#endif							/* TIMESCALEDB_BGW_LAUNCHER_INTERFACE_H */
