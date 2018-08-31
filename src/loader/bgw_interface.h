#ifndef TIMESCALEDB_BGW_INTERFACE_H
#define TIMESCALEDB_BGW_INTERFACE_H

#include <postgres.h>

extern void bgw_interface_register_api_version(void);
extern const int32 ts_bgw_loader_api_version;

#endif
