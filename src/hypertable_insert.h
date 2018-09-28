#ifndef TIMESCALEDB_HYPERTABLE_INSERT_H
#define TIMESCALEDB_HYPERTABLE_INSERT_H

#include <postgres.h>
#include <nodes/execnodes.h>

typedef struct HypertableInsertState HypertableInsertState;

struct HypertableInsertState
{
	CustomScanState cscan_state;
	ModifyTable *mt;
};

Plan	   *hypertable_insert_plan_create(ModifyTable *mt);

#endif							/* TIMESCALEDB_HYPERTABLE_INSERT_H */
