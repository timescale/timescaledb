/* -*- Mode: C; tab-width: 4; indent-tabs-mode: t; c-basic-offset: 4 -*- */
#ifndef IOBEAMDB_H
#define IOBEAMDB_H

#define HYPERTABLE_INFO_QUERY   "\
                                SELECT  format('%I.%I', hr.schema_name, hr.table_name)::regclass::oid, \
                                  pe.partitioning_column, pe.partitioning_func_schema, pe.partitioning_func, pe.partitioning_mod, \
                                  format('%I.%I', h.root_schema_name, h.root_table_name)::regclass::oid, \
                                  h.id \
                                FROM _iobeamdb_catalog.hypertable h \
                                INNER JOIN _iobeamdb_catalog.default_replica_node drn ON (drn.hypertable_id = h.id AND drn.database_name = current_database()) \
                                INNER JOIN _iobeamdb_catalog.hypertable_replica hr ON (hr.replica_id = drn.replica_id AND hr.hypertable_id = drn.hypertable_id) \
                                INNER JOIN _iobeamdb_catalog.partition_epoch pe ON (pe.hypertable_id = h.id) \
                                WHERE h.schema_name = $1 AND h.table_name = $2"

#include "postgres.h"
#include "optimizer/planner.h"
#include "nodes/nodes.h"

void _PG_init(void);
void _PG_fini(void);

bool IobeamLoaded(void);

bool change_table_name_walker(Node *node, void *context);

#endif /* IOBEAMDB_H */
