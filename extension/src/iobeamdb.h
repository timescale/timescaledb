#ifndef IOBEAMDB_H
#define IOBEAMDB_H

#define HYPERTABLE_NAME_QUERY "SELECT name FROM hypertable WHERE root_schema_name = '%s' AND root_table_name = '%s'"

#define REPLICA_OID_QUERY "SELECT  format('%%I.%%I', hr.schema_name, hr.table_name)::regclass::oid \
	                           FROM public.hypertable h \
                               INNER JOIN public.default_replica_node drn ON (drn.hypertable_name = h.name AND drn.database_name = current_database()) \
                               INNER JOIN public.hypertable_replica hr ON (hr.replica_id = drn.replica_id AND hr.hypertable_name = drn.hypertable_name) \
                               WHERE main_schema_name = '%s' AND main_table_name = '%s'"

#include "postgres.h"
#include "optimizer/planner.h"
#include "nodes/nodes.h"

void _PG_init(void);
void _PG_fini(void);

bool IobeamLoaded(void);

bool change_table_name_walker(Node *node, void *context);
Oid get_replica_oid(Oid mainRelationOid);

#endif /* IOBEAMDB_H */
