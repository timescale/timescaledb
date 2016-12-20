#ifndef IOBEAMDB_H
#define IOBEAMDB_H

#define HYPERTABLE_NAME_QUERY "SELECT name FROM hypertable WHERE root_schema_name = '%s' AND root_table_name = '%s'"

#include "postgres.h"
#include "optimizer/planner.h"
#include "nodes/nodes.h"


void _PG_init(void);
void _PG_fini(void);

bool IobeamLoaded(void);
char* cmdToString(CmdType cmdType);

Oid get_single_from_oid(Query *parse);
char* get_hypertable_name(Oid relationOid);
char* get_optimized_query_sql(Query *parse, char *hypertableName);
Query *re_parse_optimized(char * sql);

#endif /* IOBEAMDB_H */
