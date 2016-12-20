#include "postgres.h"
#include "catalog/namespace.h"
#include "optimizer/planner.h"
#include "nodes/nodes.h"
#include "nodes/print.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"
#include "executor/spi.h"
#include "commands/extension.h"
#include "tcop/tcopprot.h"

#include "fmgr.h"


#include "iobeamdb.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* Postgres hook interface */
static planner_hook_type prev_planner_hook = NULL;
static bool isLoaded = false;
PlannedStmt *iobeamdb_planner(Query *parse, int cursorOptions, ParamListInfo boundParams);

void
_PG_init(void)
{
    elog(INFO, "iobeamdb loaded");
    prev_planner_hook = planner_hook;
    planner_hook = iobeamdb_planner;
}

void		
_PG_fini(void)
{
    planner_hook = prev_planner_hook;
}

bool IobeamLoaded(void)
{
	if (!isLoaded)
	{
		Oid id = get_extension_oid("iobeamdb", true);
		if (id != InvalidOid)
		{
			isLoaded = true;
		}
	}
	return isLoaded;
}

char* 
cmdToString(CmdType cmdType)
{
    switch (cmdType) {
        case CMD_SELECT: return("select");
        case CMD_UPDATE: return("update");
        case CMD_INSERT: return("insert");
        case CMD_DELETE: return("delete");
        case CMD_UTILITY: return("utility");
        case CMD_UNKNOWN:
        default: 	return("unknown");		
    }

    return("unknown");	
}

PlannedStmt *
iobeamdb_planner(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
    PlannedStmt *rv = NULL;

	// elog(INFO, "iobeamdb got called");
    
    if (rv != NULL)
        return rv;
    
    if (prev_planner_hook != NULL) {
        /* Call any earlier hooks */
        elog(LOG, "     calling prev planner-hook");
        rv = (prev_planner_hook)(parse, cursorOptions, boundParams);
    } else {
        /* Call the standard planner */
        elog(LOG, "     calling standard_planner");
        rv = standard_planner(parse, cursorOptions, boundParams);
    }

	if(!IobeamLoaded()) \
	{
		return rv;
	} else 
	{
		/* 
		   here we have the plan and can start to mess around with it. 
		  */

		Oid singleFromTable = get_single_from_oid(parse);


		if (rv->commandType ==  CMD_SELECT && singleFromTable!= InvalidOid) 
		{
			char *hypertableName = get_hypertable_name(singleFromTable);
			if (hypertableName != NULL)
			{
		        Query *optimizedQuery;
				char *optimizedSql = get_optimized_query_sql(parse, hypertableName);
				pprint(parse);
				
				optimizedQuery = re_parse_optimized(optimizedSql);
				pprint(optimizedQuery);
				rv = standard_planner(optimizedQuery, cursorOptions, NULL);
			}
		}
	}
	return rv;
}

Query *re_parse_optimized(char * sql) {
	/*
	 * Parse the SQL string into a list of raw parse trees.
	 */
	List *raw_parsetree_list = pg_parse_query(sql);
    Node *parsetree;
	List *querytree_list;
	Query * parse;

	if(list_length(raw_parsetree_list) != 1) {
		elog(ERROR, "Expected one parsetree");
	}

	parsetree = (Node *) linitial(raw_parsetree_list);
	querytree_list = pg_analyze_and_rewrite(parsetree,
											sql,
											NULL,
											0);

	if(list_length(querytree_list) != 1) {
		elog(ERROR, "Expected one querytree");
	}

	parse = (Query *) linitial(querytree_list);
	return parse;
}

char* get_optimized_query_sql(Query *parse, char *hypertableName) {
	StringInfo codeGenSql = makeStringInfo();
	appendStringInfo(codeGenSql, "SELECT ioql_exec_query_record_sql(new_ioql_query(namespace_name => '%s'))", hypertableName);
	char *resultSql = NULL;

	SPI_connect();
	if (SPI_exec(codeGenSql->data, 1) <= 0) {
		elog(ERROR, "Got an SPI error");
	}

	if(SPI_processed != 1) {
		elog(ERROR, "Didn't get 1 row in code gen");
		SPI_finish();
		return NULL;
	}
	resultSql = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
	elog(LOG, "The optimized query is %s", resultSql);
	SPI_finish();
	return resultSql;
}

	
Oid get_single_from_oid(Query *parse) {
	/* one from entry which is to a table that is a hypertable */
	if (list_length(parse->jointree->fromlist)==1)
	{
		Node	   *jtnode = (Node *) linitial(parse->jointree->fromlist);
		if (IsA(jtnode, RangeTblRef))
		{
			int			varno = ((RangeTblRef *) jtnode)->rtindex;
			RangeTblEntry *rte = rt_fetch(varno, parse->rtable);
			if (rte->rtekind == RTE_RELATION)
			{
				Oid relationOid = rte->relid;
				return relationOid;
			}
		}
	}
    return InvalidOid;
}


char * get_hypertable_name(Oid relationOid) {
  Oid namespace = get_rel_namespace(relationOid);
  //todo cache this 
  Oid hypertable_meta = get_relname_relid("hypertable", get_namespace_oid("public", false));
  char *tableName = get_rel_name(relationOid);
  char *schemaName = get_namespace_name(namespace);
  StringInfo sql = makeStringInfo();
  int ret;
  uint64 results;
  char *name = NULL;

  /* prevents infinite recursion, don't check hypertable meta table */
  if (hypertable_meta == relationOid) {
	return NULL;
  }

  appendStringInfo(sql, HYPERTABLE_NAME_QUERY, schemaName, tableName);


  SPI_connect();

  ret = SPI_exec(sql->data, 1);

  
  if (ret <= 0) {
    elog(ERROR, "Got an SPI error");
  }

  results = SPI_processed;


  if(results == 1) {
	TupleDesc tupdesc = SPI_tuptable->tupdesc;
    HeapTuple tuple =  SPI_tuptable->vals[0];
	name = SPI_getvalue(tuple, tupdesc, 1);
    elog(LOG, "Is a hypertable query for %s %s.%s", name, schemaName, tableName);
  }
  SPI_finish();
  return name;
}
