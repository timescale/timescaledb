#include "postgres.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "optimizer/planner.h"
#include "nodes/nodes.h"
#include "nodes/print.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"
#include "executor/spi.h"
#include "commands/extension.h"
#include "tcop/tcopprot.h"

#include "access/xact.h"

#include "fmgr.h"


#include "iobeamdb.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* Postgres hook interface */
static planner_hook_type prev_planner_hook = NULL;
static bool isLoaded = false;
PlannedStmt *iobeamdb_planner(Query *parse, int cursorOptions, ParamListInfo boundParams);
static void io_xact_callback(XactEvent event, void *arg);

void 
_PG_init(void)
{
	elog(INFO, "iobeamdb loaded");
	prev_planner_hook = planner_hook;
	planner_hook = iobeamdb_planner;
	RegisterXactCallback(io_xact_callback, NULL);
}

void 
_PG_fini(void)
{
	planner_hook = prev_planner_hook;
}

bool 
IobeamLoaded(void)
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

PlannedStmt * 
iobeamdb_planner(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *rv = NULL;

	if(IobeamLoaded()) 
	{
		/* replace call to main table with call to the replica table */ 
		if (parse->commandType ==  CMD_SELECT) {
			change_table_name_walker((Node *) parse, NULL);
		}
	}

	if (prev_planner_hook != NULL) {
		/* Call any earlier hooks */
		elog(LOG, "     calling prev planner-hook");
		rv = (prev_planner_hook)(parse, cursorOptions, boundParams);
	} else {
		/* Call the standard planner */
		//elog(LOG, "     calling standard_planner");
		rv = standard_planner(parse, cursorOptions, boundParams);
	}

	return rv;
}

/*
 * Change all main tables to one of the replicas in the parse tree.  
 * 
 */
bool 
change_table_name_walker(Node *node, void *context)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) node;
		if (rangeTableEntry->rtekind == RTE_RELATION && rangeTableEntry->inh)
		{
			Oid replicaOid;
			replicaOid = get_replica_oid(rangeTableEntry->relid);
			if (replicaOid != InvalidOid) {
				rangeTableEntry->relid = replicaOid;
			}
		}
		return false;
	}

	if (IsA(node, Query))
	{
		return query_tree_walker((Query *) node, change_table_name_walker,
								 context, QTW_EXAMINE_RTES);
	}

	return expression_tree_walker(node,  change_table_name_walker, context);
}

/* 
 *
 * Use the default_replica_node to look up the oid for a replica table from the oid of the main table. 
 * TODO: make this use a cache instead of a db lookup every time.
 *
 * */
Oid 
get_replica_oid(Oid mainRelationOid) 
{
	Oid namespace = get_rel_namespace(mainRelationOid);
	//TODO: cache this 
	Oid hypertable_meta = get_relname_relid("hypertable", get_namespace_oid("public", false));
	char *tableName = get_rel_name(mainRelationOid);
	char *schemaName = get_namespace_name(namespace);
	StringInfo sql = makeStringInfo();
	int ret;
	Oid replicaOid = InvalidOid;

	/* prevents infinite recursion, don't check hypertable meta tables */
	if (
		hypertable_meta == InvalidOid 
		|| namespace == PG_CATALOG_NAMESPACE
		|| mainRelationOid == hypertable_meta 
		|| mainRelationOid ==  get_relname_relid("hypertable_replica", get_namespace_oid("public", false))
		|| mainRelationOid ==  get_relname_relid("default_replica_node", get_namespace_oid("public", false))
	   ) 
	{
		return InvalidOid;
	}

	appendStringInfo(sql, REPLICA_OID_QUERY, schemaName, tableName);

	SPI_connect();

	ret = SPI_exec(sql->data, 1);

	if (ret <= 0) 
	{
		elog(ERROR, "Got an SPI error");
	}

	if(SPI_processed == 1) 
	{
		bool isnull;
		Datum res;

		TupleDesc tupdesc = SPI_tuptable->tupdesc;
		HeapTuple tuple =  SPI_tuptable->vals[0];
		res = SPI_getbinval(tuple, tupdesc, 1, &isnull);
		replicaOid = DatumGetObjectId(res);
	}
	SPI_finish();
	return replicaOid;
}

/*
 * Commits meta commands issued with utility function in meta_commands.sql (e.g. _sysinternal.meta_transaction_exec)
 * These commands are committed in the pre-commit hook of the local transaction.
 * */
static void io_xact_callback(XactEvent event, void *arg)
{
	char* shouldPrecommit = GetConfigOptionByName("io.commit_meta_conn_in_precommit_hook", NULL, true);
	
	/* Quick exit if no connections with meta were in this transaction. */
	if (shouldPrecommit == NULL || strlen(shouldPrecommit) == 0)
		return;
	
	if (event == XACT_EVENT_PRE_COMMIT) {
		int ret;
		
		SPI_connect();
		ret = SPI_execute("SELECT 1 FROM _sysinternal.meta_transaction_end()", false, 0);
		if (ret <= 0)
		{
			elog(ERROR, "Got an SPI error");
		}
		
		SPI_finish();
	}
}

