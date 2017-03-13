/* -*- Mode: C; tab-width: 4; indent-tabs-mode: t; c-basic-offset: 4 -*- */
#include <unistd.h>

#include "postgres.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_class.h"
#include "optimizer/planner.h"
#include "optimizer/clauses.h"
#include "nodes/nodes.h"
#include "nodes/print.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/int8.h"
#include "executor/spi.h"
#include "commands/extension.h"
#include "commands/tablecmds.h"
#include "commands/trigger.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "deps/dblink.h"

#include "access/xact.h"
#include "parser/parse_oper.h"
#include "parser/parse_func.h"

#include "fmgr.h"


#include "timescaledb.h"
#include "insert.h"
#include "cache.h"
#include "hypertable_cache.h"
#include "errors.h"
#include "utils.h"
#include "partitioning.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

void		_PG_init(void);
void		_PG_fini(void);

/* Postgres hook interface */
static planner_hook_type prev_planner_hook = NULL;
static ProcessUtility_hook_type prev_ProcessUtility_hook = NULL;

/* variables */
static bool isLoaded = false;

/* definitions */
PlannedStmt *timescaledb_planner(Query *parse, int cursorOptions, ParamListInfo boundParams);
static void io_xact_callback(XactEvent event, void *arg);

static List *callbackConnections = NIL;

typedef struct partitioning_info
{
	Name		partitioning_column;
	Name		partitioning_func_schema;
	Name		partitioning_func;
	int32		partitioning_mod;
}	partitioning_info;

typedef struct change_table_name_context
{
	Query	   *parse;
	Cache	   *hcache;
	Hypertable *hentry;
} change_table_name_context;

typedef struct add_partitioning_func_qual_context
{
	Query	   *parse;
	Cache	   *hcache;
	Hypertable *hentry;
} add_partitioning_func_qual_context;


static void add_partitioning_func_qual(Query *parse, Cache *hcache, Hypertable *hentry);
static Node *add_partitioning_func_qual_mutator(Node *node, add_partitioning_func_qual_context *context);
static PartitioningInfo *
get_partitioning_info_for_partition_column_var(Var *var_expr, Query *parse, Cache *hcache, Hypertable *hentry);
static Expr *
			create_partition_func_equals_const(Var *var_expr, Const *const_expr, char *partitioning_func_schema, char *partitioning_func, int32 partitioning_mod);


void timescaledb_ProcessUtility(Node *parsetree,
						   const char *queryString,
						   ProcessUtilityContext context,
						   ParamListInfo params,
						   DestReceiver *dest,
						   char *completionTag);
void prev_ProcessUtility(Node *parsetree,
					const char *queryString,
					ProcessUtilityContext context,
					ParamListInfo params,
					DestReceiver *dest,
					char *completionTag);

extern void _hypertable_cache_init(void);
extern void _hypertable_cache_fini(void);

extern void _chunk_cache_init(void);
extern void _chunk_cache_fini(void);

extern void _cache_invalidate_init(void);
extern void _cache_invalidate_fini(void);
extern void _cache_invalidate_extload(void);

void
_PG_init(void)
{
	elog(INFO, "timescaledb loaded");
	_hypertable_cache_init();
	_chunk_cache_init();
	_cache_invalidate_init();

	prev_planner_hook = planner_hook;
	planner_hook = timescaledb_planner;

	prev_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = timescaledb_ProcessUtility;

	RegisterXactCallback(io_xact_callback, NULL);

}

void
_PG_fini(void)
{
	planner_hook = prev_planner_hook;
	ProcessUtility_hook = prev_ProcessUtility_hook;
	_cache_invalidate_fini();
	_hypertable_cache_fini();
	_chunk_cache_fini();
}

bool
IobeamLoaded(void)
{
	if (!isLoaded)
	{
		Oid			id;

		if (!IsTransactionState())
		{
			return false;
		}

		id = get_extension_oid("timescaledb", true);

		if (id != InvalidOid && !(creating_extension && id == CurrentExtensionObject))
		{
			isLoaded = true;
			_cache_invalidate_extload();
		}
	}
	return isLoaded;
}


/*
 * Change all main tables to one of the replicas in the parse tree.
 *
 */
static bool
change_table_name_walker(Node *node, void *context)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) node;
		change_table_name_context *ctx = (change_table_name_context *) context;

		if (rangeTableEntry->rtekind == RTE_RELATION && rangeTableEntry->inh)
		{
			Hypertable *hentry = hypertable_cache_get_entry(ctx->hcache, rangeTableEntry->relid);

			if (hentry != NULL)
			{
				ctx->hentry = hentry;
				rangeTableEntry->relid = hentry->replica_table;
			}
		}
		else if (rangeTableEntry->rtekind == RTE_RELATION && ctx->parse->commandType == CMD_INSERT)
		{
			Hypertable *hentry = hypertable_cache_get_entry(ctx->hcache, rangeTableEntry->relid);

			if (hentry != NULL)
			{
				rangeTableEntry->relid = hentry->root_table;
			}
		}
		return false;
	}

	if (IsA(node, Query))
	{
		return query_tree_walker((Query *) node, change_table_name_walker,
								 context, QTW_EXAMINE_RTES);
	}

	return expression_tree_walker(node, change_table_name_walker, context);
}

PlannedStmt *
timescaledb_planner(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *rv = NULL;

	if (IobeamLoaded())
	{
		change_table_name_context context;
		char	   *printParse = GetConfigOptionByName("io.print_parse", NULL, true);

		/* set to false to not print all internal actions */
		SetConfigOption("io.print_parse", "false", PGC_USERSET, PGC_S_SESSION);

		/* replace call to main table with call to the replica table */
		context.hcache = hypertable_cache_pin();
		context.parse = parse;
		context.hentry = NULL;
		change_table_name_walker((Node *) parse, &context);
		/* note assumes 1 hypertable per query */
		if (context.hentry != NULL)
		{
			add_partitioning_func_qual(parse, context.hcache, context.hentry);
		}
		cache_release(context.hcache);

		if (printParse != NULL && strcmp(printParse, "true") == 0)
		{
			pprint(parse);
		}

	}
	if (prev_planner_hook != NULL)
	{
		/* Call any earlier hooks */
		rv = (prev_planner_hook) (parse, cursorOptions, boundParams);
	}
	else
	{
		/* Call the standard planner */
		rv = standard_planner(parse, cursorOptions, boundParams);
	}

	return rv;
}


char *
copy_table_name(int32 hypertable_id)
{
	StringInfo	temp_table_name = makeStringInfo();

	appendStringInfo(temp_table_name, "_copy_temp_%d", hypertable_id);
	return temp_table_name->data;
}


/*
 * This function does a transformation that allows postgres's native constraint exclusion to exclude space partititions when
 * the query contains equivalence qualifiers on the space partition key.
 *
 * This function goes through the upper-level qual of a parse tree and finds quals of the form:
 *				partitioning_column = const
 * It transforms them into the qual:
 *				partitioning_column = const AND partitioning_func(partition_column, partitioning_mod) = partitioning_func(const, partitioning_mod)
 *
 * This tranformation helps because the check constraint on a table is of the form CHECK(partitioning_func(partition_column, partitioning_mod) BETWEEN X AND Y).
 */
static void
add_partitioning_func_qual(Query *parse, Cache *hcache, Hypertable *hentry)
{
	add_partitioning_func_qual_context context;

	context.parse = parse;
	context.hcache = hcache;
	context.hentry = hentry;
	parse->jointree->quals = add_partitioning_func_qual_mutator(parse->jointree->quals, &context);
}

static Node *
add_partitioning_func_qual_mutator(Node *node, add_partitioning_func_qual_context *context)
{
	if (node == NULL)
		return NULL;

	/*
	 * Detect partitioning_column = const. If not fall-thru. If detected,
	 * replace with partitioning_column = const AND
	 * partitioning_func(partition_column, partitioning_mod) =
	 * partitioning_func(const, partitioning_mod)
	 */
	if (IsA(node, OpExpr))
	{
		OpExpr	   *exp = (OpExpr *) node;

		if (list_length(exp->args) == 2)
		{
			/* only look at var op const or const op var; */
			Node	   *left = (Node *) linitial(exp->args);
			Node	   *right = (Node *) lsecond(exp->args);
			Var		   *var_expr = NULL;
			Node	   *other_expr = NULL;

			if (IsA(left, Var))
			{
				var_expr = (Var *) left;
				other_expr = right;
			}
			else if (IsA(right, Var))
			{
				var_expr = (Var *) right;
				other_expr = left;
			}

			if (var_expr != NULL)
			{
				if (!IsA(other_expr, Const))
				{
					/* try to simplify the non-var expression */
					other_expr = eval_const_expressions(NULL, other_expr);
				}
				if (IsA(other_expr, Const))
				{
					/* have a var and const, make sure the op is = */
					Const	   *const_expr = (Const *) other_expr;
					Oid			eq_oid = OpernameGetOprid(list_make2(makeString("pg_catalog"), makeString("=")), exprType(left), exprType(right));

					if (eq_oid == exp->opno)
					{
						/*
						 * I now have a var = const. Make sure var is a
						 * partitioning column
						 */
						PartitioningInfo *pi = get_partitioning_info_for_partition_column_var(var_expr,
															  context->parse,
										   context->hcache, context->hentry);

						if (pi != NULL)
						{
							/* The var is a partitioning column */
							Expr	   *partitioning_clause = create_partition_func_equals_const(var_expr, const_expr,
																								 pi->partfunc.schema, pi->partfunc.name, pi->partfunc.modulos);

							return (Node *) make_andclause(list_make2(node, partitioning_clause));

						}
					}
				}
			}
		}
	}

	return expression_tree_mutator(node, add_partitioning_func_qual_mutator,
								   (void *) context);
}

/* Returns the partitioning info for a var if the var is a partitioning column. If the var is not a partitioning
 * column return NULL */
static PartitioningInfo *
get_partitioning_info_for_partition_column_var(Var *var_expr, Query *parse, Cache *hcache, Hypertable *hentry)
{
	RangeTblEntry *rte = rt_fetch(var_expr->varno, parse->rtable);
	char	   *varname = get_rte_attribute_name(rte, var_expr->varattno);

	if (rte->relid == hentry->replica_table)
	{
		/* get latest partition epoch: TODO scan all pe */
		PartitionEpoch *eps = hypertable_cache_get_partition_epoch(hcache, hentry, OPEN_END_TIME - 1, rte->relid);

		if (eps->partitioning != NULL &&
			strncmp(eps->partitioning->column, varname, NAMEDATALEN) == 0)
		{
			return eps->partitioning;
		}
	}
	return NULL;
}

/* Creates an expression for partioning_func(var_expr, partitioning_mod) = partioning_func(const_expr, partitioning_mod).
 * This function makes a copy of all nodes given in input.
 * */
static Expr *
create_partition_func_equals_const(Var *var_expr, Const *const_expr, char *partitioning_func_schema, char *partitioning_func, int32 partitioning_mod)
{
	Expr	   *op_expr;
	List	   *func_name = list_make2(makeString(partitioning_func_schema), makeString(partitioning_func));
	Var		   *var_for_fn_call;
	Const	   *const_for_fn_call;
	Const	   *mod_const_var_call;
	Const	   *mod_const_const_call;
	List	   *args_func_var;
	List	   *args_func_const;
	FuncCall   *fc_var;
	FuncCall   *fc_const;
	Node	   *f_var;
	Node	   *f_const;

	mod_const_var_call = makeConst(INT4OID,
								   -1,
								   InvalidOid,
								   sizeof(int32),
								   Int32GetDatum(partitioning_mod),
								   false,
								   true);

	mod_const_const_call = (Const *) palloc(sizeof(Const));
	memcpy(mod_const_const_call, mod_const_var_call, sizeof(Const));

	const_for_fn_call = (Const *) palloc(sizeof(Const));
	memcpy(const_for_fn_call, const_expr, sizeof(Const));

	var_for_fn_call = (Var *) palloc(sizeof(Var));
	memcpy(var_for_fn_call, var_expr, sizeof(Var));

	args_func_var = list_make2(var_for_fn_call, mod_const_var_call);
	args_func_const = list_make2(const_for_fn_call, mod_const_const_call);

	fc_var = makeFuncCall(func_name, args_func_var, -1);
	fc_const = makeFuncCall(func_name, args_func_const, -1);

	f_var = ParseFuncOrColumn(NULL, func_name, args_func_var, fc_var, -1);
	exprSetInputCollation(f_var, var_for_fn_call->varcollid);

	f_const = ParseFuncOrColumn(NULL, func_name, args_func_const, fc_const, -1);

	op_expr = make_op(NULL, list_make2(makeString("pg_catalog"), makeString("=")), f_var, f_const, -1);

	return op_expr;
}


/* Function to register dblink connections for remote commit/abort on local pre-commit/abort. */
PG_FUNCTION_INFO_V1(register_dblink_precommit_connection);
Datum
register_dblink_precommit_connection(PG_FUNCTION_ARGS)
{
	/*
	 * allocate this stuff in top-level transaction context, so that it
	 * survives till commit
	 */
	MemoryContext old = MemoryContextSwitchTo(TopTransactionContext);

	char	   *connectionName = text_to_cstring(PG_GETARG_TEXT_PP(0));

	callbackConnections = lappend(callbackConnections, connectionName);

	MemoryContextSwitchTo(old);
	PG_RETURN_VOID();
}


/*
 * Commits dblink connections registered with register_dblink_precommit_connection.
 * Look at meta_commands.sql for example usage. Remote commits happen in pre-commit.
 * Remote aborts happen on abort.
 * */
static void
io_xact_callback(XactEvent event, void *arg)
{
	ListCell   *cell;

	if (list_length(callbackConnections) == 0)
		return;

	switch (event)
	{
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_COMMIT:
			foreach(cell, callbackConnections)
			{
				char	   *connection = (char *) lfirst(cell);

				DirectFunctionCall3(dblink_exec,
								PointerGetDatum(cstring_to_text(connection)),
								  PointerGetDatum(cstring_to_text("COMMIT")),
									BoolGetDatum(true));		/* throw error */
				DirectFunctionCall1(dblink_disconnect, PointerGetDatum(cstring_to_text(connection)));
			}
			break;
		case XACT_EVENT_PARALLEL_ABORT:
		case XACT_EVENT_ABORT:

			/*
			 * Be quite careful here. Cannot throw any errors (or infinite
			 * loop) and cannot use PG_TRY either. Make sure to test with
			 * c-asserts on.
			 */
			foreach(cell, callbackConnections)
			{
				char	   *connection = (char *) lfirst(cell);

				DirectFunctionCall3(dblink_exec,
								PointerGetDatum(cstring_to_text(connection)),
									PointerGetDatum(cstring_to_text("ABORT")),
									BoolGetDatum(false));
				DirectFunctionCall1(dblink_disconnect, PointerGetDatum(cstring_to_text(connection)));
			}
			break;
		case XACT_EVENT_PRE_PREPARE:
		case XACT_EVENT_PREPARE:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot prepare a transaction that has open dblink constraint_exclusion_options")));
			break;
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_COMMIT:
			break;
		default:
			elog(ERROR, "unkown xact event: %d", event);
	}
	callbackConnections = NIL;
}

/* Function to get the local hostname. */
PG_FUNCTION_INFO_V1(pg_gethostname);
Datum
pg_gethostname(PG_FUNCTION_ARGS)
{
	text	   *t;
	long		hostname_max_len = sysconf(_SC_HOST_NAME_MAX);
	size_t		length;

	if (hostname_max_len == -1)
	{
		PG_RETURN_TEXT_P(NULL);
	}

	t = (text *) palloc(VARHDRSZ + hostname_max_len + 1);
	SET_VARSIZE(t, VARHDRSZ);
	memset(VARDATA(t), '\0', hostname_max_len + 1);

	if (gethostname((char *) VARDATA(t), hostname_max_len) == -1)
	{
		PG_RETURN_TEXT_P(NULL);
	}

	length = strnlen((char *) VARDATA(t), hostname_max_len);
	SET_VARSIZE(t, VARHDRSZ + length);

	PG_RETURN_TEXT_P(t);
}

/* Calls the default ProcessUtility */
void
prev_ProcessUtility(Node *parsetree,
					const char *queryString,
					ProcessUtilityContext context,
					ParamListInfo params,
					DestReceiver *dest,
					char *completionTag)
{
	if (prev_ProcessUtility_hook != NULL)
	{
		/* Call any earlier hooks */
		(prev_ProcessUtility_hook) (parsetree, queryString, context, params, dest, completionTag);
	}
	else
	{
		/* Call the standard */
		standard_ProcessUtility(parsetree, queryString, context, params, dest, completionTag);
	}

}

/* Hook-intercept for ProcessUtility. Used to make COPY use a temp copy table and */
/* blocking renaming of hypertables. */
void
timescaledb_ProcessUtility(Node *parsetree,
						   const char *queryString,
						   ProcessUtilityContext context,
						   ParamListInfo params,
						   DestReceiver *dest,
						   char *completionTag)
{
	if (!IobeamLoaded())
	{
		prev_ProcessUtility(parsetree, queryString, context, params, dest, completionTag);
		return;
	}

	if (IsA(parsetree, CopyStmt))
	{
		CopyStmt   *copystmt = (CopyStmt *) parsetree;
		Oid			relId = RangeVarGetRelid(copystmt->relation, NoLock, true);

		if (OidIsValid(relId))
		{
			Cache	   *hcache = hypertable_cache_pin();
			Hypertable *hentry = hypertable_cache_get_entry(hcache, relId);

			if (hentry != NULL)
			{
				copystmt->relation = makeRangeVarFromRelid(hentry->root_table);
			}
			cache_release(hcache);
		}
		prev_ProcessUtility((Node *) copystmt, queryString, context, params, dest, completionTag);
		return;
	}

	/* We don't support renaming hypertables yet so we need to block it */
	if (IsA(parsetree, RenameStmt))
	{
		RenameStmt *renamestmt = (RenameStmt *) parsetree;
		Oid			relId = RangeVarGetRelid(renamestmt->relation, NoLock, true);

		if (OidIsValid(relId))
		{
			Cache	   *hcache = hypertable_cache_pin();
			Hypertable *hentry = hypertable_cache_get_entry(hcache, relId);

			if (hentry != NULL && renamestmt->renameType == OBJECT_TABLE)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					   errmsg("Renaming hypertables is not yet supported")));
			cache_release(hcache);
		}
		prev_ProcessUtility((Node *) renamestmt, queryString, context, params, dest, completionTag);
		return;
	}

	prev_ProcessUtility(parsetree, queryString, context, params, dest, completionTag);
}
