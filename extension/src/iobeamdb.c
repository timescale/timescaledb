#include "postgres.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
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
#include "executor/spi.h"
#include "commands/extension.h"
#include "tcop/tcopprot.h"
#include "deps/dblink.h"

#include "access/xact.h"
#include "parser/parse_oper.h"
#include "parser/parse_func.h"

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

static List *callbackConnections = NIL;

typedef struct hypertable_info
{
	Oid replica_oid;
	List *partitioning_info;
} hypertable_info;

typedef struct partitioning_info
{
	Name partitioning_field;
	Name partitioning_func;
	int32 partitioning_mod;
} partitioning_info;

typedef struct change_table_name_context
{
	List *hypertable_info;
} change_table_name_context;

typedef struct add_partitioning_func_qual_context
{
	Query *parse;
	List  *hypertable_info_list;
} add_partitioning_func_qual_context;


hypertable_info *get_hypertable_info(Oid mainRelationOid);
static void add_partitioning_func_qual(Query *parse, List *hypertable_info_list);
static Node *add_partitioning_func_qual_mutator(Node *node, add_partitioning_func_qual_context *context);
static partitioning_info *
get_partitioning_info_for_partition_field_var(Var *var_expr, Query *parse, List * hypertable_info_list);
static Expr *
create_partition_func_equals_const(Var *var_expr, Const *const_expr, Name partitioning_func, int32 partitioning_mod);

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

	if (IobeamLoaded())
	{
		char* printParse = GetConfigOptionByName("io.print_parse", NULL, true);
		/* set to false to not print all internal actions */
		SetConfigOption("io.print_parse", "false", PGC_USERSET, PGC_S_SESSION);


		/* replace call to main table with call to the replica table */
		if (parse->commandType ==  CMD_SELECT)
		{
			change_table_name_context context;
			context.hypertable_info = NIL;
			change_table_name_walker((Node *) parse, &context);
			if (list_length(context.hypertable_info) > 0)
			{
				add_partitioning_func_qual(parse, context.hypertable_info);
			}
		}

		if (printParse != NULL && strcmp(printParse, "true") == 0)
		{
			pprint(parse);
		}

	}

	if (prev_planner_hook != NULL)
	{
		/* Call any earlier hooks */
		rv = (prev_planner_hook)(parse, cursorOptions, boundParams);
	} else
	{
		/* Call the standard planner */
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
			hypertable_info* hinfo = get_hypertable_info(rangeTableEntry->relid);
			if (hinfo != NULL)
			{
				change_table_name_context* ctx = (change_table_name_context *)context;
				ctx->hypertable_info = lappend(ctx->hypertable_info, hinfo);
				rangeTableEntry->relid = hinfo->replica_oid;

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
 * Use the default_replica_node to look up the hypertable_info for a replica table from the oid of the main table.
 * TODO: make this use a cache instead of a db lookup every time.
 *
 * */
hypertable_info *
get_hypertable_info(Oid mainRelationOid)
{
	Oid namespace = get_rel_namespace(mainRelationOid);
	Oid hypertable_meta = get_relname_relid("hypertable", get_namespace_oid("public", false));
	char *tableName = get_rel_name(mainRelationOid);
	char *schemaName = get_namespace_name(namespace);
	StringInfo sql = makeStringInfo();
	int ret;


	/* prevents infinite recursion, don't check hypertable meta tables */
	if (
		hypertable_meta == InvalidOid
		|| namespace == PG_CATALOG_NAMESPACE
		|| mainRelationOid == hypertable_meta
		|| mainRelationOid ==  get_relname_relid("hypertable_replica", get_namespace_oid("public", false))
		|| mainRelationOid ==  get_relname_relid("partition_epoch", get_namespace_oid("public", false))
		|| mainRelationOid ==  get_relname_relid("default_replica_node", get_namespace_oid("public", false))
	   )
	{
		return NULL;
	}

	appendStringInfo(sql, HYPERTABLE_INFO_QUERY, schemaName, tableName);

	SPI_connect();

	ret = SPI_execute(sql->data, true, 0);

	if (ret <= 0)
	{
		elog(ERROR, "Got an SPI error");
	}

	if (SPI_processed > 0)
	{
		bool isnull;
		int total_rows = SPI_processed;
		int j;
		/* do not populate list until SPI_finish because the list cannot be populated in the SPI memory context */
		List *partitioning_info_list;
		/* used to track  list stuff til list can be populated */
		partitioning_info **partitioning_info_array = SPI_palloc(total_rows * sizeof(partitioning_info *));
		hypertable_info *hinfo = SPI_palloc(sizeof(hypertable_info));

		TupleDesc tupdesc = SPI_tuptable->tupdesc;
		HeapTuple tuple =  SPI_tuptable->vals[0];
		hinfo->replica_oid = DatumGetObjectId(SPI_getbinval(tuple, tupdesc, 1, &isnull));

		for (j = 0; j < total_rows; j++)
		{
			HeapTuple tuple = SPI_tuptable->vals[j];
			Name partitioning_field = DatumGetName(SPI_getbinval(tuple, tupdesc, 2, &isnull));
			Name partitioning_func =  DatumGetName(SPI_getbinval(tuple, tupdesc, 3, &isnull));
			int32 partitioning_mod =  DatumGetInt32(SPI_getbinval(tuple, tupdesc, 4, &isnull));
			partitioning_info* info = (partitioning_info *) SPI_palloc(sizeof(partitioning_info));

			info->partitioning_field = SPI_palloc(sizeof(NameData));
			memcpy(info->partitioning_field, partitioning_field,  sizeof(NameData));

			info->partitioning_func = SPI_palloc(sizeof(NameData));
			memcpy(info->partitioning_func, partitioning_func,  sizeof(NameData));

			info->partitioning_mod = partitioning_mod;

			partitioning_info_array[j] = info;
		}

		SPI_finish();

		partitioning_info_list = NIL;
		for (j = 0; j < total_rows; j++)
		{
			partitioning_info_list = lappend(partitioning_info_list, partitioning_info_array[j]);
		}
		pfree(partitioning_info_array);

		hinfo->partitioning_info = partitioning_info_list;
		return hinfo;
	}
	SPI_finish();
	return NULL;
}

/*
 * This function does a transformation that allows postgres's native constraint exclusion to exclude space partititions when
 * the query contains equivalence qualifiers on the space partition key.
 *
 * This function goes through the upper-level qual of a parse tree and finds quals of the form:
 * 		partitioning_field = const
 * It transforms them into the qual:
 * 		partitioning_field = const AND partitioning_func(partition_field, partitioning_mod) = partitioning_func(const, partitioning_mod)
 *
 * This tranformation helps because the check constraint on a table is of the form CHECK(partitioning_func(partition_field, partitioning_mod) BETWEEN X AND Y).
 */
static void
add_partitioning_func_qual(Query *parse, List* hypertable_info_list)
{
	add_partitioning_func_qual_context context;
	context.parse = parse;
	context.hypertable_info_list = hypertable_info_list;

	parse->jointree->quals = add_partitioning_func_qual_mutator(parse->jointree->quals, &context);
}

static Node *
add_partitioning_func_qual_mutator(Node *node, add_partitioning_func_qual_context *context)
{
	if (node == NULL)
		return NULL;

	/* Detect partitioning_field = const. If not fall-thru.
	 * If detected, replace with
	 *    partitioning_field = const AND
	 *    partitioning_func(partition_field, partitioning_mod) = partitioning_func(const, partitioning_mod)
	 */
	if (IsA(node, OpExpr))
	{
		OpExpr *exp = (OpExpr *) node;
		if (list_length(exp->args) == 2)
		{
			//only look at var op const or const op var;
			Node *left = (Node *) linitial(exp->args);
			Node *right = (Node *) lsecond(exp->args);
			Var *var_expr = NULL;
			Node *other_expr = NULL;

			if (IsA(left, Var))
			{
				var_expr = (Var *) left;
				other_expr = right;
			} else if (IsA(right, Var))
			{
				var_expr = (Var *)right;
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
					Const *const_expr = (Const *) other_expr;
					Oid eq_oid = OpernameGetOprid(list_make2(makeString("pg_catalog"), makeString("=")), exprType(left), exprType(right));

					if (eq_oid == exp->opno)
					{
						/* I now have a var = const. Make sure var is a partitioning field */
						partitioning_info *pi = get_partitioning_info_for_partition_field_var(var_expr,
																							 context->parse,
																							 context->hypertable_info_list);

						if (pi != NULL) {
							/* The var is a partitioning field */
							Expr * partitioning_clause = create_partition_func_equals_const(var_expr, const_expr,
																							pi->partitioning_func,
																							pi->partitioning_mod);

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

/* Returns the partitioning info for a var if the var is a partitioning field. If the var is not a partitioning
 * field return NULL */
static partitioning_info *
get_partitioning_info_for_partition_field_var(Var *var_expr, Query *parse, List * hypertable_info_list) {
	RangeTblEntry *rte = rt_fetch(var_expr->varno, parse->rtable);
	char *varname = get_rte_attribute_name(rte, var_expr->varattno);
	ListCell *hicell;
	foreach(hicell, hypertable_info_list)
	{
		hypertable_info *info = lfirst(hicell);
		if (rte->relid == info->replica_oid)
		{
			ListCell *picell;
			foreach(picell, info->partitioning_info)
			{
				partitioning_info *pi = lfirst(picell);
				if (strcmp(NameStr(*(pi->partitioning_field)), varname)==0)
				{
					return pi;
				}

			}
		}
	}
	return NULL;
}

/* Creates an expression for partioning_func(var_expr, partitioning_mod) = partioning_func(const_expr, partitioning_mod).
 * This function makes a copy of all nodes given in input.
 * */
static Expr *
create_partition_func_equals_const(Var *var_expr, Const *const_expr, Name partitioning_func, int32 partitioning_mod)
{
	Expr *op_expr;
	List *func_name = list_make1(makeString(NameStr(*(partitioning_func))));
	Var *var_for_fn_call;
	Const *const_for_fn_call;
	Const *mod_const_var_call;
	Const *mod_const_const_call;
	List *args_func_var;
	List *args_func_const;
	FuncCall *fc_var;
	FuncCall *fc_const;
	Node *f_var;
	Node *f_const;

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
	f_const = ParseFuncOrColumn(NULL, func_name, args_func_const, fc_const, -1);

	op_expr = make_op(NULL,list_make2(makeString("pg_catalog"), makeString("=")),f_var,f_const,-1);

	return op_expr;
}


/* Function to register dblink connections for remote commit/abort on local pre-commit/abort. */
PG_FUNCTION_INFO_V1(register_dblink_precommit_connection);
Datum
register_dblink_precommit_connection(PG_FUNCTION_ARGS)
{
	/* allocate this stuff in top-level transaction context, so that it survives till commit */
	MemoryContext old = MemoryContextSwitchTo(TopTransactionContext);

	char *connectionName = text_to_cstring(PG_GETARG_TEXT_PP(0));
	callbackConnections = lappend(callbackConnections, connectionName);

	MemoryContextSwitchTo(old);
	PG_RETURN_VOID();
}


/*
 * Commits dblink connections registered with register_dblink_precommit_connection.
 * Look at meta_commands.sql for example usage. Remote commits happen in pre-commit.
 * Remote aborts happen on abort.
 * */
static void io_xact_callback(XactEvent event, void *arg)
{
	ListCell *cell;
	
	if(list_length(callbackConnections) == 0)
		return;

	switch (event)
	{
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_COMMIT:
			foreach (cell, callbackConnections)
			{
				char *connection = (char *) lfirst(cell);
				DirectFunctionCall3(dblink_exec,
									PointerGetDatum(cstring_to_text(connection)),
									PointerGetDatum(cstring_to_text("COMMIT")),
									BoolGetDatum(true)); /* throw error */
				DirectFunctionCall1(dblink_disconnect, PointerGetDatum(cstring_to_text(connection)));
			}
			break;
		case XACT_EVENT_PARALLEL_ABORT:
		case XACT_EVENT_ABORT:
			/* Be quite careful here. Cannot throw any errors (or infinite loop) and cannot use PG_TRY either.
			 * Make sure to test with c-asserts on. */
			foreach (cell, callbackConnections)
			{
				char *connection = (char *) lfirst(cell);
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

