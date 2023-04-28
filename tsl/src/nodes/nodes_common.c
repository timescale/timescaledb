/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <access/sysattr.h>
#include <catalog/pg_namespace.h>
#include <catalog/pg_operator.h>
#include <nodes/bitmapset.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/optimizer.h>
#include <optimizer/paths.h>
#include <optimizer/plancat.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/tlist.h>
#include <parser/parsetree.h>
#include <utils/builtins.h>
#include <utils/typcache.h>

#include "compression/compression.h"
#include "compression/create.h"
#include "nodes/nodes_common.h"

/* Searches and returns compression info for a specified column name
 */
FormData_hypertable_compression *
get_column_compressioninfo(List *hypertable_compression_info, char *column_name)
{
	ListCell *lc;

	foreach (lc, hypertable_compression_info)
	{
		FormData_hypertable_compression *fd = lfirst(lc);
		if (namestrcmp(&fd->attname, column_name) == 0)
			return fd;
	}
	elog(ERROR, "No compression information for column \"%s\" found.", column_name);

	pg_unreachable();
}

Node *
constify_tableoid_walker(Node *node, ConstifyTableOidContext *ctx)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, Var))
	{
		Var *var = castNode(Var, node);

		if ((Index) var->varno != ctx->chunk_index)
			return node;

		if (var->varattno == TableOidAttributeNumber)
		{
			ctx->made_changes = true;
			return (
				Node *) makeConst(OIDOID, -1, InvalidOid, 4, (Datum) ctx->chunk_relid, false, true);
		}

		/*
		 * we doublecheck system columns here because projection will
		 * segfault if any system columns get through
		 */
		if (var->varattno < SelfItemPointerAttributeNumber)
			elog(ERROR, "transparent decompression only supports tableoid system column");

		return node;
	}

	return expression_tree_mutator(node, constify_tableoid_walker, (void *) ctx);
}

/* Walk over the nodes and rewrites the tableoid as constant
 */
List *
constify_tableoid(List *node, Index chunk_index, Oid chunk_relid)
{
	ConstifyTableOidContext ctx = {
		.chunk_index = chunk_index,
		.chunk_relid = chunk_relid,
		.made_changes = false,
	};

	List *result = (List *) constify_tableoid_walker((Node *) node, &ctx);
	if (ctx.made_changes)
	{
		return result;
	}

	return node;
}

/* System columns other than tableoid will errored out
 */
void
check_for_system_columns(Bitmapset *attrs_used)
{
	int bit = bms_next_member(attrs_used, -1);
	if (bit > 0 && bit + FirstLowInvalidHeapAttributeNumber < 0)
	{
		/* we support tableoid so skip that */
		if (bit == TableOidAttributeNumber - FirstLowInvalidHeapAttributeNumber)
			bit = bms_next_member(attrs_used, bit);

		if (bit > 0 && bit + FirstLowInvalidHeapAttributeNumber < 0)
			elog(ERROR, "transparent decompression only supports tableoid system column");
	}
}

/* replace vars that reference the compressed table with ones that reference the
 * uncompressed one. Based on replace_nestloop_params
 */
Node *
replace_compressed_vars(Node *node, CompressionInfo *info)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, Var))
	{
		Var *var = (Var *) node;
		Var *new_var;
		char *colname;

		/* constify tableoid in quals */
		if ((Index) var->varno == info->chunk_rel->relid &&
			var->varattno == TableOidAttributeNumber)
			return (Node *)
				makeConst(OIDOID, -1, InvalidOid, 4, (Datum) info->chunk_rte->relid, false, true);

		/* Upper-level Vars should be long gone at this point */
		Assert(var->varlevelsup == 0);
		/* If not to be replaced, we can just return the Var unmodified */
		if ((Index) var->varno != info->compressed_rel->relid)
			return node;

		/* Create a decompressed Var to replace the compressed one */
		colname = get_attname(info->compressed_rte->relid, var->varattno, false);
		new_var = makeVar(info->chunk_rel->relid,
						  get_attnum(info->chunk_rte->relid, colname),
						  var->vartype,
						  var->vartypmod,
						  var->varcollid,
						  var->varlevelsup);

		if (!AttributeNumberIsValid(new_var->varattno))
			elog(ERROR, "cannot find column %s on decompressed chunk", colname);

		/* And return the replacement var */
		return (Node *) new_var;
	}
	if (IsA(node, PlaceHolderVar))
		elog(ERROR, "ignoring placeholders");

	return expression_tree_mutator(node, replace_compressed_vars, (void *) info);
}

/*
 * Find the resno of the given attribute in the provided target list
 */
AttrNumber
find_attr_pos_in_tlist(List *targetlist, AttrNumber pos)
{
	ListCell *lc;

	Assert(targetlist != NIL);
	Assert(pos > 0 && pos != InvalidAttrNumber);

	foreach (lc, targetlist)
	{
		TargetEntry *target = (TargetEntry *) lfirst(lc);

		if (!IsA(target->expr, Var))
			elog(ERROR, "compressed scan targetlist entries must be Vars");

		Var *var = castNode(Var, target->expr);
		AttrNumber compressed_attno = var->varattno;

		if (compressed_attno == pos)
			return target->resno;
	}

	elog(ERROR, "Unable to locate var %d in targetlist", pos);
	pg_unreachable();
}
