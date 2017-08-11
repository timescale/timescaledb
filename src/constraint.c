#include <postgres.h>
#include <nodes/parsenodes.h>
#include <nodes/makefuncs.h>
#include <catalog/pg_type.h>
#include <utils/lsyscache.h>
#include <utils/date.h>

#include "constraint.h"
#include "dimension.h"
#include "dimension_slice.h"
#include "chunk.h"
#include "utils.h"

static ColumnRef *
make_colref(const char *colname)
{
	ColumnRef  *cref = makeNode(ColumnRef);

	cref->fields = list_make1(makeString(pstrdup(colname)));
	cref->location = -1;
	return cref;
}

static TypeCast *
make_typecast(Node *arg, TypeName *type)
{
	TypeCast   *cast = makeNode(TypeCast);

	cast->arg = arg;
	cast->typeName = type;
	cast->location = -1;

	return cast;
}

static A_Const *
make_const(const char *str)
{
	A_Const    *con = makeNode(A_Const);

	con->val.type = T_String;
	con->val.val.str = pstrdup(str);

	return con;
}

static Datum
open_dimension_datum(Dimension *dim, int64 value)
{
	Datum		datum;

	switch (dim->fd.column_type)
	{
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			datum = DirectFunctionCall1(pg_unix_microseconds_to_timestamp, value);
			break;
		case DATEOID:
			datum = DirectFunctionCall1(pg_unix_microseconds_to_timestamp, value);
			datum = DirectFunctionCall1(timestamptz_date, datum);
			break;
		case INT2OID:
			datum = Int16GetDatum(value);
			break;
		case INT4OID:
			datum = Int32GetDatum(value);
			break;
		case INT8OID:
			datum = Int64GetDatum(value);
			break;
		default:
			elog(ERROR, "Unsupported type %u for open dimensions", dim->fd.column_type);
	}

	return datum;
}

static char *
open_dimension_value_str(Dimension *dim, int64 value)
{
	Datum		datum = open_dimension_datum(dim, value);
	Oid			typoutput;
	bool		typisvarlena;

	getTypeOutputInfo(dim->fd.column_type, &typoutput, &typisvarlena);

	return OidOutputFunctionCall(typoutput, datum);
}

static Node *
make_check_expr(Dimension *dim, DimensionSlice *slice, ColumnDef *coldef)
{
	const char *range_start = open_dimension_value_str(dim, slice->fd.range_start);
	const char *range_end = open_dimension_value_str(dim, slice->fd.range_end);
	TypeCast   *start_cast = make_typecast((Node *) make_const(range_start), coldef->typeName);
	TypeCast   *end_cast = make_typecast((Node *) make_const(range_end), coldef->typeName);
	A_Expr	   *start = makeA_Expr(AEXPR_OP, list_make1(makeString(pstrdup(">="))),
						  (Node *) make_colref(NameStr(dim->fd.column_name)),
								   (Node *) start_cast, -1);
	A_Expr	   *end = makeA_Expr(AEXPR_OP, list_make1(makeString(pstrdup("<"))),
						  (Node *) make_colref(NameStr(dim->fd.column_name)),
								 (Node *) end_cast, -1);

	return (Node *) makeBoolExpr(AND_EXPR, list_make2(start, end), -1);
}

Constraint *
constraint_make_dimension_check(Chunk *chunk, Dimension *dim, ColumnDef *coldef)
{
	DimensionSlice *slice = chunk_get_dimension_slice(chunk, dim->fd.id);
	ChunkConstraint *cc = chunk_get_dimension_constraint_by_slice_id(chunk, slice->fd.id);
	Constraint *constr;

	constr = makeNode(Constraint);
	constr->contype = CONSTR_CHECK;
	constr->conname = NameStr(cc->fd.constraint_name);
	constr->raw_expr = make_check_expr(dim, slice, coldef);
	constr->initially_valid = true;
	constr->skip_validation = true;

	return constr;
}
