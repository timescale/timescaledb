/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/htup_details.h>
#include <catalog/pg_collation.h>
#include <catalog/pg_database.h>
#include <commands/dbcommands.h>
#include <common/int.h>
#include <commands/defrem.h>
#include <miscadmin.h>
#include <fmgr.h>
#include <funcapi.h>
#include <nodes/makefuncs.h>
#include <nodes/value.h>
#include <nodes/miscnodes.h>
#include <utils/acl.h>
#include <utils/snapmgr.h>
#include <utils/builtins.h>
#include <utils/jsonb.h>
#include <utils/lsyscache.h>
#include <utils/tuplestore.h>

#include "export.h"
#include "guc.h"
#include "license_guc.h"
#include "scan_iterator.h"
#include "scanner.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/storage_policy.h"
#include "with_clause/create_table_with_clause.h"

#define STORAGE_POLICY_MATCH_EXACT "exact"

static bool
regex_matches(const char *pattern, const char *value)
{
	char *anchored = psprintf("^(?:%s)$", pattern);

	return DatumGetBool(DirectFunctionCall2Coll(textregexeq,
												DEFAULT_COLLATION_OID,
												CStringGetTextDatum(value),
												CStringGetTextDatum(anchored)));
}

static int
storage_policy_cmp(const ListCell *a, const ListCell *b)
{
	const StoragePolicy *pa = lfirst(a);
	const StoragePolicy *pb = lfirst(b);

	/* an exact policy before any regex one, then the lowest priority value first */
	if (pa->is_exact != pb->is_exact)
		return pa->is_exact ? -1 : 1;
	return (pa->priority > pb->priority) - (pa->priority < pb->priority);
}

/*
 * This is the one definition of which storage policies apply to a name and in which order.
 * CREATE TABLE uses the first, and the SQL functions that show the order or look for overlaps
 * are built on this too, so they cannot disagree with it.
 */
List *
ts_storage_policy_find_all(const char *schema_name, const char *table_name)
{
	List *matches = NIL;
	ScanIterator iterator =
		ts_scan_iterator_create(STORAGE_POLICY, AccessShareLock, CurrentMemoryContext);

	/*
	 * The scanner's default is an instant snapshot, which can miss both the old and the new row
	 * of a policy that is changed at the same time, and then the table silently gets no policy.
	 * A snapshot of the committed state sees one of the two.
	 */
	Snapshot snapshot = RegisterSnapshot(GetLatestSnapshot());
	iterator.ctx.snapshot = snapshot;

	/* The table is small and rows are matched by pattern, so scan it all. */
	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		bool should_free;
		HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
		Datum values[Natts_storage_policy];
		bool nulls[Natts_storage_policy];

		heap_deform_tuple(tuple, ts_scanner_get_tupledesc(ti), values, nulls);

		if (DatumGetBool(values[AttrNumberGetAttrOffset(Anum_storage_policy_enabled)]))
		{
			const char *kind =
				TextDatumGetCString(values[AttrNumberGetAttrOffset(Anum_storage_policy_match_kind)]);
			const char *schema_pattern = TextDatumGetCString(
				values[AttrNumberGetAttrOffset(Anum_storage_policy_schema_name)]);
			const char *table_pattern = TextDatumGetCString(
				values[AttrNumberGetAttrOffset(Anum_storage_policy_table_name)]);
			bool is_exact = strcmp(kind, STORAGE_POLICY_MATCH_EXACT) == 0;
			bool is_match;

			if (is_exact)
				is_match = strcmp(schema_pattern, schema_name) == 0 &&
						   strcmp(table_pattern, table_name) == 0;
			else
				is_match = !nulls[AttrNumberGetAttrOffset(Anum_storage_policy_priority)] &&
						   regex_matches(schema_pattern, schema_name) &&
						   regex_matches(table_pattern, table_name);

			if (is_match)
			{
				StoragePolicy *policy = palloc0(sizeof(StoragePolicy));

				policy->id = DatumGetInt32(values[AttrNumberGetAttrOffset(Anum_storage_policy_id)]);
				namestrcpy(&policy->name,
						   NameStr(*DatumGetName(
							   values[AttrNumberGetAttrOffset(Anum_storage_policy_name)])));
				policy->policy = DatumGetJsonbPCopy(
					values[AttrNumberGetAttrOffset(Anum_storage_policy_policy)]);
				policy->is_exact = is_exact;
				if (!is_exact)
					policy->priority = DatumGetInt32(
						values[AttrNumberGetAttrOffset(Anum_storage_policy_priority)]);
				matches = lappend(matches, policy);
			}
		}

		if (should_free)
			heap_freetuple(tuple);
	}
	ts_scan_iterator_close(&iterator);
	UnregisterSnapshot(snapshot);

	list_sort(matches, storage_policy_cmp);
	return matches;
}

bool
ts_storage_policy_find(const char *schema_name, const char *table_name, StoragePolicy *policy)
{
	List *matches = ts_storage_policy_find_all(schema_name, table_name);

	if (matches == NIL)
		return false;

	*policy = *(StoragePolicy *) linitial(matches);
	return true;
}

static DefElem *
make_option(const char *name, Node *arg)
{
	return makeDefElem(pstrdup(name), arg, -1) /* namespace is set by the caller */;
}

static DefElem *
make_tsdb_option(const char *name, Node *arg)
{
	DefElem *def = make_option(name, arg);
	def->defnamespace = pstrdup("tsdb");
	return def;
}

static char *
policy_value_to_string(const char *path, const JsonbValue *value)
{
	switch (value->type)
	{
		case jbvString:
			return pnstrdup(value->val.string.val, value->val.string.len);
		case jbvNumeric:
			return DatumGetCString(
				DirectFunctionCall1(numeric_out, NumericGetDatum(value->val.numeric)));
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid storage policy: \"%s\" must be a string or a number", path)));
	}
	pg_unreachable();
}

static bool
policy_value_to_bool(const char *path, const JsonbValue *value)
{
	if (value->type != jbvBool)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid storage policy: \"%s\" must be true or false", path)));
	return value->val.boolean;
}

/*
 * Walk one level of a JSON object. For each key the callback is handed the key, the value and,
 * for nested objects, an iterator over the nested object.
 */
typedef void (*PolicyKeyFn)(const char *key, JsonbValue *value, StoragePolicyOptions *out);

static void
policy_foreach_key(JsonbContainer *container, const char *path, PolicyKeyFn fn, StoragePolicyOptions *out)
{
	JsonbIterator *it = JsonbIteratorInit(container);
	JsonbValue v;
	JsonbIteratorToken tok = JsonbIteratorNext(&it, &v, false);

	if (tok != WJB_BEGIN_OBJECT)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid storage policy: \"%s\" must be a JSON object", path)));

	while ((tok = JsonbIteratorNext(&it, &v, true)) != WJB_END_OBJECT)
	{
		Assert(tok == WJB_KEY);
		char *key = pnstrdup(v.val.string.val, v.val.string.len);
		JsonbIteratorNext(&it, &v, true);
		fn(key, &v, out);
	}
}

/* The text of a column name or a column list, which cannot be empty. */
static char *
policy_value_to_column_text(const char *path, const JsonbValue *value)
{
	char *text;

	if (value->type != jbvString)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid storage policy: \"%s\" must be a string", path)));

	text = pnstrdup(value->val.string.val, value->val.string.len);
	if (strspn(text, " \t\n") == strlen(text))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid storage policy: \"%s\" cannot be empty", path)));
	return text;
}

static void
columnstore_key(const char *key, JsonbValue *value, StoragePolicyOptions *out)
{
	if (strcmp(key, "segmentby") == 0 || strcmp(key, "orderby") == 0 || strcmp(key, "index") == 0)
	{
		char path[64];
		snprintf(path, sizeof(path), "columnstore.%s", key);
		out->with_options =
			lappend(out->with_options,
					make_tsdb_option(key, (Node *) makeString(policy_value_to_column_text(path, value))));
		return;
	}

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("invalid storage policy: unknown key \"columnstore.%s\"", key),
			 errhint("Valid keys are \"segmentby\", \"orderby\" and \"index\".")));
}

typedef struct AgeSpec
{
	char *mode;
	char *after;
} AgeSpec;

static AgeSpec age_spec;

static void
age_key(const char *key, JsonbValue *value, StoragePolicyOptions *out)
{
	if (strcmp(key, "mode") == 0)
		age_spec.mode = policy_value_to_string("mode", value);
	else if (strcmp(key, "after") == 0)
		age_spec.after = policy_value_to_string("after", value);
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid storage policy: unknown key \"%s\"", key),
				 errhint("Valid keys are \"mode\" and \"after\".")));
}

/* Parse interval input without raising the error, which is a soft error where Postgres allows it. */
static bool
interval_input_ok(const char *str, Datum *result)
{
	ErrorSaveContext escontext = { .type = T_ErrorSaveContext };

	return DirectInputFunctionCallSafe(interval_in,
									   (char *) str,
									   InvalidOid,
									   -1,
									   (Node *) &escontext,
									   result);
}

static bool
int8_input_ok(const char *str, Datum *result)
{
	ErrorSaveContext escontext = { .type = T_ErrorSaveContext };

	return DirectInputFunctionCallSafe(int8in, (char *) str, InvalidOid, -1, (Node *) &escontext, result);
}

/*
 * An age must be a positive interval, or a positive integer for an integer time column. An
 * integer is also valid interval input, so one check covers both. Whether it fits the time column
 * can only be checked when a table is created.
 */
static void
validate_age(const char *path, const char *after)
{
	static const Interval zero = { 0 };
	Datum age = 0;

	if (!interval_input_ok(after, &age))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid storage policy: \"%s\" of \"%s\" is not a valid age", after, path),
				 errhint("Use an interval such as '7 days', or an integer for an integer time "
						 "column.")));

	if (DatumGetBool(DirectFunctionCall2(interval_le, age, IntervalPGetDatum(&zero))))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid storage policy: \"%s\" of \"%s\" must be greater than zero",
						after,
						path)));
}

/*
 * A chunk interval is a positive interval for a time column, or a positive integer for an integer
 * column. Whether it fits the column can only be checked when a table is created.
 */
static void
validate_chunk_interval(const char *chunk_interval)
{
	static const Interval zero = { 0 };
	Datum value = 0;

	if (int8_input_ok(chunk_interval, &value))
	{
		if (DatumGetInt64(value) <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid storage policy: \"chunk_interval\" of \"%s\" must be greater "
							"than zero",
							chunk_interval)));
		return;
	}

	if (!interval_input_ok(chunk_interval, &value))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid storage policy: \"chunk_interval\" of \"%s\" is not a valid "
						"interval",
						chunk_interval),
				 errhint("Use an interval such as '1 day', or an integer for an integer time "
						 "column.")));

	if (DatumGetBool(DirectFunctionCall2(interval_le, value, IntervalPGetDatum(&zero))))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid storage policy: \"chunk_interval\" of \"%s\" must be greater than "
						"zero",
						chunk_interval)));
}

/*
 * Parse {"mode": "NEVER"} or {"mode": "AFTER", "after": <age>}. Returns the age, or NULL for
 * NEVER.
 */
static char *
parse_age_object(const char *name, JsonbValue *value, StoragePolicyOptions *out)
{
	age_spec.mode = NULL;
	age_spec.after = NULL;

	if (value->type != jbvBinary)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid storage policy: \"%s\" must be a JSON object", name)));

	policy_foreach_key(value->val.binary.data, name, age_key, out);

	if (age_spec.mode == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid storage policy: \"%s\" requires \"mode\"", name)));

	if (pg_strcasecmp(age_spec.mode, "NEVER") == 0)
	{
		if (age_spec.after != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid storage policy: \"%s\" mode NEVER cannot have \"after\"",
							name)));
		return NULL;
	}

	if (pg_strcasecmp(age_spec.mode, "AFTER") != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid storage policy: unknown mode \"%s\" for \"%s\"",
						age_spec.mode,
						name),
				 errhint("Valid modes are NEVER and AFTER.")));

	if (age_spec.after == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid storage policy: \"%s\" mode AFTER requires \"after\"", name)));

	validate_age(name, age_spec.after);
	return age_spec.after;
}

static void
compression_key(const char *name, JsonbValue *value, StoragePolicyOptions *out)
{
	char *after = parse_age_object(name, value, out);

	out->with_options =
		lappend(out->with_options,
				make_tsdb_option("compress_after", (Node *) makeString(after ? after : pstrdup("never"))));
}

/* NEVER and an absent key both schedule no retention job, but only NEVER is something to compare. */
static void
retention_key(const char *name, JsonbValue *value, StoragePolicyOptions *out)
{
	out->retention_after = parse_age_object(name, value, out);
	out->retention_never = (out->retention_after == NULL);
}

/*
 * Columnstore and compression are not part of the Apache build, so a policy that sets them is
 * rejected when it is stored, as the CREATE TABLE it would apply to must not fail halfway.
 */
static void
require_timescale_license(const char *key)
{
	if (ts_license_is_apache())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid storage policy: \"%s\" is not supported under the current "
						"\"%s\" license",
						key,
						ts_guc_license),
				 errhint("Upgrade your license to 'timescale' to use this feature.")));
}

static void
top_level_key(const char *key, JsonbValue *value, StoragePolicyOptions *out)
{
	if (strcmp(key, "partition_column") == 0)
	{
		char *column = policy_value_to_column_text(key, value);

		if (strlen(column) >= NAMEDATALEN)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid storage policy: \"partition_column\" is longer than %d bytes",
							NAMEDATALEN - 1)));
		out->with_options =
			lappend(out->with_options, make_tsdb_option(key, (Node *) makeString(column)));
	}
	else if (strcmp(key, "chunk_interval") == 0)
	{
		char *chunk_interval = policy_value_to_string(key, value);

		validate_chunk_interval(chunk_interval);
		out->with_options =
			lappend(out->with_options, make_tsdb_option(key, (Node *) makeString(chunk_interval)));
	}
	else if (strcmp(key, "create_default_indexes") == 0)
	{
		out->with_options = lappend(out->with_options,
						   make_tsdb_option(key,
											(Node *) makeBoolean(policy_value_to_bool(key, value))));
	}
	else if (strcmp(key, "columnstore") == 0)
	{
		require_timescale_license(key);
		if (value->type != jbvBinary)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid storage policy: \"columnstore\" must be a JSON object")));
		policy_foreach_key(value->val.binary.data, "columnstore", columnstore_key, out);
	}
	else if (strcmp(key, "compression") == 0)
	{
		require_timescale_license(key);
		compression_key(key, value, out);
	}
	else if (strcmp(key, "retention") == 0)
	{
		require_timescale_license(key);
		retention_key(key, value, out);
	}
	else if (strcmp(key, "tiering") == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid storage policy: \"%s\" is not supported yet", key)));
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid storage policy: unknown key \"%s\"", key),
				 errhint("Valid keys are \"partition_column\", \"chunk_interval\", "
						 "\"create_default_indexes\", \"columnstore\", \"compression\" and \"retention\".")));
	}
}

StoragePolicyOptions *
ts_storage_policy_to_options(const Jsonb *policy)
{
	StoragePolicyOptions *out = palloc0(sizeof(StoragePolicyOptions));

	/* A matching policy always means a hypertable. */
	out->with_options =
		lappend(out->with_options, make_tsdb_option("hypertable", (Node *) makeBoolean(true)));
	policy_foreach_key((JsonbContainer *) &policy->root, "policy", top_level_key, out);

	return out;
}

TS_FUNCTION_INFO_V1(ts_storage_policy_validate);

Datum
ts_storage_policy_validate(PG_FUNCTION_ARGS)
{
	Jsonb *policy = PG_GETARG_JSONB_P(0);

	(void) ts_storage_policy_to_options(policy);

	PG_RETURN_VOID();
}

TS_FUNCTION_INFO_V1(ts_storage_policy_check_owner);

/*
 * GetOuterUserId() is the role the session runs as, including SET ROLE, but not the
 * switch SECURITY DEFINER functions make. object_ownercheck() is true for superusers and
 * for members of the owning role.
 */
Datum
ts_storage_policy_check_owner(PG_FUNCTION_ARGS)
{
	if (!object_ownercheck(DatabaseRelationId, MyDatabaseId, GetOuterUserId()))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be owner of database \"%s\" or a superuser to manage storage "
						"policies",
						get_database_name(MyDatabaseId))));

	PG_RETURN_VOID();
}

TS_FUNCTION_INFO_V1(ts_storage_policy_caller);

/* The role the session runs as, which is who made a change, and not the function owner. */
Datum
ts_storage_policy_caller(PG_FUNCTION_ARGS)
{
	Name name = (Name) palloc0(NAMEDATALEN);

	namestrcpy(name, GetUserNameFromId(GetOuterUserId(), false));
	PG_RETURN_NAME(name);
}

TS_FUNCTION_INFO_V1(ts_storage_policy_matches);

/* The ids of the policies a new table with this name matches, with the rank in the order. */
Datum
ts_storage_policy_matches(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int32 rank = 0;
	ListCell *lc;

	InitMaterializedSRF(fcinfo, 0);

	foreach (lc, ts_storage_policy_find_all(NameStr(*PG_GETARG_NAME(0)),
											NameStr(*PG_GETARG_NAME(1))))
	{
		Datum values[2] = { Int32GetDatum(++rank), Int32GetDatum(((StoragePolicy *) lfirst(lc))->id) };
		bool nulls[2] = { false, false };

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	PG_RETURN_NULL();
}

TS_FUNCTION_INFO_V1(ts_storage_policy_explain);

static void
explain_put_row(Tuplestorestate *store, TupleDesc tupdesc, const char *setting, const char *value,
				const char *source)
{
	Datum values[3] = { CStringGetTextDatum(setting),
						value ? CStringGetTextDatum(value) : (Datum) 0,
						CStringGetTextDatum(source) };
	bool nulls[3] = { false, value == NULL, false };

	tuplestore_putvalues(store, tupdesc, values, nulls);
}

/* The options a statement would give as WITH (tsdb.<key> = <value>, ...). */
static List *
explain_statement_options(const Jsonb *options)
{
	List *list = NIL;
	JsonbIterator *it = JsonbIteratorInit((JsonbContainer *) &options->root);
	JsonbValue v;
	JsonbIteratorToken tok = JsonbIteratorNext(&it, &v, false);

	if (tok != WJB_BEGIN_OBJECT)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("options must be a JSON object"),
				 errhint("Pass the WITH options as a JSON object, for example "
						 "'{\"chunk_interval\": \"1 day\"}'.")));

	while ((tok = JsonbIteratorNext(&it, &v, true)) != WJB_END_OBJECT)
	{
		char *key = pnstrdup(v.val.string.val, v.val.string.len);
		Node *arg;

		JsonbIteratorNext(&it, &v, true);
		if (v.type == jbvBool)
			arg = (Node *) makeBoolean(v.val.boolean);
		else if (v.type == jbvString || v.type == jbvNumeric)
			arg = (Node *) makeString(policy_value_to_string(key, &v));
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("option \"%s\" must be a string, a number or true or false", key)));
		list = lappend(list, make_tsdb_option(key, arg));
	}

	return list;
}

static char *
explain_value_to_string(const WithClauseResult *clause)
{
	Oid outfunc;
	bool is_varlena;

	getTypeOutputInfo(clause->definition->type_id, &outfunc, &is_varlena);
	return OidOutputFunctionCall(outfunc, clause->parsed);
}

/*
 * What CREATE TABLE <schema_name>.<table_name> WITH (<options>) would do, as one row per setting
 * and where it comes from. The lookup and the merge are the ones CREATE TABLE uses. What cannot
 * be known from a name alone is not covered: temporary tables, partitions and tables created
 * with INHERITS or OF never get a policy.
 */
Datum
ts_storage_policy_explain(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	StoragePolicy policy;
	WithClauseResult *statement = NULL;
	WithClauseResult *result;
	const char *none_reason = NULL;
	Size count = ts_create_table_with_clause_count();

	InitMaterializedSRF(fcinfo, 0);

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	const char *schema_name = NameStr(*PG_GETARG_NAME(0));
	const char *table_name = NameStr(*PG_GETARG_NAME(1));

	if (!PG_ARGISNULL(2))
	{
		List *list = explain_statement_options(PG_GETARG_JSONB_P(2));

		if (list != NIL)
			statement = ts_create_table_with_clause_parse(list);
	}

	/*
	 * Drift and apply ask with the fourth argument true, as they are asked for by name and the
	 * setting is for the tables a session creates.
	 */
	bool ignore_guc = PG_NARGS() > 3 && !PG_ARGISNULL(3) && PG_GETARG_BOOL(3);

	if (!ts_guc_enable_storage_policies && !ignore_guc)
		none_reason = "timescaledb.enable_storage_policies is off";
	else if (statement != NULL && !statement[CreateTableFlagHypertable].is_default &&
			 !DatumGetBool(statement[CreateTableFlagHypertable].parsed))
		none_reason = "the statement sets tsdb.hypertable = false";
	else if (!ts_storage_policy_find(schema_name, table_name, &policy))
		none_reason = "no policy matches";

	if (none_reason != NULL)
	{
		explain_put_row(rsinfo->setResult,
						rsinfo->setDesc,
						"storage_policy",
						NULL,
						psprintf("none: %s", none_reason));
		result = statement;
	}
	else
	{
		StoragePolicyOptions *options = ts_storage_policy_to_options(policy.policy);
		WithClauseResult *from_policy = ts_create_table_with_clause_parse(options->with_options);

		explain_put_row(rsinfo->setResult,
						rsinfo->setDesc,
						"storage_policy",
						NameStr(policy.name),
						policy.is_exact ? "exact match" :
										  psprintf("regex match, priority %d", policy.priority));
		result = ts_create_table_with_clause_overlay(statement, from_policy);
	}

	for (Size i = 0; result != NULL && i < count; i++)
	{
		if (result[i].is_default)
			continue;

		explain_put_row(rsinfo->setResult,
						rsinfo->setDesc,
						psprintf("tsdb.%s", result[i].definition->arg_names[0]),
						explain_value_to_string(&result[i]),
						(statement != NULL && !statement[i].is_default) ? "statement" : "policy");
	}

	if (none_reason == NULL)
	{
		StoragePolicyOptions *options = ts_storage_policy_to_options(policy.policy);

		if (options->retention_after != NULL || options->retention_never)
			explain_put_row(rsinfo->setResult,
							rsinfo->setDesc,
							"retention_after",
							options->retention_after != NULL ? options->retention_after : "never",
							"policy");
	}

	PG_RETURN_NULL();
}
