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
#include <commands/defrem.h>
#include <miscadmin.h>
#include <fmgr.h>
#include <nodes/makefuncs.h>
#include <nodes/value.h>
#include <utils/acl.h>
#include <utils/builtins.h>
#include <utils/jsonb.h>

#include "export.h"
#include "guc.h"
#include "license_guc.h"
#include "scan_iterator.h"
#include "scanner.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/storage_policy.h"

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

bool
ts_storage_policy_find(const char *schema_name, const char *table_name, StoragePolicy *policy)
{
	bool found = false;
	bool found_exact = false;
	int32 best_priority = 0;
	ScanIterator iterator =
		ts_scan_iterator_create(STORAGE_POLICY, AccessShareLock, CurrentMemoryContext);

	/* The table is small and rows are matched by pattern, so scan it all. */
	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		bool should_free;
		HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
		Datum values[Natts_storage_policy];
		bool nulls[Natts_storage_policy];
		bool is_candidate = false;
		bool is_exact = false;
		int32 priority = 0;

		heap_deform_tuple(tuple, ts_scanner_get_tupledesc(ti), values, nulls);

		if (DatumGetBool(values[AttrNumberGetAttrOffset(Anum_storage_policy_enabled)]) &&
			!found_exact)
		{
			const char *kind =
				TextDatumGetCString(values[AttrNumberGetAttrOffset(Anum_storage_policy_match_kind)]);
			const char *schema_pattern = TextDatumGetCString(
				values[AttrNumberGetAttrOffset(Anum_storage_policy_schema_name)]);
			const char *table_pattern = TextDatumGetCString(
				values[AttrNumberGetAttrOffset(Anum_storage_policy_table_name)]);

			if (strcmp(kind, STORAGE_POLICY_MATCH_EXACT) == 0)
			{
				is_exact = strcmp(schema_pattern, schema_name) == 0 &&
						   strcmp(table_pattern, table_name) == 0;
				is_candidate = is_exact;
			}
			else if (!nulls[AttrNumberGetAttrOffset(Anum_storage_policy_priority)] &&
					 regex_matches(schema_pattern, schema_name) &&
					 regex_matches(table_pattern, table_name))
			{
				priority =
					DatumGetInt32(values[AttrNumberGetAttrOffset(Anum_storage_policy_priority)]);
				/* Priorities are unique, so there are no ties. */
				is_candidate = !found || priority < best_priority;
			}
		}

		if (is_candidate)
		{
			policy->id = DatumGetInt32(values[AttrNumberGetAttrOffset(Anum_storage_policy_id)]);
			namestrcpy(&policy->name,
					   NameStr(*DatumGetName(
						   values[AttrNumberGetAttrOffset(Anum_storage_policy_name)])));
			policy->policy = DatumGetJsonbPCopy(
				values[AttrNumberGetAttrOffset(Anum_storage_policy_policy)]);
			found = true;
			found_exact = is_exact;
			best_priority = priority;
		}

		if (should_free)
			heap_freetuple(tuple);
	}
	ts_scan_iterator_close(&iterator);

	return found;
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

static void
columnstore_key(const char *key, JsonbValue *value, StoragePolicyOptions *out)
{
	if (strcmp(key, "segmentby") == 0 || strcmp(key, "orderby") == 0 || strcmp(key, "index") == 0)
	{
		char path[64];
		snprintf(path, sizeof(path), "columnstore.%s", key);
		out->with_options = lappend(out->with_options,
						   make_tsdb_option(key,
											(Node *) makeString(
												policy_value_to_string(path, value))));
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
	bool valid = true;
	MemoryContext oldcontext = CurrentMemoryContext;

	PG_TRY();
	{
		age = DirectFunctionCall3(interval_in,
								  CStringGetDatum(after),
								  ObjectIdGetDatum(InvalidOid),
								  Int32GetDatum(-1));
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(oldcontext);
		FlushErrorState();
		valid = false;
	}
	PG_END_TRY();

	if (!valid)
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

/* NEVER and an absent key are the same, no retention job is scheduled. */
static void
retention_key(const char *name, JsonbValue *value, StoragePolicyOptions *out)
{
	out->retention_after = parse_age_object(name, value, out);
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
	if (strcmp(key, "partition_column") == 0 || strcmp(key, "chunk_interval") == 0)
	{
		out->with_options = lappend(out->with_options,
						   make_tsdb_option(key,
											(Node *) makeString(
												policy_value_to_string(key, value))));
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

TS_FUNCTION_INFO_V1(ts_storage_policy_match_id);

/* The id of the policy a new table with this name would get, or NULL if there is none. */
Datum
ts_storage_policy_match_id(PG_FUNCTION_ARGS)
{
	StoragePolicy policy;

	if (!ts_storage_policy_find(NameStr(*PG_GETARG_NAME(0)), NameStr(*PG_GETARG_NAME(1)), &policy))
		PG_RETURN_NULL();

	PG_RETURN_INT32(policy.id);
}
