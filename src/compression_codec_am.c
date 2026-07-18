/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/amapi.h>
#include <access/amvalidate.h>
#include <access/htup_details.h>
#include <catalog/namespace.h>
#include <catalog/pg_amproc.h>
#include <catalog/pg_opclass.h>
#include <catalog/pg_type.h>
#include <commands/defrem.h>
#include <parser/parse_coerce.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/regproc.h>
#include <utils/syscache.h>

#include "compression_codec_am.h"
#include "export.h"

TS_FUNCTION_INFO_V1(ts_compression_codec_handler);

/*
 * See compression_codec_am.h: this access method exists only to carry
 * compression codec operator classes. Every index entry point rejects use as
 * an index, and amvalidate checks the codec contract.
 */

static void
compression_codec_not_an_index(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("\"%s\" is not a real index access method", COMPRESSION_CODEC_AM_NAME),
			 errdetail("Its operator classes only register compression codecs for data types.")));
}

static IndexBuildResult *
compression_codec_ambuild(Relation heap, Relation index, struct IndexInfo *index_info)
{
	compression_codec_not_an_index();
	pg_unreachable();
}

static void
compression_codec_ambuildempty(Relation index)
{
	compression_codec_not_an_index();
}

static bool
compression_codec_aminsert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid,
						   Relation heap, IndexUniqueCheck check_unique, bool index_unchanged,
						   struct IndexInfo *index_info)
{
	compression_codec_not_an_index();
	pg_unreachable();
}

static IndexBulkDeleteResult *
compression_codec_ambulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
							   IndexBulkDeleteCallback callback, void *callback_state)
{
	compression_codec_not_an_index();
	pg_unreachable();
}

static IndexBulkDeleteResult *
compression_codec_amvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	compression_codec_not_an_index();
	pg_unreachable();
}

static void
compression_codec_amcostestimate(struct PlannerInfo *root, struct IndexPath *path,
								 double loop_count, Cost *index_startup_cost,
								 Cost *index_total_cost, Selectivity *index_selectivity,
								 double *index_correlation, double *index_pages)
{
	compression_codec_not_an_index();
}

static bytea *
compression_codec_amoptions(Datum reloptions, bool validate)
{
	compression_codec_not_an_index();
	pg_unreachable();
}

static IndexScanDesc
compression_codec_ambeginscan(Relation index, int nkeys, int norderbys)
{
	compression_codec_not_an_index();
	pg_unreachable();
}

static void
compression_codec_amrescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys,
						   int norderbys)
{
	compression_codec_not_an_index();
}

static void
compression_codec_amendscan(IndexScanDesc scan)
{
	compression_codec_not_an_index();
}

/*
 * Check an operator class against the codec contract. Runs on explicit
 * SELECT amvalidate(opclass); CREATE OPERATOR CLASS does not call it, so the
 * EXTERNAL algorithm re-checks the support function signatures before use.
 */
static bool
compression_codec_amvalidate(Oid opclassoid)
{
	HeapTuple classtup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassoid));
	if (!HeapTupleIsValid(classtup))
	{
		elog(ERROR, "cache lookup failed for operator class %u", opclassoid);
	}
	Form_pg_opclass classform = (Form_pg_opclass) GETSTRUCT(classtup);
	Oid opfamilyoid = classform->opcfamily;
	Oid opcintype = classform->opcintype;
	char *opclassname = pstrdup(NameStr(classform->opcname));
	ReleaseSysCache(classtup);

	bool result = true;

	Oid array_type = get_array_type(opcintype);
	if (!OidIsValid(array_type))
	{
		ereport(INFO,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("compression codec operator class \"%s\" is for a type with no array type",
						opclassname)));
		return false;
	}

	/* if a codec has operators, something is wrong with it */
	CatCList *oprlist = SearchSysCacheList1(AMOPSTRATEGY, ObjectIdGetDatum(opfamilyoid));
	if (oprlist->n_members != 0)
	{
		ereport(INFO,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("compression codec operator class \"%s\" must not contain operators",
						opclassname)));
		result = false;
	}
	ReleaseCatCacheList(oprlist);

	/* check the support functions for sanity */
	bool has_decompress = false;
	CatCList *proclist = SearchSysCacheList1(AMPROCNUM, ObjectIdGetDatum(opfamilyoid));
	for (int i = 0; i < proclist->n_members; i++)
	{
		HeapTuple proctup = &proclist->members[i]->tuple;
		Form_pg_amproc procform = (Form_pg_amproc) GETSTRUCT(proctup);
		bool ok = true;

		if (procform->amproclefttype != opcintype || procform->amprocrighttype != opcintype)
		{
			ereport(INFO,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("compression codec operator class \"%s\" contains support function "
							"%s with cross-type registration",
							opclassname,
							format_procedure(procform->amproc))));
			result = false;
			continue;
		}

		switch (procform->amprocnum)
		{
			case COMPRESSION_CODEC_COMPRESS_PROC:
				ok = check_amproc_signature(procform->amproc, BYTEAOID, true, 1, 1, array_type);
				break;
			case COMPRESSION_CODEC_DECOMPRESS_PROC:
				ok = check_amproc_signature(procform->amproc, array_type, true, 1, 1, BYTEAOID);
				has_decompress = true;
				break;
			default:
				ereport(INFO,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("compression codec operator class \"%s\" contains function %s "
								"with invalid support number %d",
								opclassname,
								format_procedure(procform->amproc),
								procform->amprocnum)));
				result = false;
				continue;
		}
		if (!ok)
		{
			ereport(INFO,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("compression codec operator class \"%s\" contains function %s with "
							"wrong signature for support number %d",
							opclassname,
							format_procedure(procform->amproc),
							procform->amprocnum)));
			result = false;
		}
	}
	ReleaseCatCacheList(proclist);

	/*
	 * A decompress-only registration is legal. It is an intermediate state,
	 * keeping existing batches readable while new compression uses the
	 * current setting. Compress-only is not valid, since it would write
	 * batches that can't be read.
	 */
	if (!has_decompress)
	{
		ereport(INFO,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("compression codec operator class \"%s\" is missing support function %d",
						opclassname,
						COMPRESSION_CODEC_DECOMPRESS_PROC)));
		result = false;
	}

	return result;
}

Datum
ts_compression_codec_handler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = 0;
	amroutine->amsupport = 2;
	amroutine->amoptsprocnum = 0;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = compression_codec_ambuild;
	amroutine->ambuildempty = compression_codec_ambuildempty;
	amroutine->aminsert = compression_codec_aminsert;
	amroutine->ambulkdelete = compression_codec_ambulkdelete;
	amroutine->amvacuumcleanup = compression_codec_amvacuumcleanup;
	amroutine->amcostestimate = compression_codec_amcostestimate;
	amroutine->amoptions = compression_codec_amoptions;
	amroutine->amvalidate = compression_codec_amvalidate;
	amroutine->ambeginscan = compression_codec_ambeginscan;
	amroutine->amrescan = compression_codec_amrescan;
	amroutine->amendscan = compression_codec_amendscan;

	PG_RETURN_POINTER(amroutine);
}

/* The schema-qualified display name of an operator class. */
char *
ts_compression_codec_opclass_qualname(Oid opclass)
{
	HeapTuple opctup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
	if (!HeapTupleIsValid(opctup))
	{
		elog(ERROR, "cache lookup failed for operator class %u", opclass);
	}
	Form_pg_opclass opcform = (Form_pg_opclass) GETSTRUCT(opctup);
	char *qualname = quote_qualified_identifier(get_namespace_name(opcform->opcnamespace),
												NameStr(opcform->opcname));
	ReleaseSysCache(opctup);
	return qualname;
}

/*
 * Checks that a codec operator class fits a column type and
 * errors on violations of the codec contract.
 *
 * Like amvalidate, but used where something is about to rely on a
 * codec registration. Settings validation is done at ALTER TABLE,
 * compression, and decompression times.
 *
 * A registration without the compress function is only accepted when
 * require_compress is false: existing batches remain readable through the
 * decompress function, but no new batches may be written.
 */
void
ts_compression_codec_opclass_functions(Oid opclass, Oid element_type, bool require_compress,
									   Oid *compress_fn, Oid *decompress_fn)
{
	HeapTuple opctup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
	if (!HeapTupleIsValid(opctup))
	{
		elog(ERROR, "cache lookup failed for operator class %u", opclass);
	}
	Form_pg_opclass opcform = (Form_pg_opclass) GETSTRUCT(opctup);
	/*
	 * The opclass may be for a binary-coercible ancestor of element_type;
	 * the support functions are declared for its own input type.
	 */
	Oid opcintype = opcform->opcintype;
	Oid opfamily = opcform->opcfamily;
	ReleaseSysCache(opctup);
	char *opclass_name = ts_compression_codec_opclass_qualname(opclass);

	if (opcintype != element_type && !IsBinaryCoercible(opcintype, element_type) &&
		!IsBinaryCoercible(element_type, opcintype))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("compression codec operator class %s is not for type \"%s\"",
						opclass_name,
						format_type_be(element_type))));
	}

	Oid array_type = get_array_type(opcintype);
	Oid compress_oid =
		get_opfamily_proc(opfamily, opcintype, opcintype, COMPRESSION_CODEC_COMPRESS_PROC);
	Oid decompress_oid =
		get_opfamily_proc(opfamily, opcintype, opcintype, COMPRESSION_CODEC_DECOMPRESS_PROC);

	/*
	 * CREATE OPERATOR CLASS does not run amvalidate, so an illegal
	 * registration can possibly exist. Calling a wrong-typed
	 * function would crash, so re-check the signatures before calling.
	 */
	bool malformed = !OidIsValid(array_type) || !OidIsValid(decompress_oid);
	if (!malformed && OidIsValid(compress_oid))
	{
		malformed = !check_amproc_signature(compress_oid, BYTEAOID, true, 1, 1, array_type);
	}
	if (!malformed)
	{
		malformed = !check_amproc_signature(decompress_oid, array_type, true, 1, 1, BYTEAOID);
	}
	if (malformed)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("compression codec operator class %s is malformed", opclass_name),
				 errhint("Run amvalidate() on the operator class.")));
	}

	if (require_compress && !OidIsValid(compress_oid))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("compression codec operator class %s is decompress-only", opclass_name),
				 errdetail("Support function %d is required to compress new batches.",
						   COMPRESSION_CODEC_COMPRESS_PROC)));
	}

	if (compress_fn != NULL)
	{
		*compress_fn = compress_oid;
	}
	if (decompress_fn != NULL)
	{
		*decompress_fn = decompress_oid;
	}
}

/*
 * Resolve a codec operator class name and check it against the codec
 * contract for element_type.
 */
Oid
ts_compression_codec_opclass_resolve(const char *opclass_name, Oid element_type,
									 bool require_compress, Oid *compress_fn, Oid *decompress_fn)
{
	Oid am_oid = get_am_oid(COMPRESSION_CODEC_AM_NAME, /* missing_ok */ false);
	List *namelist = stringToQualifiedNameList(opclass_name, NULL);
	Oid opclass = get_opclass_oid(am_oid, namelist, /* missing_ok */ false);

	ts_compression_codec_opclass_functions(opclass,
										   element_type,
										   require_compress,
										   compress_fn,
										   decompress_fn);
	return opclass;
}
