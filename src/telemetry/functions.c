
/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <fmgr.h>

#include <access/genam.h>
#include <access/htup_details.h>
#include <access/table.h>
#include <catalog/indexing.h>
#include <catalog/pg_depend.h>
#include <catalog/pg_extension.h>
#include <catalog/pg_proc.h>
#include <commands/extension.h>
#include <nodes/nodeFuncs.h>
#include <port/atomics.h>
#include <storage/lwlock.h>
#include <utils/hsearch.h>
#include <utils/fmgroids.h>

#include <utils/regproc.h>

#include "functions.h"

#include "guc.h"
#include "loader/function_telemetry.h"

static bool skip_telemetry = false;
static LWLock *function_counts_lock = NULL;
static HTAB *function_counts;

/***************************
 * Telemetry draining code *
 ***************************/

typedef struct AllowedFnHashEntry
{
	Oid fn;
} AllowedFnHashEntry;

// Get a HTAB of AllowedFnHashEntrys containing all and only those functions
// that are withing visible_extensions. This function should be equivalent to
// the SQL
//     SELECT objid
//     FROM pg_catalog.pg_depend, pg_catalog.pg_extension extension
//      WHERE refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
//        AND refobjid = extension.oid
//        AND deptype = 'e'
//        AND extname IN ('timescaledb','promscale','timescaledb_toolkit')
//        AND classid = 'pg_catalog.pg_proc'::regclass;
static HTAB *
allowed_extension_functions(const char **visible_extensions, int num_visible_extensions)
{
	HASHCTL hash_info = {
		.keysize = sizeof(Oid),
		.entrysize = sizeof(AllowedFnHashEntry),
	};
	HTAB *allowed_fns =
		hash_create("fn telemetry allowed_functions", 1000, &hash_info, HASH_ELEM | HASH_BLOBS);

	Relation depRel = table_open(DependRelationId, AccessShareLock);

	Oid *visible_extension_ids = palloc(num_visible_extensions * sizeof(Oid));

	// get the Oid for each of the visible extensions
	for (int i = 0; i < num_visible_extensions; i++)
		visible_extension_ids[i] = get_extension_oid(visible_extensions[i], true);

	// go through the objects owned by each visible extension, and store the
	// ones that are functions in the set.
	for (int i = 0; i < num_visible_extensions; i++)
	{
		HeapTuple tup;
		ScanKeyData key[2];
		SysScanDesc scan;
		Oid extension_id = visible_extension_ids[i];

		if (extension_id == InvalidOid)
			continue;

		// Look in the (referenced object class, referenced object) index for
		// the allowed extensions.
		ScanKeyInit(&key[0],
					Anum_pg_depend_refclassid,
					BTEqualStrategyNumber,
					F_OIDEQ,
					ObjectIdGetDatum(ExtensionRelationId));
		ScanKeyInit(&key[1],
					Anum_pg_depend_refobjid,
					BTEqualStrategyNumber,
					F_OIDEQ,
					ObjectIdGetDatum(extension_id));

		scan = systable_beginscan(depRel, DependReferenceIndexId, true, NULL, 2, key);

		while (HeapTupleIsValid(tup = systable_getnext(scan)))
		{
			Form_pg_depend deprec = (Form_pg_depend) GETSTRUCT(tup);
			// Filter for those objects that have an extension dependencies
			// exist in pg_proc, those are the functions that live in the extension
			if (deprec->deptype == 'e' && deprec->classid == ProcedureRelationId)
			{
				AllowedFnHashEntry *entry =
					hash_search(allowed_fns, &deprec->objid, HASH_ENTER, NULL);
				entry->fn = deprec->objid;
			}
		}

		systable_endscan(scan);
	}

	table_close(depRel, AccessShareLock);

	return allowed_fns;
}

static fn_telemetry_entry_vec *
read_shared_map()
{
	HASH_SEQ_STATUS hash_seq;
	long i;
	long num_entries = hash_get_num_entries(function_counts);
	fn_telemetry_entry_vec *entries =
		fn_telemetry_entry_vec_create(CurrentMemoryContext, num_entries);

	LWLockAcquire(function_counts_lock, LW_SHARED);

	hash_seq_init(&hash_seq, function_counts);
	// limit to num_entries so we don't hold the lock for a realloc
	for (i = 0; i < num_entries; i++)
	{
		FnTelemetryEntry entry;
		FnTelemetryHashEntry *hash_entry = hash_seq_search(&hash_seq);
		if (!hash_entry)
			break;

		entry.fn = hash_entry->key;
		/*
		 * We never remove entries here, merely set their counts to 0. At
		 * steady-state we expect the functions used by most workloads to be
		 * effectively constant, so by keeping the hashmap entries allocated we
		 * reduce contention during the telemetry-gathering stage. If memory
		 * usage become an issue we can delete based off some heuristic, eg. if
		 * the count starts out as 0.
		 */
		entry.count = pg_atomic_read_u64(&hash_entry->count);
		if (entry.count != 0)
			fn_telemetry_entry_vec_append(entries, entry);
	}
	if (i == num_entries)
		hash_seq_term(&hash_seq);

	LWLockRelease(function_counts_lock);

	return entries;
}

/*
 * Read the function telemetry shared-memory hashmap for telemetry send.
 *
 * This function gathers (function_id, count) pairs from the shared hashmap,
 * and filters the set for the functions we're allowed to send back.
 *
 * In general, we should never send telemetry information about any functions
 * except for core functions and those is a specified list of extensions
 * (when originally written, the set of related_extensions along with
 * timescaledb itself), so this function is designed to make it difficult to do
 * so.
 *
 * @param visible_extensions list of extensions whose functions should be
 *                           returned
 * @param num_visible_extensions length of visible_extensions
 * @return vector of FnTelemetryEntry containing (function_id, count)s for the
 *         functions in visible_extensions.
 *
 */
fn_telemetry_entry_vec *
ts_function_telemetry_read(const char **visible_extensions, int num_visible_extensions)
{
	fn_telemetry_entry_vec *entries_to_send;
	fn_telemetry_entry_vec *all_entries;
	HTAB *allowed_ext_fns;

	if (!function_counts)
		return NULL;

	all_entries = read_shared_map();
	entries_to_send =
		fn_telemetry_entry_vec_create(CurrentMemoryContext, all_entries->num_elements);
	allowed_ext_fns = allowed_extension_functions(visible_extensions, num_visible_extensions);

	for (uint32 i = 0; i < all_entries->num_elements; i++)
	{
		FnTelemetryEntry *entry = fn_telemetry_entry_vec_at(all_entries, i);
		bool is_builtin = entry->fn >= 1 && entry->fn <= 9999;
		bool is_visible = is_builtin || hash_search(allowed_ext_fns, &entry->fn, HASH_FIND, NULL);
		if (is_visible)
			fn_telemetry_entry_vec_append(entries_to_send, *entry);
	}

	return entries_to_send;
}

/*
 * Reset the counts in the function telemetry shared-memory hashmap.
 *
 * This function resets the shared function counts after we send back telemetry
 * in preparation for the next recording cycle. Note that there is no way to
 * atomically read and reset the counts in the shared hashmap, so writes that
 * occur between sending the old counts and reseting for the next cycle will be
 * lost. Since this this telemetry is only ever an approximation of reality, we
 * believe this loss is acceptable considering that the alternatives are
 * resetting the counts whenever the telemetry is read (potentially even more
 * lossy), or holding the lock for the entire telemetry send (to long a
 * contention window).
 */
void
ts_function_telemetry_reset_counts()
{
	HASH_SEQ_STATUS hash_seq;
	if (!function_counts)
		return;

	LWLockAcquire(function_counts_lock, LW_SHARED);

	hash_seq_init(&hash_seq, function_counts);
	// limit to num_entries so we don't hold the lock for a realloc
	while (true)
	{
		FnTelemetryHashEntry *hash_entry = hash_seq_search(&hash_seq);
		if (!hash_entry)
			break;

		/*
		 * We never remove entries here, merely set their counts to 0. At
		 * steady-state we expect the functions used by most workloads to be
		 * effectively constant, so by keeping the hashmap entries allocated we
		 * reduce contention during the telemetry-gathering stage. If memory
		 * usage become an issue we can delete based off some heuristic, eg. if
		 * the count starts out as 0, though we will have to use a stronger
		 * lock.
		 */
		pg_atomic_write_u64(&hash_entry->count, 0);
	}

	LWLockRelease(function_counts_lock);
}

/****************************
 * Telemetry gathering code *
 ****************************/

static bool
function_telemetry_increment(Oid func_id, HTAB **local_counts)
{
	FnTelemetryEntry *entry;
	bool found;

	// if this is the first function we've seen initialize local_counts
	if (!*local_counts)
	{
		HASHCTL hash_info = {
			.keysize = sizeof(Oid),
			.entrysize = sizeof(FnTelemetryEntry),
			.hcxt = CurrentMemoryContext,
		};
		*local_counts = hash_create("fn telemetry local function hash",
									10,
									&hash_info,
									HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	entry = hash_search(*local_counts, &func_id, HASH_ENTER, &found);
	if (!found)
		entry->count = 0;

	entry->count += 1;

	return true;
}

static bool
function_gather_checker(Oid func_id, void *context)
{
	function_telemetry_increment(func_id, context);
	return false;
}

static bool
function_gather_walker(Node *node, void *context)
{
	bool end_early;

	if (node == NULL)
		return false;

	end_early = check_functions_in_node(node, function_gather_checker, context);
	if (end_early)
		return true;

	if (IsA(node, Query))
	{
		/* Recurse into subselects */
		return query_tree_walker((Query *) node, function_gather_walker, context, 0);
	}
	return expression_tree_walker(node, function_gather_walker, context);
}

static HTAB *
record_function_counts(Query *query)
{
	HTAB *query_function_counts = NULL;
	query_tree_walker(query, function_gather_walker, &query_function_counts, 0);
	return query_function_counts;
}

/*
 * Store a map of (function_oid, count) into shared memory so it can be seen by
 * the telemetry worker. This insertion works in two phases:
 *   1. Under a SHARED lock, we increment the counts of all those functions that
 *      are already present in the map, using atomic fetch-add to prevent races.
 *   2. Under an EXCLUSIVE lock, we insert entries for all those functions that
 *      were not already in the map.
 * At steady state we expect that vast majority the time all the functions a
 * query uses will already be in the shared map, so this strategy should
 * minimize contention between queries.
 *
 * @param query_function_counts A hashtable of FnTelemetryEntry storing
 *                              function usage counts
 */
static void
store_function_counts_in_shared_mem(HTAB *query_function_counts)
{
	HASH_SEQ_STATUS hash_seq;
	FnTelemetryEntry *local_entry = NULL;
	fn_telemetry_entry_vec missing_entries;
	fn_telemetry_entry_vec_init(&missing_entries, CurrentMemoryContext, 0);

	/*
	 * Increment the counts of any functions already in the table under a
	 * shared lock; the atomicity of increments will handle concurrency.
	 */
	LWLockAcquire(function_counts_lock, LW_SHARED);
	hash_seq_init(&hash_seq, query_function_counts);

	while ((local_entry = hash_seq_search(&hash_seq)))
	{
		FnTelemetryHashEntry *shared_entry =
			hash_search(function_counts, &local_entry->fn, HASH_FIND, NULL);

		if (shared_entry)
			pg_atomic_fetch_add_u64(&shared_entry->count, local_entry->count);
		else
			fn_telemetry_entry_vec_append(&missing_entries, *local_entry);
	}

	LWLockRelease(function_counts_lock);

	/*
	 * If any functions did not have an entries create them under an
	 * exclusive lock
	 */
	if (missing_entries.num_elements > 0)
	{
		LWLockAcquire(function_counts_lock, LW_EXCLUSIVE);
		for (uint32 i = 0; i < missing_entries.num_elements; i++)
		{
			bool found = false;
			FnTelemetryEntry *missing_entry = fn_telemetry_entry_vec_at(&missing_entries, i);
			FnTelemetryHashEntry *shared_entry =
				hash_search(function_counts, &missing_entry->fn, HASH_ENTER_NULL, &found);

			if (!shared_entry)
				break;

			if (found)
				pg_atomic_fetch_add_u64(&shared_entry->count, missing_entry->count);
			else
				pg_atomic_init_u64(&shared_entry->count, missing_entry->count);
		}
		LWLockRelease(function_counts_lock);
	}
}

/*
 * Gather function usage telemetry for a query.
 *
 * This function walks a query looking for function Oids, counts their
 * occurrence, and stores the (function_id, count) set into the shared-memory
 * function telemetry hashtable for later processing by the telemetry background
 * worker.
 */
void
ts_telemetry_function_info_gather(Query *query)
{
	HTAB *query_function_counts;

	if (skip_telemetry || !ts_function_telemetry_on())
		return;

	// At the first time through initialize the shared state
	if (function_counts == NULL)
	{
		FnTelemetryRendezvous **rendezvous =
			(FnTelemetryRendezvous **) find_rendezvous_variable(RENDEZVOUS_FUNCTION_TELEMENTRY);

		if (*rendezvous == NULL)
		{
			skip_telemetry = true;
			return;
		}

		function_counts = (*rendezvous)->function_counts;
		function_counts_lock = (*rendezvous)->lock;
	}

	query_function_counts = record_function_counts(query);

	if (query_function_counts)
		store_function_counts_in_shared_mem(query_function_counts);
}
