#include <postgres.h>
#include <access/xact.h>
#include <access/transam.h>
#include <commands/extension.h>
#include <commands/event_trigger.h>
#include <catalog/namespace.h>
#include <utils/lsyscache.h>
#include <utils/inval.h>

#include "catalog.h"
#include "extension.h"

#define EXTENSION_PROXY_TABLE "cache_inval_extension"

static Oid	extension_proxy_oid = InvalidOid;

/*
 * ExtensionState tracks the state of extension metadata in the backend.
 *
 * Since we want to cache extension metadata to speed up common checks (e.g.,
 * check for presence of the extension itself), we also need to track the
 * extension state to know when the metadata is valid.
 *
 * The metadata itself is initialized and cached lazily when calling, e.g.,
 * extension_is_loaded().
 */
enum ExtensionState
{
	/*
	 * INITIAL is the state for new backends or after an extension was
	 * dropped. The extension could be loaded or not. Only a check can tell.
	 */
	EXTENSION_STATE_INITIAL,

	/*
	 * CREATING only occurs on the backend that issues the CREATE EXTENSION
	 * statement.
	 */
	EXTENSION_STATE_CREATING,

	/*
	 * CREATED means we know the extension is loaded, metadata is up-to-date,
	 * and we therefore do not need a full check until we enter another state.
	 */
	EXTENSION_STATE_CREATED,

	/*
	 * DROPPING happens immediately after a DROP EXTENSION is issued. This
	 * only happends on the backend that issues the actual extension drop. All
	 * backends, including the issuing one, will be notified and move to
	 * INITIAL. Note that DROPPING does NOT mean the extension is not present,
	 * but rather that it will be gone by the end of the transaction that set
	 * this state.
	 */
	EXTENSION_STATE_DROPPING,
};

static enum ExtensionState extstate = EXTENSION_STATE_INITIAL;
static TransactionId drop_transaction_id = InvalidTransactionId;

bool
extension_is_being_dropped(Oid relid)
{
	return relid == extension_proxy_oid;
}

static void
extension_init(void)
{
	Oid			nsid = get_namespace_oid(CACHE_SCHEMA_NAME, false);

	drop_transaction_id = InvalidTransactionId;
	extension_proxy_oid = get_relname_relid(EXTENSION_PROXY_TABLE, nsid);
	catalog_reset();
}

void
extension_reset(void)
{
	extension_proxy_oid = InvalidOid;
	catalog_reset();
	extstate = EXTENSION_STATE_INITIAL;
}

PG_FUNCTION_INFO_V1(extension_event_trigger);

Datum
extension_event_trigger(PG_FUNCTION_ARGS)
{
	EventTriggerData *trigdata = (EventTriggerData *) fcinfo->context;

	if (!CALLED_AS_EVENT_TRIGGER(fcinfo))
		elog(ERROR, "not fired by event trigger manager");

	if (strcmp(trigdata->event, "ddl_command_end") == 0 &&
		strcmp(trigdata->tag, "CREATE EXTENSION") == 0 &&
		IsA(trigdata->parsetree, CreateExtensionStmt) &&
		strcmp(((CreateExtensionStmt *) trigdata->parsetree)->extname, EXTENSION_NAME) == 0)
	{
		extstate = EXTENSION_STATE_CREATING;
	}
	else if (strcmp(trigdata->event, "ddl_command_start") == 0 &&
			 strcmp(trigdata->tag, "DROP EXTENSION") == 0 &&
			 IsA(trigdata->parsetree, DropStmt))
	{
		DropStmt   *stmt = (DropStmt *) trigdata->parsetree;
		const char *extname = strVal(linitial(linitial(stmt->objects)));

		if (strcmp(extname, EXTENSION_NAME) == 0)
		{
			extstate = EXTENSION_STATE_DROPPING;

			/*
			 * Save the transaction ID, so that we can avoid going from
			 * INITIAL to CREATED in the transaction that issued DROP
			 * EXTENSION.
			 */
			drop_transaction_id = GetCurrentTransactionId();

			/*
			 * Notify other backends that the extension was dropped. We do
			 * this via the relcache invalidation mechanism in Postgres by
			 * issuing an invalidation on a proxy table. Other backends will
			 * see an invalidation event on the proxy table and then knows
			 * they need to move back to INITIAL state.
			 */
			CacheInvalidateRelcacheByRelid(extension_proxy_oid);
		}
	}

	PG_RETURN_NULL();
}

bool
extension_is_loaded(void)
{
	Oid			id;

	/* The extension is always valid in CREATED state */
	if (EXTENSION_STATE_CREATED == extstate)
		return true;

	if (!IsTransactionState())
		return false;

	/*
	 * Do a full check for extension presence. If present, initialize the
	 * cached extension state unless the extension is being dropped.
	 */
	id = get_extension_oid(EXTENSION_NAME, true);

	if (OidIsValid(id))
	{
		if (creating_extension && id == CurrentExtensionObject)
		{
			/* Extension is still being created */
			extstate = EXTENSION_STATE_CREATING;
			return false;
		}

		/*
		 * This check protects against resetting the extension state while
		 * still in the transaction that is dropping the extension, which
		 * could otherwise leave us with a state indicating the extension is
		 * still present after it is dropped.
		 */
		if (extstate < EXTENSION_STATE_CREATED &&
			!TransactionIdIsCurrentTransactionId(drop_transaction_id))
		{
			extstate = EXTENSION_STATE_CREATED;
			extension_init();
		}
		return true;
	}

	return false;
}
