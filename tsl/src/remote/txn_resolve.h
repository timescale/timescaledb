/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_REMOTE_TXN_RESOLVE_H
#define TIMESCALEDB_TSL_REMOTE_TXN_RESOLVE_H
#include <postgres.h>

#include "txn_id.h"
#include "fmgr.h"
/*
   This implementation uses the presumed-abort variant of 2PC. The frontend is the coordinator
   and data nodes are the participants. Participant actions are implemented by native postgres
   `PREPARE TRANSACTION`/`COMMIT PREPARED`/`ROLLBACK PREPARED`. This code relates to the coordinator
   actions. In presumed-abort, the coordinator needs to write a commit message to stable storage
   between the 1st and 2nd phase of 2PC. This is accomplished in this implementation by doing
   the 1st phase and writing records to `remote_txn` in the pre-commit hook. Then doing a commit,
   thus will write the `remote_txn` to stable storage. Finally we do the 2nd phase of 2PC in a
   post-commit hook (possible, later, in a background task).

   For the two-pc implementation we define the following correctness criteria:
   For all transactions, every associated remote transaction either commits or aborts.

   This is implements by using PREPARE TRANSACTION to make sure all remote nodes can commit.
   If the frontend receives OKs for all PREPARE TRANSACTIONS, it writes an entry into the
   `remote_txn` table and then COMMITS locally. That commit serves as the sync point,
   if it happened then all nodes in the transaction should commit, otherwise nodes are free to
   abort.

   For each remote transactions there are three possible states of the leading transaction on the
   frontend: 1) The transaction is ongoing - in this case the state of the remote transaction is
   unknown (REMOTE_TXN_RESOLVE_UNKNOWN) 2) The transaction is committed - The remote transaction
   MUST BE be committed (REMOTE_TXN_RESOLVE_COMMT)
			- Invariant:  All remote transaction have been `PREPARE TRANSACTION` successfully
				-> Otherwise the frontend transaction would have aborted
				-> Note: This guarantees that the remote transaction can be committed (commit cannot
   fail)
			- Invariant: An entry for each remote commit will exist in `remote_txn`
		3) The transaction is aborted - the remote transactions MUST BE aborted
   (REMOTE_TXN_RESOLVE_COMMT)
			- Invariant: No entry will exist in `remote_txn`.

	Resolution procedure: A remote transaction commits IFF the frontend transaction is finished and
   there is an entry in `remote_txn`. If the transaction is ongoing, wait. Otherwise, abort.

	Note from the above we can do a case analysis:
		Case 1 - the transaction is ongoing) The transaction will eventually end up in either case 2
   or 3. No remote transactions have been committed. Case 2 - frontend commit) All remote
   transactions will eventually commit since they have been PREPARED and there is an entry in
   `remote_txn` Case 3 - frontend abort) All remote transactions will eventually abort since there
   is no entry in `remote_txn`
*/

typedef enum RemoteTxnResolution
{
	REMOTE_TXN_RESOLUTION_UNKNOWN = 0,
	REMOTE_TXN_RESOLUTION_ABORT,
	REMOTE_TXN_RESOLUTION_COMMT
} RemoteTxnResolution;

extern RemoteTxnResolution remote_txn_resolution(Oid foreign_server,
												 const RemoteTxnId *transaction_id);
extern Datum remote_txn_heal_data_node(PG_FUNCTION_ARGS);

#endif /* TIMESCALEDB_TSL_REMOTE_TXN_RESOLVE_H */
