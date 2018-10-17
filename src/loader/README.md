# Loader

The loader has two main purposes:

1) Load the correct versioned library for each database. Multiple databases in
   the same Postgres instance may have different versions of TimescaleDB
   installed. The loader is responsible for loading the shared library
   corresponding to the correct TimescaleDB version for the database as soon as
   possible. For example, a database containing TimescaleDB version 0.8.0 will
   have timescaledb-0.8.0.so loaded.

2) Starting background worker schedulers for each database. Background worker
   schedulers launch background worker tasks for TimescaleDB. The launcher is
   responsible for launching schedulers for any database that has TimescaleDB
   installed. This is done by a background task called the launcher.

# Messages the launcher may receive
The launcher implements a simple message queue to be notified when it should
take certain actions, like starting or restarting a scheduler for a given
database. 

##Message types sent to the launcher:

`start`: Used to start the scheduler by the user. It is meant to be an idempotent
start, as in, if it is run multiple times, and startup is successful, it is the
same as if it were run once. It is used mainly to reactivate a scheduler that
the user had stopped.

`stop`: Used to stop the scheduler immediately. It does not wait on a vxid and
it is idempotent. 

`restart`: Used to stop and restart the scheduler if it is running. The
scheduler is immediately restarted, but waits on the vxid of the txn that sent
the message. It is not idempotent, and will restart newly started schedulers,
even while they are waiting.  

`start_or_restart`: Used to either stop and restart the scheduler if it is
running or start it if it is not. It waits on a vxid and, much like restart, is
not idempotent. 

## When/which messages are sent:

Server startup: no message sent. The launcher will attempt to start a scheduler
for each database. It cannot figure out whether a scheduler should exist for a
given database because it can only connect to shared catalogs. The scheduler is
responsible for shutting down if it should not exist (because either TimescaleDB
is not installed in the database or the version of TimescaleDB installed does
not have a scheduler function to call). 

`CREATE DATABASE`: essentially the same as server startup. The launcher checks
for new databases each time it wakes up and will start schedulers for any that
it has not seen before.

`CREATE EXTENSION`: the create script sends a `start_or_restart` message. It
does not use the `start` message because there is a race condition where a
scheduler starts (say at server start), and is not waiting on the vxid of the
process that is running `CREATE EXTENSION`, but has not yet shut down (therefore
the start action does nothing), so we need to restart and wait on the correct
vxid in that case. (A normal restart action will not work as much of the time
there is no existing scheduler and then the restart action will have no effect).

`ALTER EXTENSION UPDATE`: the pre-update script sends a `restart` message. For
updates that move either between versions that do not have a scheduler or that
do have a scheduler, this respects any schedulers that have been shut down using
the `stop` message by the user. However, the update script from `0.11-0.12`, the
first version to use the scheduler, sends a `start_or_restart` message, as it is
similar to the `CREATE EXTENSION` case.

`DROP EXTENSION`: sends a `restart` message, which is necessary because a
rollback of the drop extension command can still happen. The scheduler therefore
waits on the vxid of the txn running `DROP EXTENSION` and then will take the
correct action depending on whether the extension exists when the txn finishes.

`DROP DATABASE`: sends a `stop` message, causing immediate shutdown of the
scheduler. This is necessary as the database cannot be dropped if there are any
open connections to it (the scheduler maintains a connection to the db).


# Launcher per-DB state machine

The following is the state machine that the launcher maintains for each
database. The CAPITAL labels are the possible states, and the `lowercase` names
for messages that trigger the accompanying transitions. Transitions without
labels are taken automatically whenever available resources exist.
```

                   stop
      ENABLED+--------------+
         +   ^--------------|
         |         start   ||
         |                 ||
         |                 ||
         v                 +v
      ALLOCATED+------> DISABLED
        ^+       stop       ^
        ||                  |
restart ||                  |
        ||                  |
        +v                  |
      STARTED+--------------+
                stop / scheduler quit

```

## The following is a detailed description of the transitions

Note that `set vxid` sets a vxid variable on the scheduler. This variable is
passed down to the scheduler and the scheduler waits on that vxid when it first
starts. 

Transitions that happen automatically (at least once per poll period).
* `ENABLED->ALLOCATED`: Reserved slot for worker
* `ALLOCATED->STARTED`: Scheduler started
* `STARTED->DISABLED`: Iff scheduler has stopped. Release slot.

Transitions that happen upon getting a STOP MESSAGE:
* `ENABLED->DISABLED`: No action
* `ALLOCATED->DISABLED`: Release slot.
* `STARTED->DISABLED`: Terminate scheduler & release slot
* `DISABLED->DISABLED`: No Action

Transitions that happen upon getting a START MESSAGE
* Database not yet registed: Register, set to ENABLED and take ENABLED action below.
* `ENABLED->ENABLED`: Set vxid, try automatic transitions
* `ALLOCATED->ALLOCATED`: Set vxid, try automatic transitions
* `STARTED->STARTED`: No action
* `DISABLED->ENABLED`: Set vxid, try automatic transitions

Transitions that happen upon getting a RESTART MESSAGE
* Database not yet registed: Failure - no action taken
* `ENABLED->ENABLED`: Set vxid, try automatic transitions
* `ALLOCATED->ALLOCATED`: Set vxid, try automatic transitions
* `STARTED->ALLOCATED`: Terminate scheduler,do /not/ release slot, set vxid, then try automatic transitions
* `DISABLED->DISABLED`: Failure - no action taken

Transitions that happen upon getting a START_OR_RESTART MESSAGE
* Database not yet registed: Register it set to ENABLED, take ENABLED actions
* `ENABLED->ENABLED`: Set vxid, try automatic transitions
* `ALLOCATED->ALLOCATED`: Set vxid, try automatic transitions
* `STARTED->ALLOCATED`: Terminate scheduler, do /not/ release slot, set vxid, then try automatic transitions
* `DISABLED->ENABLED`: Set vxid, try automatic transitions 
