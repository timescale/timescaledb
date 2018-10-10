# Loader

The loader has two main purposes:

1) Load the correct versioned library for each database.
Multiple databases in the same Postgres instance may contain
different versions of TimescaleDB installed. The loader is
responsible for loading the shared library corresponding
to the correct TimescaleDB version for the database as soon
as possible. For example, a database containing TimescaleDB
version 0.8.0 will have timescaledb-0.8.0.so loaded.

2) Starting background worker schedulers for each database.
   Background worker schedulers launch background worker tasks
   for TimescaleDB. The launcher is responsible for launching
   schedulers for any database that has TimescaleDB installed.
   This is done by a background task called the launcher.


# Launcher per-DB state machine

The following is the state machine that the launcher maintains
for each database. The CAPITAL labels are the possible states,
and the `lowercase` names for messages that trigger the accompanying
transitions. Transitions without labels are taken automatically
whenever available resources exist.
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

Note that `set vxid` sets a vxid variable on the scheduler. This variable
is passed down to the scheduler and the scheduler waits on that vxid when
it first starts.

Transitions that happen automatically (at least once per poll period).
* `ENABLED->ALLOCATED`: Reserved slot for worker
* `ALLOCATED->STARTED`: Scheduler started
* `STARTED->DISABLED`: Iff scheduler has stopped. Slot released.

Transition that happen upon getting a STOP MESSAGE:
* `ENABLED->DISABLED`: No action
* `ALLOCATED->DISABLED`: Slot released
* `STARTED->DISABLED`: Scheduler terminated & slot released
* `DISABLED->DISABLED`: No Action

Transition that happen upon getting a START MESSAGE
* Database not yet registed: Register, set to ENABLED and take ENABLED action below.
* `ENABLED->ENABLED`: Set vxid; then try the automatic transitions
* `ALLOCATED->ALLOCATED`: Set vxid; then try the automatic transitions
* `STARTED->STARTED`: No action
* `DISABLED->ENABLED`: Set vxid

Transition that happen upon getting a RESTART MESSAGE
* Database not yet registed: Failure - no action taken
* `ENABLED->ENABLED`: Set vxid
* `ALLOCATED->ALLOCATED`: Set vxid
* `STARTED->ALLOCATED`: Scheduler terminated, slot /not/ released, set vxid
* `DISABLED->DISABLED`: Failure - no action taken
