# Background worker jobs

TimescaleDB needs to run multiple background jobs. This module
implements a simple scheduler so that jobs inserted into a jobs table
can be run on a schedule. Each database in an instance runs it's own
scheduler because different databases may run different TimescaleDB
extension versions which may require different scheduler logic.

## Schedules

The scheduler allows you to set a `schedule_interval` for every job.
That defines the interval the scheduler will wait after a job finishes to start
it again, if the job is successful. If the job fails, the scheduler uses `retry_period`
in an exponential backoff to decide when to run the job again.

## Design

The scheduler itself is a background job that continuously runs and waits
for a time when jobs need to be scheduled. It then launches jobs as new
background workers that it controls through the background worker handle.

Aggregate statistics about a job are kept in the job stat catalog table.
These statistics include the start and finish times of the last run of the job
as well as whether or not the job succeeded. The `next_start` is used to
figure out when next to run a job after a scheduler is restarted.

The statistics table also tracks consecutive failures and crashes for the job
which are used for calculating the exponential backoff after a crash or failure
(which is used to set the `next_start` after the crash/failure). Note also that
there is a minimum time after the database scheduler starts up and a crashed job
is restarted. This is to allow the operator enough time to disable the job
if needed.

Note that the number of crashes is an overestimate of the actual number of crashes
for a job. This is so that we are conservative and never miss a crash and fail to
use the appropriate backoff logic. There is some complexity
in ensuring that all crashes are counted. A crash in Postgres causes /all/
processes to quit immediately therefore we cannot write anything to the database once
any process has crashed. Thus, we must be able to deduce that a crash occured
from a commit that happened before any crash. We accomplish
this by committing a changes to the stats table before a job starts and
undoing the change after it finishes. If a job crashed, it will be left
in an intermediate state from which we deduce that it could have been the
crashing process.

## Scheduler State Machine

The scheduler implements a state machine for each job.
Each job starts in the SCHEDULED state. As soon as a job starts
it enters the STARTING state. If the scheduler determines the
job should be terminated (e.g. it has reached a timeout), it moves
the job to a TERMINATING state. Once a background worker has for
a job has stopped, the job returns to the SCHEDULED state.
The states and associated transitions are as follows.

```
      +---------+         +--------+
+---> |SCHEDULED+-------> |DISABLED|
|     +----+----+         +--------+
|          |
|          |
|          v
|      +---+----+
+<-----+STARTING|
|      +---+----+
|          |
|          |
|          v
|      +---+-------+
+<-----+TERMINATING|
       +-----------+
```
## Limitations
This first implementation has two limitations:

- The list of jobs to be run is read from the database when the scheduler is first started.
We do not update this list if the jobs table changes.
- There is no prioritization for when to run jobs.

