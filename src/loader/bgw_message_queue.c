/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

#include <miscadmin.h>
#include <storage/lwlock.h>
#include <storage/shmem.h>
#include <storage/proc.h>
#include <storage/procarray.h>
#include <storage/shm_mq.h>
#include <access/xact.h>
#include <storage/spin.h>
#include <pgstat.h>

#include "../compat.h"

#include "bgw_message_queue.h"

#define BGW_MQ_MAX_MESSAGES 16
#define BGW_MQ_NAME "ts_bgw_message_queue"
#define BGW_MQ_TRANCHE_NAME "ts_bgw_mq_tranche"

#define BGW_MQ_NUM_WAITS 100

/* WaitLatch expects a long */
#define BGW_MQ_WAIT_INTERVAL 1000L

#define BGW_ACK_RETRIES 20

/* WaitLatch expects a long */
#define BGW_ACK_WAIT_INTERVAL 100L
#define BGW_ACK_QUEUE_SIZE (MAXALIGN(shm_mq_minimum_size + sizeof(int)))

/* We're using a relatively simple implementation of a circular queue similar to:
 * http://opendatastructures.org/ods-python/2_3_ArrayQueue_Array_Based_.html */
typedef struct MessageQueue
{
	pid_t reader_pid; /* Should only be set once at cluster launcher
					   * startup */
	slock_t mutex;	/* Controls access to the reader pid */
	LWLock *lock;	 /* Pointer to the lock to control
					   * adding/removing elements from queue */
	uint8 read_upto;
	uint8 num_elements;
	BgwMessage buffer[BGW_MQ_MAX_MESSAGES];
} MessageQueue;

typedef enum QueueResponseType
{
	MESSAGE_SENT = 0,
	QUEUE_FULL,
	READER_DETACHED
} QueueResponseType;

static MessageQueue *mq = NULL;

/*
 * This is run during the shmem_startup_hook.
 * On Linux, it's only run once, but in EXEC_BACKEND mode / on Windows/ other systems
 * that do forking differently, it is run in every backend at startup
 */
static void
queue_init()
{
	bool found;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	mq = ShmemInitStruct(BGW_MQ_NAME, sizeof(MessageQueue), &found);
	if (!found)
	{
		memset(mq, 0, sizeof(MessageQueue));
		mq->reader_pid = InvalidPid;
		SpinLockInit(&mq->mutex);
		mq->lock = &(GetNamedLWLockTranche(BGW_MQ_TRANCHE_NAME))->lock;
	}
	LWLockRelease(AddinShmemInitLock);
}

/* This gets called when shared memory is initialized in a backend
 * (shmem_startup_hook) */
extern void
ts_bgw_message_queue_shmem_startup(void)
{
	queue_init();
}

/* This is called in the loader during server startup to allocate a shared
 * memory segment*/
extern void
ts_bgw_message_queue_alloc(void)
{
	RequestAddinShmemSpace(sizeof(MessageQueue));
	RequestNamedLWLockTranche(BGW_MQ_TRANCHE_NAME, 1);
}

/*
 * Notes on managing the queue/locking: We decided that for this application,
 * simplicity of locking scheme was more important than being very good about
 * concurrency as the frequency of these messages will be low and the number
 * of messages on this queue should be low, given that they mostly happen when
 * we update the extension. Therefore we decided to simply take an exclusive
 * lock whenever we were modifying anything in the shared memory segment to
 * avoid collisions.
 */
static pid_t
queue_get_reader(MessageQueue *queue)
{
	pid_t reader;
	volatile MessageQueue *vq = queue;

	SpinLockAcquire(&vq->mutex);
	reader = vq->reader_pid;
	SpinLockRelease(&vq->mutex);
	return reader;
}

static void
queue_set_reader(MessageQueue *queue)
{
	volatile MessageQueue *vq = queue;
	pid_t reader_pid;

	SpinLockAcquire(&vq->mutex);
	if (vq->reader_pid == InvalidPid)
	{
		vq->reader_pid = MyProcPid;
	}
	reader_pid = vq->reader_pid;
	SpinLockRelease(&vq->mutex);
	if (reader_pid != MyProcPid)
		ereport(ERROR,
				(errmsg("only one reader allowed for TimescaleDB background worker message queue"),
				 errhint("Current process is %d", reader_pid)));
}

static void
queue_reset_reader(MessageQueue *queue)
{
	volatile MessageQueue *vq = queue;
	bool reset = false;

	SpinLockAcquire(&vq->mutex);
	if (vq->reader_pid == MyProcPid)
	{
		reset = true;
		vq->reader_pid = InvalidPid;
	}
	SpinLockRelease(&vq->mutex);

	if (!reset)
		ereport(ERROR,
				(ERRCODE_INTERNAL_ERROR,
				 errmsg("multiple TimescaleDB background worker launchers have been started when "
						"only one is allowed"),
				 errhint("This is a bug, please report it on our github page.")));
}

/* Add a message to the queue - we can do this if the queue is not full */
static QueueResponseType
queue_add(MessageQueue *queue, BgwMessage *message)
{
	QueueResponseType message_result = QUEUE_FULL;

	LWLockAcquire(queue->lock, LW_EXCLUSIVE);
	if (queue->num_elements < BGW_MQ_MAX_MESSAGES)
	{
		memcpy(&queue->buffer[(queue->read_upto + queue->num_elements) % BGW_MQ_MAX_MESSAGES],
			   message,
			   sizeof(BgwMessage));
		queue->num_elements++;
		message_result = MESSAGE_SENT;
	}
	LWLockRelease(queue->lock);

	if (queue_get_reader(queue) != InvalidPid)
		SetLatch(&BackendPidGetProc(queue_get_reader(queue))->procLatch);
	else
		message_result = READER_DETACHED;
	return message_result;
}

static BgwMessage *
queue_remove(MessageQueue *queue)
{
	BgwMessage *message = NULL;

	LWLockAcquire(queue->lock, LW_EXCLUSIVE);
	if (queue_get_reader(queue) != MyProcPid)
		ereport(ERROR,
				(errmsg(
					"cannot read if not reader for TimescaleDB background worker message queue")));

	if (queue->num_elements > 0)
	{
		message = palloc(sizeof(BgwMessage));
		memcpy(message, &queue->buffer[queue->read_upto], sizeof(BgwMessage));
		queue->read_upto = (queue->read_upto + 1) % BGW_MQ_MAX_MESSAGES;
		queue->num_elements--;
	}
	LWLockRelease(queue->lock);
	return message;
}

/* Construct a message */
static BgwMessage *
bgw_message_create(BgwMessageType message_type, Oid db_oid)
{
	BgwMessage *message = palloc(sizeof(BgwMessage));
	dsm_segment *seg;

	seg = dsm_create(BGW_ACK_QUEUE_SIZE, 0);

	*message = (BgwMessage){ .message_type = message_type,
							 .sender_pid = MyProcPid,
							 .db_oid = db_oid,
							 .ack_dsm_handle = dsm_segment_handle(seg) };

	return message;
}

/*
 * Our own version of shm_mq_wait_for_attach that waits with a timeout so that
 * should our counterparty die before attaching, we don't end up hanging.
 */
static shm_mq_result
ts_shm_mq_wait_for_attach(MessageQueue *queue, shm_mq_handle *ack_queue_handle)
{
	int n;
	PGPROC *reader_proc;

	for (n = 1; n <= BGW_MQ_NUM_WAITS; n++)
	{
		/* The reader is the sender on the ack queue */
		reader_proc = shm_mq_get_sender(shm_mq_get_queue(ack_queue_handle));
		if (reader_proc != NULL)
			return SHM_MQ_SUCCESS;
		else if (queue_get_reader(queue) == InvalidPid)
			return SHM_MQ_DETACHED; /* Reader died after we enqueued our
									 * message */
#if PG96
		WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT, BGW_MQ_WAIT_INTERVAL);
#elif PG12_LT
		WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT, BGW_MQ_WAIT_INTERVAL, WAIT_EVENT_MQ_INTERNAL);
#else
		WaitLatch(MyLatch,
				  WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
				  BGW_MQ_WAIT_INTERVAL,
				  WAIT_EVENT_MQ_INTERNAL);
#endif

		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}
	return SHM_MQ_DETACHED;
}

static bool
enqueue_message_wait_for_ack(MessageQueue *queue, BgwMessage *message,
							 shm_mq_handle *ack_queue_handle)
{
	Size bytes_received = 0;
	QueueResponseType send_result;
	bool *data = NULL;
	shm_mq_result mq_res;
	bool ack_received = false;
	int n;

	/*
	 * We don't want the process restarting workers to really distinguish the
	 * reasons workers might or might not be restarted, and we don't really
	 * want them to error when workers can't be started, as there are multiple
	 * valid reasons for that. So we'll simply return false for the ack even
	 * if we can't attach to the queue etc.
	 */
	send_result = queue_add(queue, message);
	if (send_result != MESSAGE_SENT)
		return false;

	mq_res = ts_shm_mq_wait_for_attach(queue, ack_queue_handle);
	if (mq_res != SHM_MQ_SUCCESS)
		return false;

	/* Get a response, non-blocking, with retries */
	for (n = 1; n <= BGW_ACK_RETRIES; n++)
	{
		mq_res = shm_mq_receive(ack_queue_handle, &bytes_received, (void **) &data, true);
		if (mq_res != SHM_MQ_WOULD_BLOCK)
			break;
		ereport(DEBUG1, (errmsg("TimescaleDB ack message receive failure, retrying")));
#if PG96
		WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT, BGW_ACK_WAIT_INTERVAL);
#elif PG12_LT
		WaitLatch(MyLatch,
				  WL_LATCH_SET | WL_TIMEOUT,
				  BGW_ACK_WAIT_INTERVAL,
				  WAIT_EVENT_MQ_INTERNAL);
#else
		WaitLatch(MyLatch,
				  WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
				  BGW_ACK_WAIT_INTERVAL,
				  WAIT_EVENT_MQ_INTERNAL);
#endif
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}

	if (mq_res != SHM_MQ_SUCCESS)
		return false;

	ack_received = (bytes_received != 0) && *data;

	return ack_received;
}

/*
 * Write element to queue, wait/error if queue is full
 * consumes message and deallocates
 */
extern bool
ts_bgw_message_send_and_wait(BgwMessageType message_type, Oid db_oid)
{
	shm_mq *ack_queue;
	dsm_segment *seg;
	shm_mq_handle *ack_queue_handle;
	BgwMessage *message;
	bool ack_received = false;

	message = bgw_message_create(message_type, db_oid);

	seg = dsm_find_mapping(message->ack_dsm_handle);
	if (seg == NULL)
		ereport(ERROR,
				(errmsg("TimescaleDB background worker dynamic shared memory segment not mapped")));
	ack_queue = shm_mq_create(dsm_segment_address(seg), BGW_ACK_QUEUE_SIZE);
	shm_mq_set_receiver(ack_queue, MyProc);
	ack_queue_handle = shm_mq_attach(ack_queue, seg, NULL);
	if (ack_queue_handle != NULL)
		ack_received = enqueue_message_wait_for_ack(mq, message, ack_queue_handle);
	dsm_detach(seg); /* Queue detach happens in dsm detach callback */
	pfree(message);
	return ack_received;
}

/*
 * Called only by the launcher
 */
extern BgwMessage *
ts_bgw_message_receive(void)
{
	return queue_remove(mq);
}

extern void
ts_bgw_message_queue_set_reader(void)
{
	queue_set_reader(mq);
}

typedef enum MessageAckSent
{
	ACK_SENT = 0,
	DSM_SEGMENT_UNAVAILABLE,
	QUEUE_NOT_ATTACHED,
	SEND_FAILURE
} MessageAckSent;

static const char *message_ack_sent_err[] = { [ACK_SENT] = "Sent ack successfully",
											  [DSM_SEGMENT_UNAVAILABLE] = "DSM Segment unavailable",
											  [QUEUE_NOT_ATTACHED] = "Ack queue unable to attach",
											  [SEND_FAILURE] = "Unable to send ack on queue" };

static MessageAckSent
send_ack(dsm_segment *seg, bool success)
{
	shm_mq *ack_queue;
	shm_mq_handle *ack_queue_handle;
	shm_mq_result ack_res;
	int n;

	ack_queue = dsm_segment_address(seg);
	if (ack_queue == NULL)
		return DSM_SEGMENT_UNAVAILABLE;

	shm_mq_set_sender(ack_queue, MyProc);
	ack_queue_handle = shm_mq_attach(ack_queue, seg, NULL);
	if (ack_queue_handle == NULL)
		return QUEUE_NOT_ATTACHED;

	/* Send the message off, non blocking, with retries */
	for (n = 1; n <= BGW_ACK_RETRIES; n++)
	{
		ack_res = shm_mq_send(ack_queue_handle, sizeof(bool), &success, true);
		if (ack_res != SHM_MQ_WOULD_BLOCK)
			break;
		ereport(DEBUG1, (errmsg("TimescaleDB ack message send failure, retrying")));
#if PG96
		WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT, BGW_ACK_WAIT_INTERVAL);
#elif PG12_LT
		WaitLatch(MyLatch,
				  WL_LATCH_SET | WL_TIMEOUT,
				  BGW_ACK_WAIT_INTERVAL,
				  WAIT_EVENT_MQ_INTERNAL);
#else
		WaitLatch(MyLatch,
				  WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
				  BGW_ACK_WAIT_INTERVAL,
				  WAIT_EVENT_MQ_INTERNAL);
#endif
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}

	/* we are responsible for pfree'ing the handle, the dsm infrastructure only
	 * deals with the queue itself
	 */
	pfree(ack_queue_handle);
	if (ack_res != SHM_MQ_SUCCESS)
		return SEND_FAILURE;

	return ACK_SENT;
}

/*
 * Called by launcher once it has taken action based on the contents of the message
 * consumes message and deallocates
 */
extern void
ts_bgw_message_send_ack(BgwMessage *message, bool success)
{
	dsm_segment *seg;

	/*
	 * PG 9.6 does not check to see if we had a CurrentResourceOwner inside of
	 * dsm.c->dsm_create_descriptor.  Basically, it assumed we were in a
	 * transaction if we ever attached to the dsm, whereas PG 10 addressed
	 * that and did proper NULL checking. So, if we are in 9.6, we start a
	 * transaction and then commit it at the end of ack sending, to be sure
	 * everything is cleaned up properly etc.
	 */
#if PG96
	StartTransactionCommand();
#endif
	seg = dsm_attach(message->ack_dsm_handle);
	if (seg != NULL)
	{
		MessageAckSent ack_res;

		ack_res = send_ack(seg, success);
		if (ack_res != ACK_SENT)
			ereport(DEBUG1,
					(errmsg("TimescaleDB background worker launcher unable to send ack to backend "
							"pid %d",
							message->sender_pid),
					 errhint("Reason: %s", message_ack_sent_err[ack_res])));
		dsm_detach(seg);
	}
#if PG96
	CommitTransactionCommand();
#endif
	pfree(message);
}

/*
 * This gets called before shmem exit in the launcher (even if we're exiting
 * in error, but not if we're exiting due to possible shmem corruption)
 */
static void
queue_shmem_cleanup(MessageQueue *queue)
{
	queue_reset_reader(queue);
}

extern void
ts_bgw_message_queue_shmem_cleanup(void)
{
	queue_shmem_cleanup(mq);
}
