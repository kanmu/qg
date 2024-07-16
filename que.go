package qg

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"sync"
	"time"

	null "gopkg.in/guregu/null.v3"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Job is a single unit of work for Que to perform.
type Job struct {
	// ID is the unique database ID of the Job. It is ignored on job creation.
	ID int64

	// Queue is the name of the queue. It defaults to the empty queue "".
	Queue string

	// Priority is the priority of the Job. The default priority is 100, and a
	// lower number means a higher priority. A priority of 5 would be very
	// important.
	Priority int16

	// RunAt is the time that this job should be executed. It defaults to now(),
	// meaning the job will execute immediately. Set it to a value in the future
	// to delay a job's execution.
	RunAt time.Time

	// Type corresponds to the Ruby job_class. If you are interoperating with
	// Ruby, you should pick suitable Ruby class names (such as MyJob).
	Type string

	// Args must be the bytes of a valid JSON string
	Args []byte

	// ErrorCount is the number of times this job has attempted to run, but
	// failed with an error. It is ignored on job creation.
	ErrorCount int32

	// LastError is the error message or stack trace from the last time the job
	// failed. It is ignored on job creation.
	LastError sql.NullString

	mu      sync.Mutex
	deleted bool
	c       *Client
	conn    *pgxpool.Conn
	tx      Txer
}

// Queryer is interface for query
type Queryer interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
}

// Txer is interface for tx
type Txer interface {
	Queryer
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// JobStats stores the statistics information for the queue and type
type JobStats struct {
	Queue             string
	Type              string
	Count             int
	CountWorking      int
	CountErrored      int
	HighestErrorCount int
	OldestRunAt       time.Time
}

// Conn returns transaction
func (j *Job) Conn() *pgxpool.Conn {
	j.mu.Lock()
	defer j.mu.Unlock()

	return j.conn
}

// Tx returns transaction
func (j *Job) Tx() Txer {
	j.mu.Lock()
	defer j.mu.Unlock()

	return j.tx
}

// Delete marks this job as complete by deleting it form the database.
//
// You must also later call Done() to return this job's database connection to
// the pool.
func (j *Job) Delete(ctx context.Context) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.deleted {
		return nil
	}

	_, err := j.conn.Exec(ctx, "que_destroy_job", j.Queue, j.Priority, j.RunAt, j.ID)
	if err != nil {
		return err
	}

	j.deleted = true
	return nil
}

// Done releases the Postgres advisory lock on the job and returns the database
// connection to the pool.
func (j *Job) Done(ctx context.Context) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.conn == nil || j.c == nil {
		// already marked as done
		return
	}

	var ok bool
	// Swallow this error because we don't want an unlock failure to cause work to
	// stop.
	if err := j.conn.QueryRow(ctx, "que_unlock_job", j.ID).Scan(&ok); err != nil {
		log.Printf("failed to unlock job job_id=%d job_type=%s", j.ID, j.Type)
	}

	j.conn.Release()
	j.c.dischargeJob(j)
	j.c = nil
	j.conn = nil
}

// Error marks the job as failed and schedules it to be reworked. An error
// message or backtrace can be provided as msg, which will be saved on the job.
// It will also increase the error count.
//
// You must also later call Done() to return this job's database connection to
// the pool.
func (j *Job) Error(ctx context.Context, msg string) error {
	errorCount := j.ErrorCount + 1
	delay := intPow(int(errorCount), 4) + 3 // TODO: configurable delay

	_, err := j.conn.Exec(ctx, "que_set_error", errorCount, delay, msg, j.Queue, j.Priority, j.RunAt, j.ID)
	if err != nil {
		return err
	}
	return nil
}

// Client is a Que client that can add jobs to the queue and remove jobs from
// the queue.
type Client struct {
	pool *pgxpool.Pool

	mu           sync.Mutex
	jobsManaged  map[int64]*Job
	// TODO: add a way to specify default queueing options
}

// NewClient creates a new Client that uses the pgx pool.
func NewClient(pool *pgxpool.Pool) (*Client, error) {
	return &Client{
		pool:         pool,
		jobsManaged:  map[int64]*Job{},
	}, nil
}

// Close disposes all the resources associated to the client
func (c *Client) Close(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pool == nil {
		return
	}
	for _, j := range c.jobsManaged {
		j.Done(ctx)
	}
	c.pool = nil
	c.jobsManaged = nil
}

// ErrMissingType is returned when you attempt to enqueue a job with no Type
// specified.
var ErrMissingType = errors.New("job type must be specified")

// Enqueue adds a job to the queue.
func (c *Client) Enqueue(ctx context.Context, j *Job) error {
	return execEnqueue(ctx, j, c.pool)
}

// EnqueueInTx adds a job to the queue within the scope of the transaction tx.
// This allows you to guarantee that an enqueued job will either be committed or
// rolled back atomically with other changes in the course of this transaction.
//
// It is the caller's responsibility to Commit or Rollback the transaction after
// this function is called.
func (c *Client) EnqueueInTx(ctx context.Context, j *Job, tx pgx.Tx) error {
	return execEnqueue(ctx, j, tx)
}

func execEnqueue(ctx context.Context, j *Job, q Queryer) error {
	if j.Type == "" {
		return ErrMissingType
	}

	queue := sql.NullString{
		String: j.Queue,
		Valid:  j.Queue != "",
	}
	if string(j.Args) == "" {
		j.Args = []byte("[]")
	}
	priority := sql.NullInt64{
		Int64: int64(j.Priority),
		Valid: j.Priority != 0,
	}
	runAt := null.Time{
		Time:  j.RunAt,
		Valid: !j.RunAt.IsZero(),
	}

	_, err := q.Exec(ctx, "que_insert_job", queue, priority, runAt, j.Type, j.Args)
	return err
}

func (c *Client) manageJob(j *Job) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.jobsManaged[j.ID] = j
}

func (c *Client) dischargeJob(j *Job) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.jobsManaged, j.ID)
}

// Maximum number of loop iterations in LockJob before giving up.  This is to
// avoid looping forever in case something is wrong.
const maxLockJobAttempts = 10

// ErrAgain Returned by LockJob if a job could not be retrieved from the queue after
// several attempts because of concurrently running transactions.  This error
// should not be returned unless the queue is under extremely heavy
// concurrency.
var ErrAgain = errors.New("maximum number of LockJob attempts reached")

// TODO: consider an alternate Enqueue func that also returns the newly
// enqueued Job struct. The query sqlInsertJobAndReturn was already written for
// this.

// LockJob attempts to retrieve a Job from the database in the specified queue.
// If a job is found, a session-level Postgres advisory lock is created for the
// Job's ID. If no job is found, nil will be returned instead of an error.
//
// Because Que uses session-level advisory locks, we have to hold the
// same connection throughout the process of getting a job, working it,
// deleting it, and removing the lock.
//
// After the Job has been worked, you must call either Done() or Error() on it
// in order to return the database connection to the pool and remove the lock.
func (c *Client) LockJob(ctx context.Context, queue string) (*Job, error) {
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	j := Job{c: c, conn: conn}

	for i := 0; i < maxLockJobAttempts; i++ {
		err = conn.QueryRow(ctx, "que_lock_job", queue).Scan(
			&j.Queue,
			&j.Priority,
			&j.RunAt,
			&j.ID,
			&j.Type,
			&j.Args,
			&j.ErrorCount,
		)
		if err != nil {
			conn.Release()

			if err == pgx.ErrNoRows {
				return nil, nil
			}
			return nil, err
		}

		// Deal with race condition. Explanation from the Ruby Que gem:
		//
		// Edge case: It's possible for the lock_job query to have
		// grabbed a job that's already been worked, if it took its MVCC
		// snapshot while the job was processing, but didn't attempt the
		// advisory lock until it was finished. Since we have the lock, a
		// previous worker would have deleted it by now, so we just
		// double check that it still exists before working it.
		//
		// Note that there is currently no spec for this behavior, since
		// I'm not sure how to reliably commit a transaction that deletes
		// the job in a separate thread between lock_job and check_job.
		var ok bool
		err = conn.QueryRow(ctx, "que_check_job", j.Queue, j.Priority, j.RunAt, j.ID).Scan(&ok)
		if err == nil {
			c.manageJob(&j)
			return &j, nil
		} else if err == pgx.ErrNoRows {
			// Encountered job race condition; start over from the beginning.
			// We're still holding the advisory lock, though, so we need to
			// release it before resuming.  Otherwise we leak the lock,
			// eventually causing the server to run out of locks.
			//
			// Also swallow the possible error, exactly like in Done.
			_ = conn.QueryRow(ctx, "que_unlock_job", j.ID).Scan(&ok)
			continue
		} else {
			conn.Release()
			return nil, err
		}
	}

	conn.Release()
	return nil, ErrAgain
}

var preparedStatements = map[string]string{
	"que_check_job":   sqlCheckJob,
	"que_destroy_job": sqlDeleteJob,
	"que_insert_job":  sqlInsertJob,
	"que_lock_job":    sqlLockJob,
	"que_set_error":   sqlSetError,
	"que_unlock_job":  sqlUnlockJob,
}

// PrepareStatements prepare statements
func PrepareStatements(ctx context.Context, conn *pgx.Conn) error {
	for name, sql := range preparedStatements {
		if _, err := conn.Prepare(ctx, name, sql); err != nil {
			return err
		}
	}
	return nil
}

// Stats retrieves the stats of all the queues
func (c *Client) Stats(ctx context.Context) (results []JobStats, err error) {
	rows, err := c.pool.Query(ctx, sqlJobStats)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var result JobStats
		err = rows.Scan(
			&result.Queue,
			&result.Type,
			&result.Count,
			&result.CountWorking,
			&result.CountErrored,
			&result.HighestErrorCount,
			&result.OldestRunAt,
		)
		if err != nil {
			return
		}
		results = append(results, result)
	}
	err = rows.Err()
	return
}
