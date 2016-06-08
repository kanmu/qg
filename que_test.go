package qg

import (
	"database/sql"
	"testing"
	"time"

	// "github.com/inconshreveable/log15"
	"github.com/jackc/pgx"
)

var testConnConfig = pgx.ConnConfig{
	Host:     "localhost",
	Database: "qgtest",
	// LogLevel: pgx.LogLevelDebug,
	// Logger:   log15.New("testlogger", "test/qg"),
}

func openTestClientMaxConns(t testing.TB, maxConnections int) (*Client, func()) {
	connPoolConfig := pgx.ConnPoolConfig{
		ConnConfig:     testConnConfig,
		MaxConnections: maxConnections,
		AcquireTimeout: time.Duration(50 * time.Millisecond),
	}
	pool, err := pgx.NewConnPool(connPoolConfig)
	if err != nil {
		t.Fatal(err)
	}
	c := NewClient(pool)
	fn := func() {
		if _, err := c.pool.Exec("TRUNCATE TABLE que_jobs"); err != nil {
			t.Fatal(err)
		}
		c.stdConn.Close()
		c.pool.Close()
	}
	return c, fn
}

func openTestClient(t testing.TB) (*Client, func()) {
	return openTestClientMaxConns(t, 5)
}

func findOneJob(q queryer) (*Job, error) {
	findSQL := `
	SELECT priority, run_at, job_id, job_class, args, error_count, last_error, queue
	FROM que_jobs LIMIT 1`

	j := &Job{}
	err := q.QueryRow(findSQL).Scan(
		&j.Priority,
		&j.RunAt,
		&j.ID,
		&j.Type,
		&j.Args,
		&j.ErrorCount,
		&j.LastError,
		&j.Queue,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return j, nil
}
