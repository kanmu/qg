package qg

import (
	"database/sql"
	"testing"
	"time"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/stdlib"
)

var testConnConfig = pgx.ConnConfig{
	Host:     "localhost",
	Database: "qgtest",
	User:     "qgtest",
	// LogLevel: pgx.LogLevelDebug,
	// Logger:   log15.New("testlogger", "test/qg"),
}

const maxConn = 5

func openTestClientMaxConns(t testing.TB, maxConnections int) *Client {
	// connPoolConfig := pgx.ConnPoolConfig{
	// 	ConnConfig:     testConnConfig,
	// 	MaxConnections: maxConnections,
	// 	AfterConnect:   PrepareStatements,
	// }
	// pool, err := pgx.NewConnPool(connPoolConfig)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	driverConfig := stdlib.DriverConfig{
		ConnConfig: pgx.ConnConfig{
			Host:     "localhost",
			Database: "qgtest",
			User:     "qgtest",
		},
		AfterConnect: PrepareStatements,
	}
	stdlib.RegisterDriverConfig(&driverConfig)
	db, err := sql.Open("pgx", driverConfig.ConnectionString(""))
	if err != nil {
		t.Fatal(err)
	}
	// using stdlib, it's difficult to open max conn from the begining
	// if we want to open connections till its limit, need to use go routine to
	// concurrently open connections
	db.SetMaxOpenConns(maxConnections)
	db.SetMaxIdleConns(maxConnections)
	// make lifetime sufficiently long
	db.SetConnMaxLifetime(time.Duration(5 * time.Minute))
	c, err := NewClient2(db)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func openTestClient(t testing.TB) *Client {
	return openTestClientMaxConns(t, maxConn)
}

func truncateAndClose(c *Client) {
	pool := c.pool
	c.Close()
	if _, err := pool.Exec("TRUNCATE TABLE que_jobs"); err != nil {
		panic(err)
	}
	pool.Close()
}

func findOneJob(q Queryer) (*Job, error) {
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
