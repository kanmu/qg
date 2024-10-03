package qg_test

import (
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/kanmu/qg/v4"
	sqltracer "gopkg.in/DataDog/dd-trace-go.v1/contrib/database/sql"
)

var testConnConfig = func() *pgx.ConnConfig {
	conn, _ := pgx.ParseConfig("postgres://qgtest@localhost:5432/qgtest")
	// conn.LogLevel = pgx.LogLevelDebug
	// conn.Logger = log15.New("testlogger", "test/qg")
	return conn
}()

const maxConn = 5

func openTestClientMaxConns(t testing.TB, maxConnections int, openDB func(driver.Connector) *sql.DB) *qg.Client {
	connector, err := qg.GetConnector("localhost", 5432, "qgtest", "", "qgtest")
	if err != nil {
		t.Fatal(err)
	}
	db := openDB(connector)
	// using stdlib, it's difficult to open max conn from the beginning
	// if we want to open connections till its limit, need to use go routine to
	// concurrently open connections
	db.SetMaxOpenConns(maxConnections)
	db.SetMaxIdleConns(maxConnections)
	// make lifetime sufficiently long
	db.SetConnMaxLifetime(time.Duration(5 * time.Minute))
	c, err := qg.NewClient(db)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func openTestClient(t testing.TB) *qg.Client {
	return openTestClientMaxConns(t, maxConn, sql.OpenDB)
}

func openTestClientWithTracer(t testing.TB) *qg.Client {
	return openTestClientMaxConns(t, maxConn, func(c driver.Connector) *sql.DB {
		return sqltracer.OpenDB(c)
	})
}

func truncateAndClose(c *qg.Client) {
	pool := c.TestGetPool()
	c.Close()
	if _, err := pool.Exec("TRUNCATE TABLE que_jobs"); err != nil {
		panic(err)
	}
	pool.Close()
}

func findOneJob(q qg.Queryer) (*qg.Job, error) {
	findSQL := `
	SELECT priority, run_at, job_id, job_class, args, error_count, last_error, queue
	FROM que_jobs LIMIT 1`

	j := &qg.Job{}
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
