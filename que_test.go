package qg

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

func newConn(ctx context.Context) *pgx.Conn {
		conn, err := pgx.Connect(ctx, "postgres://qgtest@localhost/qgtest")
		if err != nil {
			panic(err)
		}
		return conn
}

const DefaultMaxConns = 5

func openTestClientMaxConns(ctx context.Context, t testing.TB, maxConnections int) *Client {
	config, err := pgxpool.ParseConfig("postgres://qgtest@localhost/qgtest")
	if err != nil {
		t.Fatal(err)
	}

	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		return PrepareStatements(ctx, conn)
	}
	config.MaxConns = int32(maxConnections)

	// make lifetime sufficiently long
	config.MaxConnLifetime = 5 * time.Minute

	pool, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}

	c, err := NewClient(pool)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func openTestClient(ctx context.Context, t testing.TB) *Client {
	return openTestClientMaxConns(ctx, t, DefaultMaxConns)
}

func truncateAndClose(ctx context.Context, c *Client) {
	pool := c.pool
	c.Close(ctx)
	if _, err := pool.Exec(ctx, "TRUNCATE TABLE que_jobs"); err != nil {
		panic(err)
	}
	pool.Close()
}

func findOneJob(ctx context.Context, q Queryer) (*Job, error) {
	findSQL := `
	SELECT priority, run_at, job_id, job_class, args, error_count, last_error, queue
	FROM que_jobs LIMIT 1`

	j := &Job{}
	err := q.QueryRow(ctx, findSQL).Scan(
		&j.Priority,
		&j.RunAt,
		&j.ID,
		&j.Type,
		&j.Args,
		&j.ErrorCount,
		&j.LastError,
		&j.Queue,
	)
	
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return j, nil
}
