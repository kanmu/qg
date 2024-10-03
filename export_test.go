package qg

import (
	"database/sql"
)

var RawConn = rawConn

func (c *Client) TestGetPool() *sql.DB {
	return c.pool
}

func (j *Job) TestGetClient() *Client {
	return j.c
}

func (w *Worker) TestGetDone() bool {
	return w.done
}
