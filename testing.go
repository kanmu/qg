package qg

import "github.com/jackc/pgx/v5/pgxpool"

// // TestInjectJobConn injects *pgxpool.Conn to Job
func TestInjectJobConn(j *Job, conn *pgxpool.Conn) *Job {
	j.conn = conn
	return j
}

// TestInjectJobTx injects tx to Job
func TestInjectJobTx(j *Job, tx Txer) *Job {
	j.tx = tx
	return j
}
