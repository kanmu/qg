package qg

import "database/sql"

// TestInjectJobConn injects *pgx.Conn to Job
func TestInjectJobConn(j *Job, conn *sql.DB) *Job {
	j.pool = conn
	return j
}

// TestInjectJobTx injects tx to Job
func TestInjectJobTx(j *Job, tx Txer) *Job {
	j.tx = tx
	return j
}
