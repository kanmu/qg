package qg

// TestInjectJobConn injects *pgx.Conn to Job
func TestInjectJobConn(j *Job, conn Conner) *Job {
	j.stdConn = conn
	return j
}

// TestInjectJobTx injects tx to Job
func TestInjectJobTx(j *Job, tx Txer) *Job {
	j.tx = tx
	return j
}
