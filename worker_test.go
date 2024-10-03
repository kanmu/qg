package qg_test

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/kanmu/qg/v5"
)

func init() {
	log.SetOutput(io.Discard)
}

func TestWorkerWorkOne(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c)

	success := false
	wm := qg.WorkMap{
		"MyJob": func(j *qg.Job) error {
			success = true
			return nil
		},
	}
	w := qg.NewWorker(c, wm)

	didWork := w.WorkOne()
	if didWork {
		t.Errorf("want didWork=false when no job was queued")
	}

	if err := c.Enqueue(&qg.Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	didWork = w.WorkOne()
	if !didWork {
		t.Errorf("want didWork=true")
	}
	if !success {
		t.Errorf("want success=true")
	}
}

func TestWorkerWorkOneWithCustomConnector(t *testing.T) {
	callNumWrappedConn := 0

	c := openTestClientMaxConns(t, maxConn, func(c driver.Connector) *sql.DB {
		connector := NewTestConnector(c, &callNumWrappedConn)
		return sql.OpenDB(connector)
	})

	defer truncateAndClose(c)

	success := false
	wm := qg.WorkMap{
		"MyJob": func(j *qg.Job) error {
			success = true
			return nil
		},
	}
	w := qg.NewWorker(c, wm)

	didWork := w.WorkOne()
	if didWork {
		t.Errorf("want didWork=false when no job was queued")
	}

	if err := c.Enqueue(&qg.Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	didWork = w.WorkOne()
	if !didWork {
		t.Errorf("want didWork=true")
	}
	if !success {
		t.Errorf("want success=true")
	}

	if callNumWrappedConn != 5 {
		t.Errorf("want callNumWrappedConn=5")
	}
}

func TestWorkerShutdown(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c)

	w := qg.NewWorker(c, qg.WorkMap{})
	finished := false
	go func() {
		w.Work()
		finished = true
	}()
	w.Shutdown()
	if !finished {
		t.Errorf("want finished=true")
	}
	if !w.TestGetDone() {
		t.Errorf("want w.done=true")
	}
}

func BenchmarkWorker(b *testing.B) {
	c := openTestClient(b)
	log.SetOutput(io.Discard)
	defer func() {
		log.SetOutput(os.Stdout)
	}()
	defer truncateAndClose(c)

	w := qg.NewWorker(c, qg.WorkMap{"Nil": nilWorker})

	for i := 0; i < b.N; i++ {
		if err := c.Enqueue(&qg.Job{Type: "Nil"}); err != nil {
			log.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.WorkOne()
	}
}

func nilWorker(j *qg.Job) error {
	return nil
}

func TestWorkerWorkReturnsError(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c)

	called := 0
	wm := qg.WorkMap{
		"MyJob": func(j *qg.Job) error {
			called++
			return fmt.Errorf("the error msg")
		},
	}
	w := qg.NewWorker(c, wm)

	didWork := w.WorkOne()
	if didWork {
		t.Errorf("want didWork=false when no job was queued")
	}

	if err := c.Enqueue(&qg.Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	didWork = w.WorkOne()
	if !didWork {
		t.Errorf("want didWork=true")
	}
	if called != 1 {
		t.Errorf("want called=1 was: %d", called)
	}

	tx, err := c.TestGetPool().Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback() //nolint:errcheck

	j, err := findOneJob(tx)
	if err != nil {
		t.Fatal(err)
	}
	if j.ErrorCount != 1 {
		t.Errorf("want ErrorCount=1 was %d", j.ErrorCount)
	}
	if !j.LastError.Valid {
		t.Errorf("want LastError IS NOT NULL")
	}
	if j.LastError.String != "the error msg" {
		t.Errorf("want LastError=\"the error msg\" was: %q", j.LastError.String)
	}
}

func TestWorkerWorkRescuesPanic(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c)

	called := 0
	wm := qg.WorkMap{
		"MyJob": func(j *qg.Job) error {
			called++
			panic("the panic msg")
		},
	}
	w := qg.NewWorker(c, wm)

	if err := c.Enqueue(&qg.Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	w.WorkOne()
	if called != 1 {
		t.Errorf("want called=1 was: %d", called)
	}

	tx, err := c.TestGetPool().Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback() //nolint:errcheck

	j, err := findOneJob(tx)
	if err != nil {
		t.Fatal(err)
	}
	if j.ErrorCount != 1 {
		t.Errorf("want ErrorCount=1 was %d", j.ErrorCount)
	}
	if !j.LastError.Valid {
		t.Errorf("want LastError IS NOT NULL")
	}
	if !strings.Contains(j.LastError.String, "the panic msg\n") {
		t.Errorf("want LastError contains \"the panic msg\\n\" was: %q", j.LastError.String)
	}
	// basic check if a stacktrace is there - not the stacktrace format itself
	if !strings.Contains(j.LastError.String, "worker.go:") {
		t.Errorf("want LastError contains \"worker.go:\" was: %q", j.LastError.String)
	}
	if !strings.Contains(j.LastError.String, "worker_test.go:") {
		t.Errorf("want LastError contains \"worker_test.go:\" was: %q", j.LastError.String)
	}
}

func TestWorkerWorkOneTypeNotInMap(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c)

	currentConns := 2
	availConns := 2

	success := false
	wm := qg.WorkMap{}
	w := qg.NewWorker(c, wm)

	didWork := w.WorkOne()
	if didWork {
		t.Errorf("want didWork=false when no job was queued")
	}

	if err := c.Enqueue(&qg.Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	didWork = w.WorkOne()
	if !didWork {
		t.Errorf("want didWork=true")
	}
	if success {
		t.Errorf("want success=false")
	}

	if currentConns != c.TestGetPool().Stats().OpenConnections {
		t.Errorf("want currentConns euqual: before=%d  after=%d", currentConns, c.TestGetPool().Stats().OpenConnections)
	}
	if availConns != c.TestGetPool().Stats().OpenConnections {
		t.Errorf("want availConns euqual: before=%d  after=%d", availConns, c.TestGetPool().Stats().OpenConnections)
	}

	tx, err := c.TestGetPool().Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback() //nolint:errcheck

	j, err := findOneJob(tx)
	if err != nil {
		t.Fatal(err)
	}
	if j.ErrorCount != 1 {
		t.Errorf("want ErrorCount=1 was %d", j.ErrorCount)
	}
	if !j.LastError.Valid {
		t.Fatal("want non-nil LastError")
	}
	if want := "unknown job type: \"MyJob\""; j.LastError.String != want {
		t.Errorf("want LastError=%q, got %q", want, j.LastError.String)
	}

}
