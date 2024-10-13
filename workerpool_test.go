package qg_test

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kanmu/qg/v4"
)

func TestWorkerPool(t *testing.T) {
	connector, err := qg.GetConnector("localhost", 5432, "qgtest", "", "qgtest")

	if err != nil {
		t.Fatal(err)
	}

	db := sql.OpenDB(connector)
	client := qg.MustNewClient(db)
	var wg sync.WaitGroup
	queueName1 := "queue1"

	wm := qg.WorkMap{
		"job1": func(j *qg.Job) error {
			defer wg.Done()
			tx := j.Tx()
			_, err := tx.Exec("INSERT INTO job_test (job_id, name, value) VALUES ($1, $2, $3)", j.ID, "job1", queueName1)
			return err
		},
		"job2": func(j *qg.Job) error {
			defer wg.Done()
			tx := j.Tx()
			_, err := tx.Exec("INSERT INTO job_test (job_id, name, value) VALUES ($1, $2, $3)", j.ID, "job2", queueName1)
			return err
		},
		"job3": func(j *qg.Job) error {
			defer wg.Done()
			tx := j.Tx()
			_, err := tx.Exec("INSERT INTO job_test (job_id, name, value) VALUES ($1, $2, $3)", j.ID, "job3", queueName1)
			return err
		},
		"job4": func(j *qg.Job) error {
			defer wg.Done()
			tx := j.Tx()
			_, err := tx.Exec("INSERT INTO job_test (job_id, name, value) VALUES ($1, $2, $3)", j.ID, "job4", queueName1)
			return err
		},
	}

	pool := qg.NewWorkerPool(client, wm, 3)
	pool.Queue = queueName1

	checkDB, err := sql.Open("pgx", "postgres://qgtest@localhost:5432/qgtest")
	if err != nil {
		t.Fatal(err)
	}
	_, err = checkDB.Exec("TRUNCATE TABLE que_jobs, job_test")
	if err != nil {
		t.Fatal(err)
	}

	for range 10 {
		for _, jobType := range []string{"job1", "job2", "job3", "job4"} {
			client.Enqueue(&qg.Job{Type: jobType, Queue: "queue1"})
			wg.Add(1)
		}
	}

	pool.Start()
	wg.Wait()
	pool.Shutdown()

	rs, err := checkDB.Query("SELECT name, value FROM job_test")
	if err != nil {
		t.Fatal(err)
	}

	rows := []string{}

	for rs.Next() {
		var name, value string
		err = rs.Scan(&name, &value)
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, name+","+value)
	}

	sort.Strings(rows)

	if strings.Join(rows, " ")+" " !=
		strings.Repeat("job1,queue1 ", 10)+
			strings.Repeat("job2,queue1 ", 10)+
			strings.Repeat("job3,queue1 ", 10)+
			strings.Repeat("job4,queue1 ", 10) {
		t.Errorf("unexpected result: %v", rows)
	}

	var queJobsCount int
	err = checkDB.QueryRow("SELECT COUNT(*) FROM que_jobs").Scan(&queJobsCount)
	if err != nil {
		t.Fatal(err)
	}
	if queJobsCount != 0 {
		t.Errorf("unexpected que_jobs count: %d", queJobsCount)
	}
}

func TestWorkerPoolMultiQueue(t *testing.T) {
	connector, err := qg.GetConnector("localhost", 5432, "qgtest", "", "qgtest")

	if err != nil {
		t.Fatal(err)
	}

	db := sql.OpenDB(connector)
	var pool1, pool2 *qg.WorkerPool
	var wg sync.WaitGroup

	// queue1
	{
		client := qg.MustNewClient(db)
		queueName1 := "queue1"

		wm := qg.WorkMap{
			"job1": func(j *qg.Job) error {
				defer wg.Done()
				tx := j.Tx()
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, value) VALUES ($1, $2, $3)", j.ID, "job1", queueName1)
				return err
			},
			"job2": func(j *qg.Job) error {
				defer wg.Done()
				tx := j.Tx()
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, value) VALUES ($1, $2, $3)", j.ID, "job2", queueName1)
				return err
			},
			"job3": func(j *qg.Job) error {
				defer wg.Done()
				tx := j.Tx()
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, value) VALUES ($1, $2, $3)", j.ID, "job3", queueName1)
				return err
			},
			"job4": func(j *qg.Job) error {
				defer wg.Done()
				tx := j.Tx()
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, value) VALUES ($1, $2, $3)", j.ID, "job4", queueName1)
				return err
			},
		}

		pool1 = qg.NewWorkerPool(client, wm, 2)
		pool1.Queue = queueName1
	}

	// queue2
	{
		client := qg.MustNewClient(db)
		queueName2 := "queue2"

		wm := qg.WorkMap{
			"job1": func(j *qg.Job) error {
				defer wg.Done()
				tx := j.Tx()
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, value) VALUES ($1, $2, $3)", j.ID, "job1", queueName2)
				return err
			},
			"job2": func(j *qg.Job) error {
				defer wg.Done()
				tx := j.Tx()
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, value) VALUES ($1, $2, $3)", j.ID, "job2", queueName2)
				return err
			},
			"job3": func(j *qg.Job) error {
				defer wg.Done()
				tx := j.Tx()
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, value) VALUES ($1, $2, $3)", j.ID, "job3", queueName2)
				return err
			},
			"job4": func(j *qg.Job) error {
				defer wg.Done()
				tx := j.Tx()
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, value) VALUES ($1, $2, $3)", j.ID, "job4", queueName2)
				return err
			},
		}

		pool2 = qg.NewWorkerPool(client, wm, 3)
		pool2.Queue = queueName2
	}

	checkDB, err := sql.Open("pgx", "postgres://qgtest@localhost:5432/qgtest")
	if err != nil {
		t.Fatal(err)
	}
	_, err = checkDB.Exec("TRUNCATE TABLE que_jobs, job_test")
	if err != nil {
		t.Fatal(err)
	}

	client := qg.MustNewClient(db)

	for range 3 {
		for _, jobType := range []string{"job1", "job2", "job3", "job4"} {
			client.Enqueue(&qg.Job{Type: jobType, Queue: "queue1"})
			wg.Add(1)
			client.Enqueue(&qg.Job{Type: jobType, Queue: "queue2"})
			wg.Add(1)
		}
	}

	pool1.Start()
	pool2.Start()
	wg.Wait()
	pool1.Shutdown()
	pool2.Shutdown()

	rs, err := checkDB.Query("SELECT name, value FROM job_test")
	if err != nil {
		t.Fatal(err)
	}

	rows := []string{}

	for rs.Next() {
		var name, value string
		err = rs.Scan(&name, &value)
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, name+","+value)
	}

	sort.Strings(rows)

	if strings.Join(rows, " ")+" " !=
		strings.Repeat("job1,queue1 ", 3)+
			strings.Repeat("job1,queue2 ", 3)+
			strings.Repeat("job2,queue1 ", 3)+
			strings.Repeat("job2,queue2 ", 3)+
			strings.Repeat("job3,queue1 ", 3)+
			strings.Repeat("job3,queue2 ", 3)+
			strings.Repeat("job4,queue1 ", 3)+
			strings.Repeat("job4,queue2 ", 3) {
		t.Errorf("unexpected result: %v", rows)
	}

	var queJobsCount int
	err = checkDB.QueryRow("SELECT COUNT(*) FROM que_jobs").Scan(&queJobsCount)
	if err != nil {
		t.Fatal(err)
	}
	if queJobsCount != 0 {
		t.Errorf("unexpected que_jobs count: %d", queJobsCount)
	}
}

func TestWorkerPoolMultiDB(t *testing.T) {
	var pool1, pool2 *qg.WorkerPool
	var wg sync.WaitGroup

	// queue1
	{
		connector, err := qg.GetConnector("localhost", 5432, "qgtest", "", "qgtest")

		if err != nil {
			t.Fatal(err)
		}

		db := sql.OpenDB(connector)
		client := qg.MustNewClient(db)
		queueName1 := "queue1"

		wm := qg.WorkMap{
			"job1": func(j *qg.Job) error {
				defer wg.Done()
				tx := j.Tx()
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, value) VALUES ($1, $2, $3)", j.ID, "job1", queueName1)
				return err
			},
			"job2": func(j *qg.Job) error {
				defer wg.Done()
				tx := j.Tx()
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, value) VALUES ($1, $2, $3)", j.ID, "job2", queueName1)
				return err
			},
			"job3": func(j *qg.Job) error {
				defer wg.Done()
				tx := j.Tx()
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, value) VALUES ($1, $2, $3)", j.ID, "job3", queueName1)
				return err
			},
			"job4": func(j *qg.Job) error {
				defer wg.Done()
				tx := j.Tx()
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, value) VALUES ($1, $2, $3)", j.ID, "job4", queueName1)
				return err
			},
		}

		pool1 = qg.NewWorkerPool(client, wm, 2)
		pool1.Queue = queueName1
	}

	// queue2
	{
		connector, err := qg.GetConnector("localhost", 5432, "qgtest", "", "qgtest")

		if err != nil {
			t.Fatal(err)
		}

		db := sql.OpenDB(connector)
		client := qg.MustNewClient(db)
		queue2Name := "queue2"

		wm := qg.WorkMap{
			"job1": func(j *qg.Job) error {
				defer wg.Done()
				tx := j.Tx()
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, value) VALUES ($1, $2, $3)", j.ID, "job1", queue2Name)
				return err
			},
			"job2": func(j *qg.Job) error {
				defer wg.Done()
				tx := j.Tx()
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, value) VALUES ($1, $2, $3)", j.ID, "job2", queue2Name)
				return err
			},
			"job3": func(j *qg.Job) error {
				defer wg.Done()
				tx := j.Tx()
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, value) VALUES ($1, $2, $3)", j.ID, "job3", queue2Name)
				return err
			},
			"job4": func(j *qg.Job) error {
				defer wg.Done()
				tx := j.Tx()
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, value) VALUES ($1, $2, $3)", j.ID, "job4", queue2Name)
				return err
			},
		}

		pool2 = qg.NewWorkerPool(client, wm, 3)
		pool2.Queue = queue2Name
	}

	checkDB, err := sql.Open("pgx", "postgres://qgtest@localhost:5432/qgtest")
	if err != nil {
		t.Fatal(err)
	}
	_, err = checkDB.Exec("TRUNCATE TABLE que_jobs, job_test")
	if err != nil {
		t.Fatal(err)
	}

	connector, err := qg.GetConnector("localhost", 5432, "qgtest", "", "qgtest")

	if err != nil {
		t.Fatal(err)
	}

	db := sql.OpenDB(connector)
	client := qg.MustNewClient(db)

	for range 5 {
		for _, jobType := range []string{"job1", "job2", "job3", "job4"} {
			client.Enqueue(&qg.Job{Type: jobType, Queue: "queue1"})
			wg.Add(1)
			client.Enqueue(&qg.Job{Type: jobType, Queue: "queue2"})
			wg.Add(1)
		}
	}

	pool1.Start()
	pool2.Start()
	wg.Wait()
	pool1.Shutdown()
	pool2.Shutdown()

	rs, err := checkDB.Query("SELECT name, value FROM job_test")
	if err != nil {
		t.Fatal(err)
	}

	rows := []string{}

	for rs.Next() {
		var name, value string
		err = rs.Scan(&name, &value)
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, name+","+value)
	}

	sort.Strings(rows)

	if strings.Join(rows, " ")+" " !=
		strings.Repeat("job1,queue1 ", 5)+
			strings.Repeat("job1,queue2 ", 5)+
			strings.Repeat("job2,queue1 ", 5)+
			strings.Repeat("job2,queue2 ", 5)+
			strings.Repeat("job3,queue1 ", 5)+
			strings.Repeat("job3,queue2 ", 5)+
			strings.Repeat("job4,queue1 ", 5)+
			strings.Repeat("job4,queue2 ", 5) {
		t.Errorf("unexpected result: %v", rows)
	}

	var queJobsCount int
	err = checkDB.QueryRow("SELECT COUNT(*) FROM que_jobs").Scan(&queJobsCount)
	if err != nil {
		t.Fatal(err)
	}
	if queJobsCount != 0 {
		t.Errorf("unexpected que_jobs count: %d", queJobsCount)
	}
}

func TestWorkerPoolSingleQueueMultiDB(t *testing.T) {
	var pool1, pool2 *qg.WorkerPool
	var wg sync.WaitGroup
	theOneQueue := "the-one-queue"

	wm := qg.WorkMap{
		"job1": func(j *qg.Job) error {
			defer wg.Done()
			tx := j.Tx()
			_, err := tx.Exec("INSERT INTO job_test (job_id, name, value) VALUES ($1, $2, pg_backend_pid())", j.ID, "job1")
			time.Sleep(300 * time.Millisecond)
			return err
		},
		"job2": func(j *qg.Job) error {
			defer wg.Done()
			tx := j.Tx()
			_, err := tx.Exec("INSERT INTO job_test (job_id, name, value) VALUES ($1, $2, pg_backend_pid())", j.ID, "job2")
			time.Sleep(300 * time.Millisecond)
			return err
		},
		"job3": func(j *qg.Job) error {
			defer wg.Done()
			tx := j.Tx()
			_, err := tx.Exec("INSERT INTO job_test (job_id, name, value) VALUES ($1, $2, pg_backend_pid())", j.ID, "job3")
			time.Sleep(300 * time.Millisecond)
			return err
		},
		"job4": func(j *qg.Job) error {
			defer wg.Done()
			tx := j.Tx()
			time.Sleep(300 * time.Millisecond)
			_, err := tx.Exec("INSERT INTO job_test (job_id, name, value) VALUES ($1, $2, pg_backend_pid())", j.ID, "job4")
			return err
		},
	}

	// thread1
	{
		connector, err := qg.GetConnector("localhost", 5432, "qgtest", "", "qgtest")

		if err != nil {
			t.Fatal(err)
		}

		db := sql.OpenDB(connector)
		client := qg.MustNewClient(db)
		pool1 = qg.NewWorkerPool(client, wm, 4)
		pool1.Queue = theOneQueue
	}

	// thread2
	{
		connector, err := qg.GetConnector("localhost", 5432, "qgtest", "", "qgtest")

		if err != nil {
			t.Fatal(err)
		}

		db := sql.OpenDB(connector)
		client := qg.MustNewClient(db)
		pool2 = qg.NewWorkerPool(client, wm, 4)
		pool2.Queue = theOneQueue
	}

	checkDB, err := sql.Open("pgx", "postgres://qgtest@localhost:5432/qgtest")
	if err != nil {
		t.Fatal(err)
	}
	_, err = checkDB.Exec("TRUNCATE TABLE que_jobs, job_test")
	if err != nil {
		t.Fatal(err)
	}

	connector, err := qg.GetConnector("localhost", 5432, "qgtest", "", "qgtest")

	if err != nil {
		t.Fatal(err)
	}

	db := sql.OpenDB(connector)
	client := qg.MustNewClient(db)

	for range 2 {
		for _, jobType := range []string{"job1", "job2", "job3", "job4"} {
			client.Enqueue(&qg.Job{Type: jobType, Queue: theOneQueue})
			wg.Add(1)
		}
	}

	pool1.Start()
	pool2.Start()
	wg.Wait()
	pool1.Shutdown()
	pool2.Shutdown()

	rs, err := checkDB.Query("SELECT name, value, COUNT(*) as count FROM job_test GROUP BY name, value")
	if err != nil {
		t.Fatal(err)
	}

	rows := []string{}

	for rs.Next() {
		var name, _value string
		var count int
		err = rs.Scan(&name, &_value, &count)
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, fmt.Sprintf("%s,%d", name, count))
	}

	sort.Strings(rows)

	if strings.Join(rows, " ") != "job1,1 job1,1 job2,1 job2,1 job3,1 job3,1 job4,1 job4,1" {
		t.Errorf("unexpected result: %v", rows)
	}

	var queJobsCount int
	err = checkDB.QueryRow("SELECT COUNT(*) FROM que_jobs").Scan(&queJobsCount)
	if err != nil {
		t.Fatal(err)
	}
	if queJobsCount != 0 {
		t.Errorf("unexpected que_jobs count: %d", queJobsCount)
	}
}
