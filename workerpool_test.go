package qg_test

import (
	"database/sql"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/kanmu/qg/v5"
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
			_, err := tx.Exec("INSERT INTO job_test (job_id, name, queue) VALUES ($1, $2, $3)", j.ID, "job1", queueName1)
			return err
		},
		"job2": func(j *qg.Job) error {
			defer wg.Done()
			tx := j.Tx()
			_, err := tx.Exec("INSERT INTO job_test (job_id, name, queue) VALUES ($1, $2, $3)", j.ID, "job2", queueName1)
			return err
		},
		"job3": func(j *qg.Job) error {
			defer wg.Done()
			tx := j.Tx()
			_, err := tx.Exec("INSERT INTO job_test (job_id, name, queue) VALUES ($1, $2, $3)", j.ID, "job3", queueName1)
			return err
		},
		"job4": func(j *qg.Job) error {
			defer wg.Done()
			tx := j.Tx()
			_, err := tx.Exec("INSERT INTO job_test (job_id, name, queue) VALUES ($1, $2, $3)", j.ID, "job4", queueName1)
			return err
		},
	}

	pool := qg.NewWorkerPool(client, wm, 3)
	pool.Queue = queueName1

	checkDB, err := sql.Open("pgx", "postgres://qgtest@localhost:5432/qgtest")
	if err != nil {
		t.Fatal(err)
	}
	_, err = checkDB.Exec("TRUNCATE TABLE que_jobs")
	if err != nil {
		t.Fatal(err)
	}
	_, err = checkDB.Exec("TRUNCATE TABLE job_test")
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

	rs, err := checkDB.Query("SELECT name, queue FROM job_test")
	if err != nil {
		t.Fatal(err)
	}

	rows := []string{}

	for rs.Next() {
		var name, queue string
		err = rs.Scan(&name, &queue)
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, name+","+queue)
	}

	sort.Strings(rows)

	if strings.Join(rows, " ")+" " !=
		strings.Repeat("job1,queue1 ", 10)+
			strings.Repeat("job2,queue1 ", 10)+
			strings.Repeat("job3,queue1 ", 10)+
			strings.Repeat("job4,queue1 ", 10) {
		t.Errorf("unexpected result: %v", rows)
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
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, queue) VALUES ($1, $2, $3)", j.ID, "job1", queueName1)
				return err
			},
			"job2": func(j *qg.Job) error {
				defer wg.Done()
				tx := j.Tx()
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, queue) VALUES ($1, $2, $3)", j.ID, "job2", queueName1)
				return err
			},
			"job3": func(j *qg.Job) error {
				defer wg.Done()
				tx := j.Tx()
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, queue) VALUES ($1, $2, $3)", j.ID, "job3", queueName1)
				return err
			},
			"job4": func(j *qg.Job) error {
				defer wg.Done()
				tx := j.Tx()
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, queue) VALUES ($1, $2, $3)", j.ID, "job4", queueName1)
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
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, queue) VALUES ($1, $2, $3)", j.ID, "job1", queueName2)
				return err
			},
			"job2": func(j *qg.Job) error {
				defer wg.Done()
				tx := j.Tx()
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, queue) VALUES ($1, $2, $3)", j.ID, "job2", queueName2)
				return err
			},
			"job3": func(j *qg.Job) error {
				defer wg.Done()
				tx := j.Tx()
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, queue) VALUES ($1, $2, $3)", j.ID, "job3", queueName2)
				return err
			},
			"job4": func(j *qg.Job) error {
				defer wg.Done()
				tx := j.Tx()
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, queue) VALUES ($1, $2, $3)", j.ID, "job4", queueName2)
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
	_, err = checkDB.Exec("TRUNCATE TABLE que_jobs")
	if err != nil {
		t.Fatal(err)
	}
	_, err = checkDB.Exec("TRUNCATE TABLE job_test")
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

	rs, err := checkDB.Query("SELECT name, queue FROM job_test")
	if err != nil {
		t.Fatal(err)
	}

	rows := []string{}

	for rs.Next() {
		var name, queue string
		err = rs.Scan(&name, &queue)
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, name+","+queue)
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
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, queue) VALUES ($1, $2, $3)", j.ID, "job1", queueName1)
				return err
			},
			"job2": func(j *qg.Job) error {
				defer wg.Done()
				tx := j.Tx()
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, queue) VALUES ($1, $2, $3)", j.ID, "job2", queueName1)
				return err
			},
			"job3": func(j *qg.Job) error {
				defer wg.Done()
				tx := j.Tx()
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, queue) VALUES ($1, $2, $3)", j.ID, "job3", queueName1)
				return err
			},
			"job4": func(j *qg.Job) error {
				defer wg.Done()
				tx := j.Tx()
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, queue) VALUES ($1, $2, $3)", j.ID, "job4", queueName1)
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
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, queue) VALUES ($1, $2, $3)", j.ID, "job1", queue2Name)
				return err
			},
			"job2": func(j *qg.Job) error {
				defer wg.Done()
				tx := j.Tx()
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, queue) VALUES ($1, $2, $3)", j.ID, "job2", queue2Name)
				return err
			},
			"job3": func(j *qg.Job) error {
				defer wg.Done()
				tx := j.Tx()
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, queue) VALUES ($1, $2, $3)", j.ID, "job3", queue2Name)
				return err
			},
			"job4": func(j *qg.Job) error {
				defer wg.Done()
				tx := j.Tx()
				_, err := tx.Exec("INSERT INTO job_test (job_id, name, queue) VALUES ($1, $2, $3)", j.ID, "job4", queue2Name)
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
	_, err = checkDB.Exec("TRUNCATE TABLE que_jobs")
	if err != nil {
		t.Fatal(err)
	}
	_, err = checkDB.Exec("TRUNCATE TABLE job_test")
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

	rs, err := checkDB.Query("SELECT name, queue FROM job_test")
	if err != nil {
		t.Fatal(err)
	}

	rows := []string{}

	for rs.Next() {
		var name, queue string
		err = rs.Scan(&name, &queue)
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, name+","+queue)
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
}
