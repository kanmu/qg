package qg

import (
	"context"
	"testing"
	"time"
)

func TestEnqueueOnlyType(t *testing.T) {
	ctx := context.Background()
	c := openTestClient(ctx, t)
	defer truncateAndClose(ctx, c)

	if err := c.Enqueue(ctx, &Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	j, err := findOneJob(ctx, c.pool)
	if err != nil {
		t.Fatal(err)
	}

	// check resulting job
	if j.ID == 0 {
		t.Errorf("want non-zero ID")
	}
	if want := ""; j.Queue != want {
		t.Errorf("want Queue=%q, got %q", want, j.Queue)
	}
	if want := int16(100); j.Priority != want {
		t.Errorf("want Priority=%d, got %d", want, j.Priority)
	}
	if j.RunAt.IsZero() {
		t.Error("want non-zero RunAt")
	}
	if want := "MyJob"; j.Type != want {
		t.Errorf("want Type=%q, got %q", want, j.Type)
	}
	if want, got := "[]", string(j.Args); got != want {
		t.Errorf("want Args=%s, got %s", want, got)
	}
	if want := int32(0); j.ErrorCount != want {
		t.Errorf("want ErrorCount=%d, got %d", want, j.ErrorCount)
	}
	if j.LastError.Valid {
		t.Errorf("want no LastError, got %v", j.LastError)
	}
}

func TestEnqueueWithPriority(t *testing.T) {
	ctx := context.Background()
	c := openTestClient(ctx, t)
	defer truncateAndClose(ctx, c)

	want := int16(99)
	if err := c.Enqueue(ctx, &Job{Type: "MyJob", Priority: want}); err != nil {
		t.Fatal(err)
	}

	j, err := findOneJob(ctx, c.pool)
	if err != nil {
		t.Fatal(err)
	}

	if j.Priority != want {
		t.Errorf("want Priority=%d, got %d", want, j.Priority)
	}
}

func TestEnqueueWithRunAt(t *testing.T) {
	ctx := context.Background()
	c := openTestClient(ctx, t)
	defer truncateAndClose(ctx, c)

	want := time.Now().Add(2 * time.Minute)
	if err := c.Enqueue(ctx, &Job{Type: "MyJob", RunAt: want}); err != nil {
		t.Fatal(err)
	}

	j, err := findOneJob(ctx, c.pool)
	if err != nil {
		t.Fatal(err)
	}

	// truncate to the microsecond as postgres driver does
	want = want.Truncate(time.Microsecond)
	if !want.Equal(j.RunAt) {
		t.Errorf("want RunAt=%s, got %s", want, j.RunAt)
	}
}

func TestEnqueueWithArgs(t *testing.T) {
	ctx := context.Background()
	c := openTestClient(ctx, t)
	defer truncateAndClose(ctx, c)

	want := `{"arg1":0, "arg2":"a string"}`
	if err := c.Enqueue(ctx, &Job{Type: "MyJob", Args: []byte(want)}); err != nil {
		t.Fatal(err)
	}

	j, err := findOneJob(ctx, c.pool)
	if err != nil {
		t.Fatal(err)
	}

	if got := string(j.Args); got != want {
		t.Errorf("want Args=%s, got %s", want, got)
	}
}

func TestEnqueueWithQueue(t *testing.T) {
	ctx := context.Background()
	c := openTestClient(ctx, t)
	defer truncateAndClose(ctx, c)

	want := "special-work-queue"
	if err := c.Enqueue(ctx, &Job{Type: "MyJob", Queue: want}); err != nil {
		t.Fatal(err)
	}

	j, err := findOneJob(ctx, c.pool)
	if err != nil {
		t.Fatal(err)
	}

	if j.Queue != want {
		t.Errorf("want Queue=%q, got %q", want, j.Queue)
	}
}

func TestEnqueueWithEmptyType(t *testing.T) {
	ctx := context.Background()
	c := openTestClient(ctx, t)
	defer truncateAndClose(ctx, c)

	if err := c.Enqueue(ctx, &Job{Type: ""}); err != ErrMissingType {
		t.Fatalf("want ErrMissingType, got %v", err)
	}
}

func TestEnqueueInTx(t *testing.T) {
	ctx := context.Background()
	c := openTestClient(ctx, t)
	defer truncateAndClose(ctx, c)

	tx, err := c.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if err = c.EnqueueInTx(ctx, &Job{Type: "MyJob"}, tx); err != nil {
		t.Fatal(err)
	}

	j, err := findOneJob(ctx, tx)
	if err != nil {
		t.Fatal(err)
	}
	if j == nil {
		t.Fatal("want job, got none")
	}

	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	j, err = findOneJob(ctx, c.pool)
	if err != nil {
		t.Fatal(err)
	}
	if j != nil {
		t.Fatalf("wanted job to be rolled back, got %+v", j)
	}
}
