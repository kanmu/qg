package qg

import (
	"testing"
)

func TestStats(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c)

	if err := c.Enqueue(&Job{Queue: "Q1", Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	stats, err := c.Stats()
	if err != nil {
		t.Fatal(err)
	}

	if len(stats) != 1 {
		t.Errorf("len(stats) != 1 (got %v)", len(stats))
	}

	if stats[0].Queue != "Q1" {
		t.Errorf("stats[0].Queue != \"Q1\" (got %v)", stats[0].Queue)
	}

	if stats[0].Type != "MyJob" {
		t.Errorf("stats[0].Type != \"MyJob\" (got %v)", stats[0].Type)
	}

	if stats[0].Count != 1 {
		t.Errorf("stats[0].Count != 1 (got %v)", stats[0].Count)
	}

	if stats[0].CountWorking != 0 {
		t.Errorf("stats[0].CountWorking != 0 (got %v)", stats[0].CountWorking)
	}

	if stats[0].CountErrored != 0 {
		t.Errorf("stats[0].CountErrored != 0 (got %v)", stats[0].CountErrored)
	}

	if stats[0].HighestErrorCount != 0 {
		t.Errorf("stats[0].HighestErrorCount != 0 (got %v)", stats[0].HighestErrorCount)
	}

	if stats[0].OldestRunAt.IsZero() {
		t.Errorf("stats[0].OldestRunAt.IsZero() != false (got %v)", stats[0].OldestRunAt.IsZero())
	}

	if err := c.Enqueue(&Job{Queue: "Q1", Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	stats, err = c.Stats()
	if err != nil {
		t.Fatal(err)
	}

	if len(stats) != 1 {
		t.Errorf("len(stats) != 1 (got %v)", len(stats))
	}

	if stats[0].Queue != "Q1" {
		t.Errorf("stats[0].Queue != \"Q1\" (got %v)", stats[0].Queue)
	}

	if stats[0].Type != "MyJob" {
		t.Errorf("stats[0].Type != \"MyJob\" (got %v)", stats[0].Type)
	}

	if stats[0].Count != 2 {
		t.Errorf("stats[0].Count != 1 (got %v)", stats[0].Count)
	}

	if stats[0].CountWorking != 0 {
		t.Errorf("stats[0].CountWorking != 0 (got %v)", stats[0].CountWorking)
	}

	if stats[0].CountErrored != 0 {
		t.Errorf("stats[0].CountErrored != 0 (got %v)", stats[0].CountErrored)
	}

	if stats[0].HighestErrorCount != 0 {
		t.Errorf("stats[0].HighestErrorCount != 0 (got %v)", stats[0].HighestErrorCount)
	}

	if stats[0].OldestRunAt.IsZero() {
		t.Errorf("stats[0].OldestRunAt.IsZero() != false (got %v)", stats[0].OldestRunAt.IsZero())
	}

	if err := c.Enqueue(&Job{Queue: "Q2", Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	stats, err = c.Stats()
	if err != nil {
		t.Fatal(err)
	}

	if len(stats) != 2 {
		t.Errorf("len(stats) != 1 (got %v)", len(stats))
	}

	if stats[0].Queue != "Q1" {
		t.Errorf("stats[0].Queue != \"Q1\" (got %v)", stats[0].Queue)
	}

	if stats[0].Type != "MyJob" {
		t.Errorf("stats[0].Type != \"MyJob\" (got %v)", stats[0].Type)
	}

	if stats[0].Count != 2 {
		t.Errorf("stats[0].Count != 1 (got %v)", stats[0].Count)
	}

	if stats[0].CountWorking != 0 {
		t.Errorf("stats[0].CountWorking != 0 (got %v)", stats[0].CountWorking)
	}

	if stats[0].CountErrored != 0 {
		t.Errorf("stats[0].CountErrored != 0 (got %v)", stats[0].CountErrored)
	}

	if stats[0].HighestErrorCount != 0 {
		t.Errorf("stats[0].HighestErrorCount != 0 (got %v)", stats[0].HighestErrorCount)
	}

	if stats[0].OldestRunAt.IsZero() {
		t.Errorf("stats[0].OldestRunAt.IsZero() != false (got %v)", stats[0].OldestRunAt.IsZero())
	}

	if stats[1].Queue != "Q2" {
		t.Errorf("stats[1].Queue != \"Q2\" (got %v)", stats[1].Queue)
	}

	if stats[1].Type != "MyJob" {
		t.Errorf("stats[1].Type != \"MyJob\" (got %v)", stats[1].Type)
	}

	if stats[1].Count != 1 {
		t.Errorf("stats[1].Count != 1 (got %v)", stats[1].Count)
	}

	if stats[1].CountWorking != 0 {
		t.Errorf("stats[1].CountWorking != 0 (got %v)", stats[1].CountWorking)
	}

	if stats[1].CountErrored != 0 {
		t.Errorf("stats[1].CountErrored != 0 (got %v)", stats[1].CountErrored)
	}

	if stats[1].HighestErrorCount != 0 {
		t.Errorf("stats[1].HighestErrorCount != 0 (got %v)", stats[1].HighestErrorCount)
	}

	if stats[1].OldestRunAt.IsZero() {
		t.Errorf("stats[1].OldestRunAt.IsZero() != false (got %v)", stats[1].OldestRunAt.IsZero())
	}

	if err := c.Enqueue(&Job{Queue: "Q1", Type: "AnotherJob"}); err != nil {
		t.Fatal(err)
	}

	stats, err = c.Stats()
	if err != nil {
		t.Fatal(err)
	}

	if len(stats) != 3 {
		t.Errorf("len(stats) != 3 (got %v)", len(stats))
	}

	if stats[0].Queue != "Q1" {
		t.Errorf("stats[0].Queue != \"Q1\" (got %v)", stats[0].Queue)
	}

	if stats[0].Type != "AnotherJob" {
		t.Errorf("stats[0].Type != \"AnotherJob\" (got %v)", stats[0].Type)
	}

	if stats[0].Count != 1 {
		t.Errorf("stats[0].Count != 1 (got %v)", stats[0].Count)
	}

	if stats[0].CountWorking != 0 {
		t.Errorf("stats[0].CountWorking != 0 (got %v)", stats[0].CountWorking)
	}

	if stats[0].CountErrored != 0 {
		t.Errorf("stats[0].CountErrored != 0 (got %v)", stats[0].CountErrored)
	}

	if stats[0].HighestErrorCount != 0 {
		t.Errorf("stats[0].HighestErrorCount != 0 (got %v)", stats[0].HighestErrorCount)
	}

	if stats[0].OldestRunAt.IsZero() {
		t.Errorf("stats[0].OldestRunAt.IsZero() != false (got %v)", stats[0].OldestRunAt.IsZero())
	}

	if stats[1].Queue != "Q1" {
		t.Errorf("stats[1].Queue != \"Q1\" (got %v)", stats[1].Queue)
	}

	if stats[1].Type != "MyJob" {
		t.Errorf("stats[1].Type != \"MyJob\" (got %v)", stats[1].Type)
	}

	if stats[1].Count != 2 {
		t.Errorf("stats[1].Count != 1 (got %v)", stats[1].Count)
	}

	if stats[1].CountWorking != 0 {
		t.Errorf("stats[1].CountWorking != 0 (got %v)", stats[1].CountWorking)
	}

	if stats[1].CountErrored != 0 {
		t.Errorf("stats[1].CountErrored != 0 (got %v)", stats[1].CountErrored)
	}

	if stats[1].HighestErrorCount != 0 {
		t.Errorf("stats[1].HighestErrorCount != 0 (got %v)", stats[1].HighestErrorCount)
	}

	if stats[0].OldestRunAt.IsZero() {
		t.Errorf("stats[0].OldestRunAt.IsZero() != false (got %v)", stats[0].OldestRunAt.IsZero())
	}

	if stats[2].Queue != "Q2" {
		t.Errorf("stats[2].Queue != \"Q2\" (got %v)", stats[2].Queue)
	}

	if stats[2].Type != "MyJob" {
		t.Errorf("stats[2].Type != \"MyJob\" (got %v)", stats[2].Type)
	}

	if stats[2].Count != 1 {
		t.Errorf("stats[2].Count != 1 (got %v)", stats[2].Count)
	}

	if stats[2].CountWorking != 0 {
		t.Errorf("stats[2].CountWorking != 0 (got %v)", stats[2].CountWorking)
	}

	if stats[2].CountErrored != 0 {
		t.Errorf("stats[2].CountErrored != 0 (got %v)", stats[2].CountErrored)
	}

	if stats[2].HighestErrorCount != 0 {
		t.Errorf("stats[2].HighestErrorCount != 0 (got %v)", stats[2].HighestErrorCount)
	}

	if stats[2].OldestRunAt.IsZero() {
		t.Errorf("stats[2].OldestRunAt.IsZero() != false (got %v)", stats[2].OldestRunAt.IsZero())
	}

	func() {
		j, err := c.LockJob("Q1")
		if err != nil {
			t.Fatal(err)
		}
		if j == nil {
			t.Fatal(err)
		}
		defer j.Done()

		stats, err = c.Stats()
		if err != nil {
			t.Fatal(err)
		}

		if len(stats) != 3 {
			t.Errorf("len(stats) != 3 (got %v)", len(stats))
		}

		if stats[0].Queue != "Q1" {
			t.Errorf("stats[0].Queue != \"Q1\" (got %v)", stats[0].Queue)
		}

		if stats[0].Type != "AnotherJob" {
			t.Errorf("stats[0].Type != \"AnotherJob\" (got %v)", stats[0].Type)
		}

		if stats[0].Count != 1 {
			t.Errorf("stats[0].Count != 1 (got %v)", stats[0].Count)
		}

		if stats[0].CountWorking != 0 {
			t.Errorf("stats[0].CountWorking != 0 (got %v)", stats[0].CountWorking)
		}

		if stats[0].CountErrored != 0 {
			t.Errorf("stats[0].CountErrored != 0 (got %v)", stats[0].CountErrored)
		}

		if stats[0].HighestErrorCount != 0 {
			t.Errorf("stats[0].HighestErrorCount != 0 (got %v)", stats[0].HighestErrorCount)
		}

		if stats[0].OldestRunAt.IsZero() {
			t.Errorf("stats[0].OldestRunAt.IsZero() != false (got %v)", stats[0].OldestRunAt.IsZero())
		}

		if stats[1].Queue != "Q1" {
			t.Errorf("stats[1].Queue != \"Q1\" (got %v)", stats[1].Queue)
		}

		if stats[1].Type != "MyJob" {
			t.Errorf("stats[1].Type != \"MyJob\" (got %v)", stats[1].Type)
		}

		if stats[1].Count != 2 {
			t.Errorf("stats[1].Count != 1 (got %v)", stats[1].Count)
		}

		if stats[1].CountWorking != 1 {
			t.Errorf("stats[1].CountWorking != 1 (got %v)", stats[1].CountWorking)
		}

		if stats[1].CountErrored != 0 {
			t.Errorf("stats[1].CountErrored != 0 (got %v)", stats[1].CountErrored)
		}

		if stats[1].HighestErrorCount != 0 {
			t.Errorf("stats[1].HighestErrorCount != 0 (got %v)", stats[1].HighestErrorCount)
		}

		if stats[0].OldestRunAt.IsZero() {
			t.Errorf("stats[0].OldestRunAt.IsZero() != false (got %v)", stats[0].OldestRunAt.IsZero())
		}

		if stats[2].Queue != "Q2" {
			t.Errorf("stats[2].Queue != \"Q2\" (got %v)", stats[2].Queue)
		}

		if stats[2].Type != "MyJob" {
			t.Errorf("stats[2].Type != \"MyJob\" (got %v)", stats[2].Type)
		}

		if stats[2].Count != 1 {
			t.Errorf("stats[2].Count != 1 (got %v)", stats[2].Count)
		}

		if stats[2].CountWorking != 0 {
			t.Errorf("stats[2].CountWorking != 0 (got %v)", stats[2].CountWorking)
		}

		if stats[2].CountErrored != 0 {
			t.Errorf("stats[2].CountErrored != 0 (got %v)", stats[2].CountErrored)
		}

		if stats[2].HighestErrorCount != 0 {
			t.Errorf("stats[2].HighestErrorCount != 0 (got %v)", stats[2].HighestErrorCount)
		}

		if stats[2].OldestRunAt.IsZero() {
			t.Errorf("stats[2].OldestRunAt.IsZero() != false (got %v)", stats[2].OldestRunAt.IsZero())
		}
	}()

	func() {
		j, err := c.LockJob("Q1")
		if err != nil {
			t.Fatal(err)
		}
		if j == nil {
			t.Fatal(err)
		}
		defer j.Done()
		j.Delete() //nolint:errcheck

		stats, err = c.Stats()
		if err != nil {
			t.Fatal(err)
		}

		if len(stats) != 3 {
			t.Errorf("len(stats) != 3 (got %v)", len(stats))
		}

		if stats[0].Queue != "Q1" {
			t.Errorf("stats[0].Queue != \"Q1\" (got %v)", stats[0].Queue)
		}

		if stats[0].Type != "AnotherJob" {
			t.Errorf("stats[0].Type != \"AnotherJob\" (got %v)", stats[0].Type)
		}

		if stats[0].Count != 1 {
			t.Errorf("stats[0].Count != 1 (got %v)", stats[0].Count)
		}

		if stats[0].CountWorking != 0 {
			t.Errorf("stats[0].CountWorking != 0 (got %v)", stats[0].CountWorking)
		}

		if stats[0].CountErrored != 0 {
			t.Errorf("stats[0].CountErrored != 0 (got %v)", stats[0].CountErrored)
		}

		if stats[0].HighestErrorCount != 0 {
			t.Errorf("stats[0].HighestErrorCount != 0 (got %v)", stats[0].HighestErrorCount)
		}

		if stats[0].OldestRunAt.IsZero() {
			t.Errorf("stats[0].OldestRunAt.IsZero() != false (got %v)", stats[0].OldestRunAt.IsZero())
		}

		if stats[1].Queue != "Q1" {
			t.Errorf("stats[1].Queue != \"Q1\" (got %v)", stats[1].Queue)
		}

		if stats[1].Type != "MyJob" {
			t.Errorf("stats[1].Type != \"MyJob\" (got %v)", stats[1].Type)
		}

		if stats[1].Count != 1 {
			t.Errorf("stats[1].Count != 1 (got %v)", stats[1].Count)
		}

		if stats[1].CountWorking != 0 {
			t.Errorf("stats[1].CountWorking != 0 (got %v)", stats[1].CountWorking)
		}

		if stats[1].CountErrored != 0 {
			t.Errorf("stats[1].CountErrored != 0 (got %v)", stats[1].CountErrored)
		}

		if stats[1].HighestErrorCount != 0 {
			t.Errorf("stats[1].HighestErrorCount != 0 (got %v)", stats[1].HighestErrorCount)
		}

		if stats[0].OldestRunAt.IsZero() {
			t.Errorf("stats[0].OldestRunAt.IsZero() != false (got %v)", stats[0].OldestRunAt.IsZero())
		}

		if stats[2].Queue != "Q2" {
			t.Errorf("stats[2].Queue != \"Q2\" (got %v)", stats[2].Queue)
		}

		if stats[2].Type != "MyJob" {
			t.Errorf("stats[2].Type != \"MyJob\" (got %v)", stats[2].Type)
		}

		if stats[2].Count != 1 {
			t.Errorf("stats[2].Count != 1 (got %v)", stats[2].Count)
		}

		if stats[2].CountWorking != 0 {
			t.Errorf("stats[2].CountWorking != 0 (got %v)", stats[2].CountWorking)
		}

		if stats[2].CountErrored != 0 {
			t.Errorf("stats[2].CountErrored != 0 (got %v)", stats[2].CountErrored)
		}

		if stats[2].HighestErrorCount != 0 {
			t.Errorf("stats[2].HighestErrorCount != 0 (got %v)", stats[2].HighestErrorCount)
		}

		if stats[2].OldestRunAt.IsZero() {
			t.Errorf("stats[2].OldestRunAt.IsZero() != false (got %v)", stats[2].OldestRunAt.IsZero())
		}
	}()

	func() {
		j, err := c.LockJob("Q1")
		if err != nil {
			t.Fatal(err)
		}
		if j == nil {
			t.Fatal(err)
		}
		j.Error("???") //nolint:errcheck
		j.Done()

		stats, err = c.Stats()
		if err != nil {
			t.Fatal(err)
		}

		if len(stats) != 3 {
			t.Errorf("len(stats) != 3 (got %v)", len(stats))
		}

		if stats[0].Queue != "Q1" {
			t.Errorf("stats[0].Queue != \"Q1\" (got %v)", stats[0].Queue)
		}

		if stats[0].Type != "AnotherJob" {
			t.Errorf("stats[0].Type != \"AnotherJob\" (got %v)", stats[0].Type)
		}

		if stats[0].Count != 1 {
			t.Errorf("stats[0].Count != 1 (got %v)", stats[0].Count)
		}

		if stats[0].CountWorking != 0 {
			t.Errorf("stats[0].CountWorking != 0 (got %v)", stats[0].CountWorking)
		}

		if stats[0].CountErrored != 0 {
			t.Errorf("stats[0].CountErrored != 0 (got %v)", stats[0].CountErrored)
		}

		if stats[0].HighestErrorCount != 0 {
			t.Errorf("stats[0].HighestErrorCount != 0 (got %v)", stats[0].HighestErrorCount)
		}

		if stats[0].OldestRunAt.IsZero() {
			t.Errorf("stats[0].OldestRunAt.IsZero() != false (got %v)", stats[0].OldestRunAt.IsZero())
		}

		if stats[1].Queue != "Q1" {
			t.Errorf("stats[1].Queue != \"Q1\" (got %v)", stats[1].Queue)
		}

		if stats[1].Type != "MyJob" {
			t.Errorf("stats[1].Type != \"MyJob\" (got %v)", stats[1].Type)
		}

		if stats[1].Count != 1 {
			t.Errorf("stats[1].Count != 1 (got %v)", stats[1].Count)
		}

		if stats[1].CountWorking != 0 {
			t.Errorf("stats[1].CountWorking != 0 (got %v)", stats[1].CountWorking)
		}

		if stats[1].CountErrored != 1 {
			t.Errorf("stats[1].CountErrored != 1 (got %v)", stats[1].CountErrored)
		}

		if stats[1].HighestErrorCount != 1 {
			t.Errorf("stats[1].HighestErrorCount != 1 (got %v)", stats[1].HighestErrorCount)
		}

		if stats[0].OldestRunAt.IsZero() {
			t.Errorf("stats[0].OldestRunAt.IsZero() != false (got %v)", stats[0].OldestRunAt.IsZero())
		}

		if stats[2].Queue != "Q2" {
			t.Errorf("stats[2].Queue != \"Q2\" (got %v)", stats[2].Queue)
		}

		if stats[2].Type != "MyJob" {
			t.Errorf("stats[2].Type != \"MyJob\" (got %v)", stats[2].Type)
		}

		if stats[2].Count != 1 {
			t.Errorf("stats[2].Count != 1 (got %v)", stats[2].Count)
		}

		if stats[2].CountWorking != 0 {
			t.Errorf("stats[2].CountWorking != 0 (got %v)", stats[2].CountWorking)
		}

		if stats[2].CountErrored != 0 {
			t.Errorf("stats[2].CountErrored != 0 (got %v)", stats[2].CountErrored)
		}

		if stats[2].HighestErrorCount != 0 {
			t.Errorf("stats[2].HighestErrorCount != 0 (got %v)", stats[2].HighestErrorCount)
		}

		if stats[2].OldestRunAt.IsZero() {
			t.Errorf("stats[2].OldestRunAt.IsZero() != false (got %v)", stats[2].OldestRunAt.IsZero())
		}
	}()

}
