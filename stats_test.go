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

	if 1 != len(stats) {
		t.Errorf("1 != len(stats) (got %v)", len(stats))
	}

	if "Q1" != stats[0].Queue {
		t.Errorf("\"Q1\" != stats[0].Queue (got %v)", stats[0].Queue)
	}

	if "MyJob" != stats[0].Type {
		t.Errorf("\"MyJob\" != stats[0].Type (got %v)", stats[0].Type)
	}

	if 1 != stats[0].Count {
		t.Errorf("1 != stats[0].Count (got %v)", stats[0].Count)
	}

	if 0 != stats[0].CountWorking {
		t.Errorf("0 != stats[0].CountWorking (got %v)", stats[0].CountWorking)
	}

	if 0 != stats[0].CountErrored {
		t.Errorf("0 != stats[0].CountErrored (got %v)", stats[0].CountErrored)
	}

	if 0 != stats[0].HighestErrorCount {
		t.Errorf("0 != stats[0].HighestErrorCount (got %v)", stats[0].HighestErrorCount)
	}

	if stats[0].OldestRunAt.IsZero() {
		t.Errorf("false != stats[0].OldestRunAt.IsZero() (got %v)", stats[0].OldestRunAt.IsZero())
	}

	if err := c.Enqueue(&Job{Queue: "Q1", Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	stats, err = c.Stats()
	if err != nil {
		t.Fatal(err)
	}

	if 1 != len(stats) {
		t.Errorf("1 != len(stats) (got %v)", len(stats))
	}

	if "Q1" != stats[0].Queue {
		t.Errorf("\"Q1\" != stats[0].Queue (got %v)", stats[0].Queue)
	}

	if "MyJob" != stats[0].Type {
		t.Errorf("\"MyJob\" != stats[0].Type (got %v)", stats[0].Type)
	}

	if 2 != stats[0].Count {
		t.Errorf("1 != stats[0].Count (got %v)", stats[0].Count)
	}

	if 0 != stats[0].CountWorking {
		t.Errorf("0 != stats[0].CountWorking (got %v)", stats[0].CountWorking)
	}

	if 0 != stats[0].CountErrored {
		t.Errorf("0 != stats[0].CountErrored (got %v)", stats[0].CountErrored)
	}

	if 0 != stats[0].HighestErrorCount {
		t.Errorf("0 != stats[0].HighestErrorCount (got %v)", stats[0].HighestErrorCount)
	}

	if stats[0].OldestRunAt.IsZero() {
		t.Errorf("false != stats[0].OldestRunAt.IsZero() (got %v)", stats[0].OldestRunAt.IsZero())
	}

	if err := c.Enqueue(&Job{Queue: "Q2", Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	stats, err = c.Stats()
	if err != nil {
		t.Fatal(err)
	}

	if 2 != len(stats) {
		t.Errorf("1 != len(stats) (got %v)", len(stats))
	}

	if "Q1" != stats[0].Queue {
		t.Errorf("\"Q1\" != stats[0].Queue (got %v)", stats[0].Queue)
	}

	if "MyJob" != stats[0].Type {
		t.Errorf("\"MyJob\" != stats[0].Type (got %v)", stats[0].Type)
	}

	if 2 != stats[0].Count {
		t.Errorf("1 != stats[0].Count (got %v)", stats[0].Count)
	}

	if 0 != stats[0].CountWorking {
		t.Errorf("0 != stats[0].CountWorking (got %v)", stats[0].CountWorking)
	}

	if 0 != stats[0].CountErrored {
		t.Errorf("0 != stats[0].CountErrored (got %v)", stats[0].CountErrored)
	}

	if 0 != stats[0].HighestErrorCount {
		t.Errorf("0 != stats[0].HighestErrorCount (got %v)", stats[0].HighestErrorCount)
	}

	if stats[0].OldestRunAt.IsZero() {
		t.Errorf("false != stats[0].OldestRunAt.IsZero() (got %v)", stats[0].OldestRunAt.IsZero())
	}

	if "Q2" != stats[1].Queue {
		t.Errorf("\"Q2\" != stats[1].Queue (got %v)", stats[1].Queue)
	}

	if "MyJob" != stats[1].Type {
		t.Errorf("\"MyJob\" != stats[1].Type (got %v)", stats[1].Type)
	}

	if 1 != stats[1].Count {
		t.Errorf("1 != stats[1].Count (got %v)", stats[1].Count)
	}

	if 0 != stats[1].CountWorking {
		t.Errorf("0 != stats[1].CountWorking (got %v)", stats[1].CountWorking)
	}

	if 0 != stats[1].CountErrored {
		t.Errorf("0 != stats[1].CountErrored (got %v)", stats[1].CountErrored)
	}

	if 0 != stats[1].HighestErrorCount {
		t.Errorf("0 != stats[1].HighestErrorCount (got %v)", stats[1].HighestErrorCount)
	}

	if stats[1].OldestRunAt.IsZero() {
		t.Errorf("false != stats[1].OldestRunAt.IsZero() (got %v)", stats[1].OldestRunAt.IsZero())
	}

	if err := c.Enqueue(&Job{Queue: "Q1", Type: "AnotherJob"}); err != nil {
		t.Fatal(err)
	}

	stats, err = c.Stats()
	if err != nil {
		t.Fatal(err)
	}

	if 3 != len(stats) {
		t.Errorf("3 != len(stats) (got %v)", len(stats))
	}

	if "Q1" != stats[0].Queue {
		t.Errorf("\"Q1\" != stats[0].Queue (got %v)", stats[0].Queue)
	}

	if "AnotherJob" != stats[0].Type {
		t.Errorf("\"AnotherJob\" != stats[0].Type (got %v)", stats[0].Type)
	}

	if 1 != stats[0].Count {
		t.Errorf("1 != stats[0].Count (got %v)", stats[0].Count)
	}

	if 0 != stats[0].CountWorking {
		t.Errorf("0 != stats[0].CountWorking (got %v)", stats[0].CountWorking)
	}

	if 0 != stats[0].CountErrored {
		t.Errorf("0 != stats[0].CountErrored (got %v)", stats[0].CountErrored)
	}

	if 0 != stats[0].HighestErrorCount {
		t.Errorf("0 != stats[0].HighestErrorCount (got %v)", stats[0].HighestErrorCount)
	}

	if stats[0].OldestRunAt.IsZero() {
		t.Errorf("false != stats[0].OldestRunAt.IsZero() (got %v)", stats[0].OldestRunAt.IsZero())
	}

	if "Q1" != stats[1].Queue {
		t.Errorf("\"Q1\" != stats[1].Queue (got %v)", stats[1].Queue)
	}

	if "MyJob" != stats[1].Type {
		t.Errorf("\"MyJob\" != stats[1].Type (got %v)", stats[1].Type)
	}

	if 2 != stats[1].Count {
		t.Errorf("1 != stats[1].Count (got %v)", stats[1].Count)
	}

	if 0 != stats[1].CountWorking {
		t.Errorf("0 != stats[1].CountWorking (got %v)", stats[1].CountWorking)
	}

	if 0 != stats[1].CountErrored {
		t.Errorf("0 != stats[1].CountErrored (got %v)", stats[1].CountErrored)
	}

	if 0 != stats[1].HighestErrorCount {
		t.Errorf("0 != stats[1].HighestErrorCount (got %v)", stats[1].HighestErrorCount)
	}

	if stats[0].OldestRunAt.IsZero() {
		t.Errorf("false != stats[0].OldestRunAt.IsZero() (got %v)", stats[0].OldestRunAt.IsZero())
	}

	if "Q2" != stats[2].Queue {
		t.Errorf("\"Q2\" != stats[2].Queue (got %v)", stats[2].Queue)
	}

	if "MyJob" != stats[2].Type {
		t.Errorf("\"MyJob\" != stats[2].Type (got %v)", stats[2].Type)
	}

	if 1 != stats[2].Count {
		t.Errorf("1 != stats[2].Count (got %v)", stats[2].Count)
	}

	if 0 != stats[2].CountWorking {
		t.Errorf("0 != stats[2].CountWorking (got %v)", stats[2].CountWorking)
	}

	if 0 != stats[2].CountErrored {
		t.Errorf("0 != stats[2].CountErrored (got %v)", stats[2].CountErrored)
	}

	if 0 != stats[2].HighestErrorCount {
		t.Errorf("0 != stats[2].HighestErrorCount (got %v)", stats[2].HighestErrorCount)
	}

	if stats[2].OldestRunAt.IsZero() {
		t.Errorf("false != stats[2].OldestRunAt.IsZero() (got %v)", stats[2].OldestRunAt.IsZero())
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

		if 3 != len(stats) {
			t.Errorf("3 != len(stats) (got %v)", len(stats))
		}

		if "Q1" != stats[0].Queue {
			t.Errorf("\"Q1\" != stats[0].Queue (got %v)", stats[0].Queue)
		}

		if "AnotherJob" != stats[0].Type {
			t.Errorf("\"AnotherJob\" != stats[0].Type (got %v)", stats[0].Type)
		}

		if 1 != stats[0].Count {
			t.Errorf("1 != stats[0].Count (got %v)", stats[0].Count)
		}

		if 0 != stats[0].CountWorking {
			t.Errorf("0 != stats[0].CountWorking (got %v)", stats[0].CountWorking)
		}

		if 0 != stats[0].CountErrored {
			t.Errorf("0 != stats[0].CountErrored (got %v)", stats[0].CountErrored)
		}

		if 0 != stats[0].HighestErrorCount {
			t.Errorf("0 != stats[0].HighestErrorCount (got %v)", stats[0].HighestErrorCount)
		}

		if stats[0].OldestRunAt.IsZero() {
			t.Errorf("false != stats[0].OldestRunAt.IsZero() (got %v)", stats[0].OldestRunAt.IsZero())
		}

		if "Q1" != stats[1].Queue {
			t.Errorf("\"Q1\" != stats[1].Queue (got %v)", stats[1].Queue)
		}

		if "MyJob" != stats[1].Type {
			t.Errorf("\"MyJob\" != stats[1].Type (got %v)", stats[1].Type)
		}

		if 2 != stats[1].Count {
			t.Errorf("1 != stats[1].Count (got %v)", stats[1].Count)
		}

		if 1 != stats[1].CountWorking {
			t.Errorf("1 != stats[1].CountWorking (got %v)", stats[1].CountWorking)
		}

		if 0 != stats[1].CountErrored {
			t.Errorf("0 != stats[1].CountErrored (got %v)", stats[1].CountErrored)
		}

		if 0 != stats[1].HighestErrorCount {
			t.Errorf("0 != stats[1].HighestErrorCount (got %v)", stats[1].HighestErrorCount)
		}

		if stats[0].OldestRunAt.IsZero() {
			t.Errorf("false != stats[0].OldestRunAt.IsZero() (got %v)", stats[0].OldestRunAt.IsZero())
		}

		if "Q2" != stats[2].Queue {
			t.Errorf("\"Q2\" != stats[2].Queue (got %v)", stats[2].Queue)
		}

		if "MyJob" != stats[2].Type {
			t.Errorf("\"MyJob\" != stats[2].Type (got %v)", stats[2].Type)
		}

		if 1 != stats[2].Count {
			t.Errorf("1 != stats[2].Count (got %v)", stats[2].Count)
		}

		if 0 != stats[2].CountWorking {
			t.Errorf("0 != stats[2].CountWorking (got %v)", stats[2].CountWorking)
		}

		if 0 != stats[2].CountErrored {
			t.Errorf("0 != stats[2].CountErrored (got %v)", stats[2].CountErrored)
		}

		if 0 != stats[2].HighestErrorCount {
			t.Errorf("0 != stats[2].HighestErrorCount (got %v)", stats[2].HighestErrorCount)
		}

		if stats[2].OldestRunAt.IsZero() {
			t.Errorf("false != stats[2].OldestRunAt.IsZero() (got %v)", stats[2].OldestRunAt.IsZero())
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
		j.Delete()

		stats, err = c.Stats()
		if err != nil {
			t.Fatal(err)
		}

		if 3 != len(stats) {
			t.Errorf("3 != len(stats) (got %v)", len(stats))
		}

		if "Q1" != stats[0].Queue {
			t.Errorf("\"Q1\" != stats[0].Queue (got %v)", stats[0].Queue)
		}

		if "AnotherJob" != stats[0].Type {
			t.Errorf("\"AnotherJob\" != stats[0].Type (got %v)", stats[0].Type)
		}

		if 1 != stats[0].Count {
			t.Errorf("1 != stats[0].Count (got %v)", stats[0].Count)
		}

		if 0 != stats[0].CountWorking {
			t.Errorf("0 != stats[0].CountWorking (got %v)", stats[0].CountWorking)
		}

		if 0 != stats[0].CountErrored {
			t.Errorf("0 != stats[0].CountErrored (got %v)", stats[0].CountErrored)
		}

		if 0 != stats[0].HighestErrorCount {
			t.Errorf("0 != stats[0].HighestErrorCount (got %v)", stats[0].HighestErrorCount)
		}

		if stats[0].OldestRunAt.IsZero() {
			t.Errorf("false != stats[0].OldestRunAt.IsZero() (got %v)", stats[0].OldestRunAt.IsZero())
		}

		if "Q1" != stats[1].Queue {
			t.Errorf("\"Q1\" != stats[1].Queue (got %v)", stats[1].Queue)
		}

		if "MyJob" != stats[1].Type {
			t.Errorf("\"MyJob\" != stats[1].Type (got %v)", stats[1].Type)
		}

		if 1 != stats[1].Count {
			t.Errorf("1 != stats[1].Count (got %v)", stats[1].Count)
		}

		if 0 != stats[1].CountWorking {
			t.Errorf("0 != stats[1].CountWorking (got %v)", stats[1].CountWorking)
		}

		if 0 != stats[1].CountErrored {
			t.Errorf("0 != stats[1].CountErrored (got %v)", stats[1].CountErrored)
		}

		if 0 != stats[1].HighestErrorCount {
			t.Errorf("0 != stats[1].HighestErrorCount (got %v)", stats[1].HighestErrorCount)
		}

		if stats[0].OldestRunAt.IsZero() {
			t.Errorf("false != stats[0].OldestRunAt.IsZero() (got %v)", stats[0].OldestRunAt.IsZero())
		}

		if "Q2" != stats[2].Queue {
			t.Errorf("\"Q2\" != stats[2].Queue (got %v)", stats[2].Queue)
		}

		if "MyJob" != stats[2].Type {
			t.Errorf("\"MyJob\" != stats[2].Type (got %v)", stats[2].Type)
		}

		if 1 != stats[2].Count {
			t.Errorf("1 != stats[2].Count (got %v)", stats[2].Count)
		}

		if 0 != stats[2].CountWorking {
			t.Errorf("0 != stats[2].CountWorking (got %v)", stats[2].CountWorking)
		}

		if 0 != stats[2].CountErrored {
			t.Errorf("0 != stats[2].CountErrored (got %v)", stats[2].CountErrored)
		}

		if 0 != stats[2].HighestErrorCount {
			t.Errorf("0 != stats[2].HighestErrorCount (got %v)", stats[2].HighestErrorCount)
		}

		if stats[2].OldestRunAt.IsZero() {
			t.Errorf("false != stats[2].OldestRunAt.IsZero() (got %v)", stats[2].OldestRunAt.IsZero())
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
		j.Error("???")
		j.Done()

		stats, err = c.Stats()
		if err != nil {
			t.Fatal(err)
		}

		if 3 != len(stats) {
			t.Errorf("3 != len(stats) (got %v)", len(stats))
		}

		if "Q1" != stats[0].Queue {
			t.Errorf("\"Q1\" != stats[0].Queue (got %v)", stats[0].Queue)
		}

		if "AnotherJob" != stats[0].Type {
			t.Errorf("\"AnotherJob\" != stats[0].Type (got %v)", stats[0].Type)
		}

		if 1 != stats[0].Count {
			t.Errorf("1 != stats[0].Count (got %v)", stats[0].Count)
		}

		if 0 != stats[0].CountWorking {
			t.Errorf("0 != stats[0].CountWorking (got %v)", stats[0].CountWorking)
		}

		if 0 != stats[0].CountErrored {
			t.Errorf("0 != stats[0].CountErrored (got %v)", stats[0].CountErrored)
		}

		if 0 != stats[0].HighestErrorCount {
			t.Errorf("0 != stats[0].HighestErrorCount (got %v)", stats[0].HighestErrorCount)
		}

		if stats[0].OldestRunAt.IsZero() {
			t.Errorf("false != stats[0].OldestRunAt.IsZero() (got %v)", stats[0].OldestRunAt.IsZero())
		}

		if "Q1" != stats[1].Queue {
			t.Errorf("\"Q1\" != stats[1].Queue (got %v)", stats[1].Queue)
		}

		if "MyJob" != stats[1].Type {
			t.Errorf("\"MyJob\" != stats[1].Type (got %v)", stats[1].Type)
		}

		if 1 != stats[1].Count {
			t.Errorf("1 != stats[1].Count (got %v)", stats[1].Count)
		}

		if 0 != stats[1].CountWorking {
			t.Errorf("0 != stats[1].CountWorking (got %v)", stats[1].CountWorking)
		}

		if 1 != stats[1].CountErrored {
			t.Errorf("1 != stats[1].CountErrored (got %v)", stats[1].CountErrored)
		}

		if 1 != stats[1].HighestErrorCount {
			t.Errorf("1 != stats[1].HighestErrorCount (got %v)", stats[1].HighestErrorCount)
		}

		if stats[0].OldestRunAt.IsZero() {
			t.Errorf("false != stats[0].OldestRunAt.IsZero() (got %v)", stats[0].OldestRunAt.IsZero())
		}

		if "Q2" != stats[2].Queue {
			t.Errorf("\"Q2\" != stats[2].Queue (got %v)", stats[2].Queue)
		}

		if "MyJob" != stats[2].Type {
			t.Errorf("\"MyJob\" != stats[2].Type (got %v)", stats[2].Type)
		}

		if 1 != stats[2].Count {
			t.Errorf("1 != stats[2].Count (got %v)", stats[2].Count)
		}

		if 0 != stats[2].CountWorking {
			t.Errorf("0 != stats[2].CountWorking (got %v)", stats[2].CountWorking)
		}

		if 0 != stats[2].CountErrored {
			t.Errorf("0 != stats[2].CountErrored (got %v)", stats[2].CountErrored)
		}

		if 0 != stats[2].HighestErrorCount {
			t.Errorf("0 != stats[2].HighestErrorCount (got %v)", stats[2].HighestErrorCount)
		}

		if stats[2].OldestRunAt.IsZero() {
			t.Errorf("false != stats[2].OldestRunAt.IsZero() (got %v)", stats[2].OldestRunAt.IsZero())
		}
	}()

}
