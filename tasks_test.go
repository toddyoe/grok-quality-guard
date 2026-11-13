package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunBoundedQueueLimitsWorkers(t *testing.T) {
	values := make([]int, 12)
	entered := make(chan struct{}, len(values))
	release := make(chan struct{})
	var active atomic.Int32
	var maxActive atomic.Int32

	done := make(chan error, 1)
	go func() {
		done <- runBoundedQueue(context.Background(), values, 3, 4, func(context.Context, int) {
			current := active.Add(1)
			for {
				observed := maxActive.Load()
				if current <= observed || maxActive.CompareAndSwap(observed, current) {
					break
				}
			}
			entered <- struct{}{}
			<-release
			active.Add(-1)
		})
	}()

	for index := 0; index < 4; index++ {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("bounded queue did not start the configured workers")
		}
	}
	select {
	case <-entered:
		t.Fatal("bounded queue started more workers than configured")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	select {
	case errRun := <-done:
		if errRun != nil {
			t.Fatal(errRun)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("bounded queue did not finish")
	}
	if got := maxActive.Load(); got != 4 {
		t.Fatalf("max active workers = %d, want 4", got)
	}
}
