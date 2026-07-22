package backend

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestAvailabilityMonitorProbesImmediatelyAndUsesBoundedBackoff(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	attempts := 0
	delays := make(chan time.Duration, 4)
	monitor := newAvailabilityMonitor(
		func(context.Context) (ProbeResult, error) {
			mu.Lock()
			defer mu.Unlock()
			attempts++
			if attempts < 4 {
				return ProbeResult{}, errors.New("unavailable")
			}
			return ProbeResult{ProcessInstanceID: "process-a", Selection: "mongodb"}, nil
		},
		availabilityMonitorOptions{
			failureDelays:  []time.Duration{time.Second, 2 * time.Second, 5 * time.Second},
			successDelay:   30 * time.Second,
			jitterFraction: -1,
			wait: func(ctx context.Context, delay time.Duration) bool {
				delays <- delay
				return ctx.Err() == nil
			},
		},
	)
	done := make(chan struct{})
	go func() {
		monitor.Run(ctx)
		close(done)
	}()

	for _, want := range []time.Duration{time.Second, 2 * time.Second, 5 * time.Second, 30 * time.Second} {
		if got := <-delays; got != want {
			t.Fatalf("delay = %s, want %s", got, want)
		}
		if want == 30*time.Second {
			cancel()
		}
	}
	<-done
	snapshot := monitor.Snapshot()
	if snapshot.State != StateUsable || snapshot.ProcessInstanceID != "process-a" ||
		snapshot.Selection != "mongodb" {
		t.Fatalf("snapshot = %+v", snapshot)
	}
}

func TestAvailabilityMonitorSuccessResetsFailureBackoff(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results := []error{errors.New("first"), nil, errors.New("again")}
	delays := make(chan time.Duration, 3)
	monitor := newAvailabilityMonitor(
		func(context.Context) (ProbeResult, error) {
			err := results[0]
			results = results[1:]
			return ProbeResult{}, err
		},
		availabilityMonitorOptions{
			failureDelays:  []time.Duration{time.Second, 2 * time.Second},
			successDelay:   30 * time.Second,
			jitterFraction: -1,
			wait: func(ctx context.Context, delay time.Duration) bool {
				delays <- delay
				if len(results) == 0 {
					cancel()
				}
				return ctx.Err() == nil
			},
		},
	)
	monitor.Run(ctx)

	for index, want := range []time.Duration{time.Second, 30 * time.Second, time.Second} {
		if got := <-delays; got != want {
			t.Fatalf("delay[%d] = %s, want %s", index, got, want)
		}
	}
}

func TestAvailabilityMonitorJitterBoundaries(t *testing.T) {
	t.Parallel()

	monitor := newAvailabilityMonitor(
		func(context.Context) (ProbeResult, error) { return ProbeResult{}, nil },
		availabilityMonitorOptions{
			jitterFraction: 0.2,
			random:         func() float64 { return 0 },
		},
	)
	if got := monitor.jitter(10 * time.Second); got != 8*time.Second {
		t.Fatalf("minimum jitter = %s", got)
	}
	monitor.options.random = func() float64 { return 1 }
	if got := monitor.jitter(10 * time.Second); got != 12*time.Second {
		t.Fatalf("maximum jitter = %s", got)
	}
}

func TestAvailabilityMonitorCancellationInterruptsWait(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	monitor := NewAvailabilityMonitor(func(context.Context) (ProbeResult, error) {
		close(started)
		return ProbeResult{}, errors.New("down")
	})
	done := make(chan struct{})
	go func() {
		monitor.Run(ctx)
		close(done)
	}()
	<-started
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("monitor did not stop after cancellation")
	}
}

func TestAvailabilityMonitorCanExposeSyncProgress(t *testing.T) {
	t.Parallel()

	monitor := NewAvailabilityMonitor(func(context.Context) (ProbeResult, error) { return ProbeResult{}, nil })
	monitor.MarkSyncing("process-a")
	if snapshot := monitor.Snapshot(); snapshot.State != StateSyncing {
		t.Fatalf("snapshot state = %q, want %q", snapshot.State, StateSyncing)
	}
}

func TestAvailabilityMonitorDoesNotFlapDuringUsableRefresh(t *testing.T) {
	t.Parallel()

	monitor := NewAvailabilityMonitor(func(context.Context) (ProbeResult, error) {
		return ProbeResult{ProcessInstanceID: "process-a"}, nil
	})
	monitor.setUsable("process-a", "")
	monitor.MarkSyncing("process-a")
	if snapshot := monitor.Snapshot(); snapshot.State != StateUsable {
		t.Fatalf("snapshot state = %q, want %q", snapshot.State, StateUsable)
	}
	monitor.MarkSyncing("process-b")
	if snapshot := monitor.Snapshot(); snapshot.State != StateSyncing {
		t.Fatalf("snapshot state after restart = %q, want %q", snapshot.State, StateSyncing)
	}
}

func TestAvailabilityMonitorSnapshotsAreSafeDuringConcurrentUpdates(t *testing.T) {
	t.Parallel()

	monitor := NewAvailabilityMonitor(func(context.Context) (ProbeResult, error) { return ProbeResult{}, nil })
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for iteration := 0; iteration < 100; iteration++ {
				if worker%2 == 0 {
					monitor.MarkUnavailable("transport")
				} else {
					_ = monitor.Snapshot()
				}
			}
		}(worker)
	}
	wg.Wait()
}
