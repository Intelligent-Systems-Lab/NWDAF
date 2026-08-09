package backend

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

const processAID = "process-a"

func testMonitor(probe Probe, reset ResetHandler) *AvailabilityMonitor {
	return newAvailabilityMonitor(probe, reset, availabilityMonitorOptions{
		failureDelays:  []time.Duration{time.Millisecond, time.Millisecond},
		successDelay:   time.Millisecond,
		jitterFraction: -1,
	})
}

func TestAvailabilityMonitorBecomesUsableAndIssuesGenerationLease(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	monitor := testMonitor(func(context.Context) (ProbeResult, error) {
		return ProbeResult{ProcessInstanceID: processAID, Ready: true}, nil
	}, nil)
	go monitor.Run(ctx)
	waitForState(t, monitor, StateUsable)
	lease, ok := monitor.Acquire()
	if !ok || lease.Generation() != processAID {
		t.Fatalf("lease = %#v ok=%v", lease, ok)
	}
	lease.Release()
}

func TestNotReadyWithSameProcessDoesNotReset(t *testing.T) {
	var calls atomic.Int32
	var resets atomic.Int32
	monitor := testMonitor(func(context.Context) (ProbeResult, error) {
		if calls.Add(1) == 1 {
			return ProbeResult{ProcessInstanceID: processAID, Ready: true}, nil
		}
		return ProbeResult{ProcessInstanceID: processAID}, errors.New("503")
	}, func(context.Context, string) { resets.Add(1) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go monitor.Run(ctx)
	waitForState(t, monitor, StateNotReady)
	if resets.Load() != 0 {
		t.Fatalf("reset count = %d", resets.Load())
	}
}

func TestChangedProcessDrainsLeaseBeforeReset(t *testing.T) {
	var calls atomic.Int32
	resetStarted := make(chan string, 1)
	secondProbe := make(chan struct{})
	monitor := testMonitor(func(context.Context) (ProbeResult, error) {
		if calls.Add(1) == 1 {
			return ProbeResult{ProcessInstanceID: processAID, Ready: true}, nil
		}
		<-secondProbe
		return ProbeResult{ProcessInstanceID: "process-b", Ready: true}, nil
	}, func(_ context.Context, old string) { resetStarted <- old })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go monitor.Run(ctx)
	waitForState(t, monitor, StateUsable)
	lease, ok := monitor.Acquire()
	if !ok {
		t.Fatal("could not acquire generation lease")
	}
	close(secondProbe)
	monitor.Refresh()
	waitForState(t, monitor, StateResetting)
	select {
	case <-resetStarted:
		t.Fatal("reset ran before the admitted request drained")
	case <-time.After(5 * time.Millisecond):
	}
	lease.Release()
	select {
	case old := <-resetStarted:
		if old != processAID {
			t.Fatalf("old generation = %q", old)
		}
	case <-time.After(time.Second):
		t.Fatal("reset did not run after lease release")
	}
}

func TestTwoTransportFailuresResetCurrentGeneration(t *testing.T) {
	var calls atomic.Int32
	reset := make(chan string, 1)
	monitor := testMonitor(func(context.Context) (ProbeResult, error) {
		if calls.Add(1) == 1 {
			return ProbeResult{ProcessInstanceID: processAID, Ready: true}, nil
		}
		return ProbeResult{}, errors.New("transport")
	}, func(_ context.Context, old string) { reset <- old })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go monitor.Run(ctx)
	select {
	case old := <-reset:
		if old != processAID {
			t.Fatalf("old generation = %q", old)
		}
	case <-time.After(time.Second):
		t.Fatal("two transport failures did not reset the generation")
	}
}

func TestNotReadyResponseDoesNotCountAsTransportFailure(t *testing.T) {
	var calls atomic.Int32
	var resets atomic.Int32
	secondTransportObserved := make(chan struct{})
	monitor := testMonitor(func(context.Context) (ProbeResult, error) {
		switch calls.Add(1) {
		case 1:
			return ProbeResult{ProcessInstanceID: processAID, Ready: true}, nil
		case 2:
			return ProbeResult{ProcessInstanceID: processAID}, errors.New("503")
		case 3:
			close(secondTransportObserved)
			return ProbeResult{}, errors.New("transport")
		default:
			return ProbeResult{ProcessInstanceID: processAID, Ready: true}, nil
		}
	}, func(context.Context, string) { resets.Add(1) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go monitor.Run(ctx)
	select {
	case <-secondTransportObserved:
	case <-time.After(time.Second):
		t.Fatal("transport probe was not observed")
	}
	waitForState(t, monitor, StateUsable)
	if resets.Load() != 0 {
		t.Fatalf("reset count = %d", resets.Load())
	}
}

func waitForState(t *testing.T, monitor *AvailabilityMonitor, want State) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if monitor.Snapshot().State == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("state = %s, want %s", monitor.Snapshot().State, want)
}
