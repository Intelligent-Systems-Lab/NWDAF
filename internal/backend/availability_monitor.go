// Package backend tracks private backend process availability.
package backend

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"
)

type State string

const (
	StateUnknown     State = "UNKNOWN"
	StatePolling     State = "POLLING"
	StateHandshaking State = "HANDSHAKING"
	StateUnavailable State = "UNAVAILABLE"
	StateUsable      State = "USABLE"
)

var defaultFailureDelays = []time.Duration{
	time.Second,
	2 * time.Second,
	5 * time.Second,
	10 * time.Second,
	30 * time.Second,
}

const (
	defaultSuccessInterval = 30 * time.Second
	defaultJitterFraction  = 0.2
)

type ProbeResult struct {
	Selection string
}

type Probe func(context.Context) (ProbeResult, error)

type Snapshot struct {
	State               State
	LastSuccessfulProbe time.Time
	LastFailure         time.Time
	FailureCategory     string
	Selection           string
}

type availabilityMonitorOptions struct {
	failureDelays  []time.Duration
	successDelay   time.Duration
	jitterFraction float64
	random         func() float64
	now            func() time.Time
	wait           func(context.Context, time.Duration) bool
}

// AvailabilityMonitor tracks whether a private backend can currently serve requests.
type AvailabilityMonitor struct {
	probe Probe

	mu       sync.RWMutex
	snapshot Snapshot
	wake     chan struct{}
	options  availabilityMonitorOptions
	waitFn   func(context.Context, time.Duration) bool
}

// NewAvailabilityMonitor creates an app-owned backend availability worker.
func NewAvailabilityMonitor(probe Probe) *AvailabilityMonitor {
	return newAvailabilityMonitor(probe, availabilityMonitorOptions{})
}

func newAvailabilityMonitor(probe Probe, options availabilityMonitorOptions) *AvailabilityMonitor {
	if len(options.failureDelays) == 0 {
		options.failureDelays = append([]time.Duration(nil), defaultFailureDelays...)
	}
	if options.successDelay <= 0 {
		options.successDelay = defaultSuccessInterval
	}
	if options.jitterFraction < 0 {
		options.jitterFraction = 0
	} else if options.jitterFraction == 0 {
		options.jitterFraction = defaultJitterFraction
	}
	if options.random == nil {
		options.random = rand.Float64
	}
	if options.now == nil {
		options.now = time.Now
	}
	monitor := &AvailabilityMonitor{
		probe:    probe,
		snapshot: Snapshot{State: StateUnknown},
		wake:     make(chan struct{}, 1),
		options:  options,
	}
	monitor.waitFn = options.wait
	if monitor.waitFn == nil {
		monitor.waitFn = monitor.wait
	}
	return monitor
}

func (m *AvailabilityMonitor) Run(ctx context.Context) {
	if m == nil || m.probe == nil || ctx == nil {
		return
	}
	failureIndex := 0
	for {
		if ctx.Err() != nil {
			return
		}
		m.setPolling()
		result, err := m.probe(ctx)
		if ctx.Err() != nil {
			return
		}

		var delay time.Duration
		if err != nil {
			m.setUnavailable(failureCategory(err))
			delay = m.options.failureDelays[failureIndex]
			if failureIndex < len(m.options.failureDelays)-1 {
				failureIndex++
			}
		} else {
			m.setUsable(result.Selection)
			failureIndex = 0
			delay = m.options.successDelay
		}
		if !m.waitFn(ctx, m.jitter(delay)) {
			return
		}
	}
}

func (m *AvailabilityMonitor) Snapshot() Snapshot {
	if m == nil {
		return Snapshot{State: StateUnavailable, FailureCategory: "disabled"}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.snapshot
}

func (m *AvailabilityMonitor) Usable() bool {
	return m.Snapshot().State == StateUsable
}

func (m *AvailabilityMonitor) MarkUnavailable(category string) {
	if m == nil {
		return
	}
	if category == "" {
		category = "operation_failure"
	}
	m.setUnavailable(category)
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func (m *AvailabilityMonitor) MarkHandshaking() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.snapshot.State = StateHandshaking
	m.mu.Unlock()
}

func (m *AvailabilityMonitor) setPolling() {
	m.mu.Lock()
	m.snapshot.State = StatePolling
	m.mu.Unlock()
}

func (m *AvailabilityMonitor) setUnavailable(category string) {
	m.mu.Lock()
	m.snapshot.State = StateUnavailable
	m.snapshot.LastFailure = m.options.now()
	m.snapshot.FailureCategory = category
	m.snapshot.Selection = ""
	m.mu.Unlock()
}

func (m *AvailabilityMonitor) setUsable(selection string) {
	m.mu.Lock()
	m.snapshot.State = StateUsable
	m.snapshot.LastSuccessfulProbe = m.options.now()
	m.snapshot.FailureCategory = ""
	m.snapshot.Selection = selection
	m.mu.Unlock()
}

func (m *AvailabilityMonitor) wait(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-m.wake:
		return true
	case <-timer.C:
		return true
	}
}

func (m *AvailabilityMonitor) jitter(delay time.Duration) time.Duration {
	if delay <= 0 || m.options.jitterFraction <= 0 {
		return delay
	}
	factor := 1 - m.options.jitterFraction + 2*m.options.jitterFraction*m.options.random()
	return time.Duration(float64(delay) * factor)
}

func failureCategory(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "probe_failure"
	}
}
