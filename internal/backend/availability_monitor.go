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
	StateDisabled  State = "DISABLED"
	StateWaiting   State = "WAITING"
	StateUsable    State = "USABLE"
	StateNotReady  State = "NOT_READY"
	StateResetting State = "RESETTING"
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
	ProcessInstanceID string
	Ready             bool
}

type (
	Probe        func(context.Context) (ProbeResult, error)
	ResetHandler func(context.Context, string)
)

type Snapshot struct {
	State               State
	LastSuccessfulProbe time.Time
	LastFailure         time.Time
	FailureCategory     string
	ProcessInstanceID   string
	ConsecutiveFailures int
}

type availabilityMonitorOptions struct {
	failureDelays  []time.Duration
	successDelay   time.Duration
	jitterFraction float64
	random         func() float64
	now            func() time.Time
	wait           func(context.Context, time.Duration) bool
}

// AvailabilityMonitor owns one backend's process generation and admission
// fence. A new process never inherits resources created by its predecessor.
type AvailabilityMonitor struct {
	probe   Probe
	onReset ResetHandler

	mu                sync.Mutex
	cond              *sync.Cond
	snapshot          Snapshot
	admissionOpen     bool
	inFlight          int
	transportFailures int
	wake              chan struct{}
	options           availabilityMonitorOptions
	waitFn            func(context.Context, time.Duration) bool
}

// GenerationLease fences one admitted request to the process generation that
// was usable when the request started.
type GenerationLease struct {
	monitor    *AvailabilityMonitor
	generation string
	once       sync.Once
}

func (l *GenerationLease) Generation() string {
	if l == nil {
		return ""
	}
	return l.generation
}

func (l *GenerationLease) Release() {
	if l == nil || l.monitor == nil {
		return
	}
	l.once.Do(func() {
		l.monitor.mu.Lock()
		l.monitor.inFlight--
		l.monitor.cond.Broadcast()
		l.monitor.mu.Unlock()
	})
}

// NewAvailabilityMonitor creates an app-owned backend availability worker.
func NewAvailabilityMonitor(probe Probe, reset ...ResetHandler) *AvailabilityMonitor {
	var handler ResetHandler
	if len(reset) > 0 {
		handler = reset[0]
	}
	return newAvailabilityMonitor(probe, handler, availabilityMonitorOptions{})
}

func newAvailabilityMonitor(
	probe Probe,
	onReset ResetHandler,
	options availabilityMonitorOptions,
) *AvailabilityMonitor {
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
		onReset:  onReset,
		snapshot: Snapshot{State: StateWaiting},
		wake:     make(chan struct{}, 1),
		options:  options,
	}
	monitor.cond = sync.NewCond(&monitor.mu)
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
	for ctx.Err() == nil {
		result, err := m.probe(ctx)
		if ctx.Err() != nil {
			return
		}

		resetGeneration, delay := m.observeProbe(result, err, failureIndex)
		if err != nil && failureIndex < len(m.options.failureDelays)-1 {
			failureIndex++
		} else if err == nil {
			failureIndex = 0
		}
		if resetGeneration != "" {
			m.reset(ctx, resetGeneration, result.ProcessInstanceID)
		}
		if !m.waitFn(ctx, m.jitter(delay)) {
			return
		}
	}
}

func (m *AvailabilityMonitor) observeProbe(
	result ProbeResult,
	err error,
	failureIndex int,
) (string, time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.options.now()
	oldGeneration := m.snapshot.ProcessInstanceID
	if result.ProcessInstanceID != "" && oldGeneration != "" && result.ProcessInstanceID != oldGeneration {
		m.snapshot.State = StateResetting
		m.admissionOpen = false
		return oldGeneration, m.options.failureDelays[0]
	}
	if err == nil && result.Ready {
		m.snapshot.State = StateUsable
		m.snapshot.LastSuccessfulProbe = now
		m.snapshot.FailureCategory = ""
		m.snapshot.ConsecutiveFailures = 0
		m.snapshot.ProcessInstanceID = result.ProcessInstanceID
		m.transportFailures = 0
		m.admissionOpen = true
		return "", m.options.successDelay
	}

	m.snapshot.LastFailure = now
	m.snapshot.FailureCategory = failureCategory(err)
	m.snapshot.ConsecutiveFailures++
	if result.ProcessInstanceID != "" {
		m.snapshot.ProcessInstanceID = result.ProcessInstanceID
		m.snapshot.State = StateNotReady
		m.transportFailures = 0
		m.admissionOpen = false
		return "", m.options.failureDelays[failureIndex]
	}
	if oldGeneration == "" {
		m.snapshot.State = StateWaiting
		m.admissionOpen = false
		return "", m.options.failureDelays[failureIndex]
	}
	m.transportFailures++
	if m.transportFailures >= 2 {
		m.snapshot.State = StateResetting
		m.admissionOpen = false
		return oldGeneration, m.options.failureDelays[failureIndex]
	}
	m.snapshot.State = StateNotReady
	m.admissionOpen = false
	return "", m.options.failureDelays[failureIndex]
}

func (m *AvailabilityMonitor) reset(ctx context.Context, oldGeneration, candidateGeneration string) {
	m.mu.Lock()
	for m.inFlight > 0 {
		m.cond.Wait()
	}
	m.mu.Unlock()

	if m.onReset != nil {
		m.onReset(ctx, oldGeneration)
	}

	m.mu.Lock()
	m.snapshot.State = StateWaiting
	m.snapshot.ProcessInstanceID = candidateGeneration
	m.snapshot.ConsecutiveFailures = 0
	m.snapshot.FailureCategory = ""
	m.transportFailures = 0
	m.admissionOpen = false
	m.mu.Unlock()
}

func (m *AvailabilityMonitor) Snapshot() Snapshot {
	if m == nil {
		return Snapshot{State: StateDisabled, FailureCategory: "disabled"}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshot
}

func (m *AvailabilityMonitor) Usable() bool {
	return m.Snapshot().State == StateUsable
}

func (m *AvailabilityMonitor) Acquire() (*GenerationLease, bool) {
	if m == nil {
		return nil, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.admissionOpen || m.snapshot.State != StateUsable || m.snapshot.ProcessInstanceID == "" {
		return nil, false
	}
	m.inFlight++
	return &GenerationLease{monitor: m, generation: m.snapshot.ProcessInstanceID}, true
}

func (m *AvailabilityMonitor) MarkUnavailable(category string) {
	if m == nil {
		return
	}
	if category == "" {
		category = "operation_failure"
	}
	m.mu.Lock()
	m.snapshot.State = StateNotReady
	m.snapshot.LastFailure = m.options.now()
	m.snapshot.FailureCategory = category
	m.snapshot.ConsecutiveFailures++
	m.admissionOpen = false
	m.mu.Unlock()
	m.Refresh()
}

func (m *AvailabilityMonitor) Refresh() {
	if m == nil {
		return
	}
	select {
	case m.wake <- struct{}{}:
	default:
	}
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
	case err == nil:
		return "not_ready"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "probe_failure"
	}
}
