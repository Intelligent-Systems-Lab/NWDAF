package mtlf

import (
	"math"
	"sync"
	"time"
)

type ringBuffer struct {
	values []float64
	next   int
}

func newRingBuffer(size int) *ringBuffer {
	if size <= 0 {
		size = 1
	}
	return &ringBuffer{values: make([]float64, 0, size)}
}

func (r *ringBuffer) Add(v float64) {
	if len(r.values) < cap(r.values) {
		r.values = append(r.values, v)
		return
	}
	r.values[r.next] = v
	r.next = (r.next + 1) % cap(r.values)
}

func (r *ringBuffer) Count() int {
	return len(r.values)
}

func (r *ringBuffer) Snapshot() []float64 {
	if len(r.values) == 0 {
		return nil
	}
	out := make([]float64, 0, len(r.values))
	if len(r.values) < cap(r.values) || r.next == 0 {
		return append(out, r.values...)
	}
	out = append(out, r.values[r.next:]...)
	out = append(out, r.values[:r.next]...)
	return out
}

type ScopeState struct {
	scopeKey      string
	bufferSize    int
	metricBuffers map[string]*ringBuffer
	breachCount   int
	lastUpdate    time.Time
	mu            sync.RWMutex
}

func newScopeState(scopeKey string, bufferSize int) *ScopeState {
	return &ScopeState{
		scopeKey:      scopeKey,
		bufferSize:    bufferSize,
		metricBuffers: make(map[string]*ringBuffer),
	}
}

func (s *ScopeState) RecordMetric(metric string, value float64, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	buffer := s.metricBuffers[metric]
	if buffer == nil {
		buffer = newRingBuffer(s.bufferSize)
		s.metricBuffers[metric] = buffer
	}
	buffer.Add(value)
	s.lastUpdate = now
}

func (s *ScopeState) SampleCount(metric string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if buffer := s.metricBuffers[metric]; buffer != nil {
		return buffer.Count()
	}
	return 0
}

func (s *ScopeState) Mean(metric string) float64 {
	values := s.metricValues(metric)
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, value := range values {
		sum += value
	}
	return sum / float64(len(values))
}

func (s *ScopeState) Std(metric string) float64 {
	values := s.metricValues(metric)
	if len(values) == 0 {
		return 0
	}
	mean := 0.0
	for _, value := range values {
		mean += value
	}
	mean /= float64(len(values))

	sumSq := 0.0
	for _, value := range values {
		diff := value - mean
		sumSq += diff * diff
	}
	return math.Sqrt(sumSq / float64(len(values)))
}

func (s *ScopeState) Min(metric string) float64 {
	values := s.metricValues(metric)
	if len(values) == 0 {
		return 0
	}
	minValue := values[0]
	for _, value := range values[1:] {
		if value < minValue {
			minValue = value
		}
	}
	return minValue
}

func (s *ScopeState) Max(metric string) float64 {
	values := s.metricValues(metric)
	if len(values) == 0 {
		return 0
	}
	maxValue := values[0]
	for _, value := range values[1:] {
		if value > maxValue {
			maxValue = value
		}
	}
	return maxValue
}

func (s *ScopeState) IncrementBreach() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.breachCount++
	return s.breachCount
}

func (s *ScopeState) ResetBreach() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.breachCount = 0
}

func (s *ScopeState) BreachCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.breachCount
}

func (s *ScopeState) LastUpdate() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastUpdate
}

func (s *ScopeState) metricValues(metric string) []float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	buffer := s.metricBuffers[metric]
	if buffer == nil {
		return nil
	}
	return buffer.Snapshot()
}

type ModelMonitorState struct {
	scopes map[string]*ScopeState
	mu     sync.RWMutex
}

type MonitorStateStore struct {
	models map[string]*ModelMonitorState
	mu     sync.RWMutex
}

func NewMonitorStateStore() *MonitorStateStore {
	return &MonitorStateStore{
		models: make(map[string]*ModelMonitorState),
	}
}

func (s *MonitorStateStore) GetOrCreateScope(modelURL, scopeKey string, bufferSize int) *ScopeState {
	s.mu.RLock()
	modelState := s.models[modelURL]
	s.mu.RUnlock()

	if modelState == nil {
		s.mu.Lock()
		modelState = s.models[modelURL]
		if modelState == nil {
			modelState = &ModelMonitorState{scopes: make(map[string]*ScopeState)}
			s.models[modelURL] = modelState
		}
		s.mu.Unlock()
	}

	modelState.mu.RLock()
	scopeState := modelState.scopes[scopeKey]
	modelState.mu.RUnlock()
	if scopeState != nil {
		return scopeState
	}

	modelState.mu.Lock()
	defer modelState.mu.Unlock()
	scopeState = modelState.scopes[scopeKey]
	if scopeState == nil {
		scopeState = newScopeState(scopeKey, bufferSize)
		modelState.scopes[scopeKey] = scopeState
	}
	return scopeState
}

func (s *MonitorStateStore) DeleteModel(modelURL string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.models, modelURL)
}

func (s *MonitorStateStore) GetScope(modelURL, scopeKey string) *ScopeState {
	s.mu.RLock()
	modelState := s.models[modelURL]
	s.mu.RUnlock()
	if modelState == nil {
		return nil
	}

	modelState.mu.RLock()
	defer modelState.mu.RUnlock()
	return modelState.scopes[scopeKey]
}

func (s *MonitorStateStore) ModelExists(modelURL string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.models[modelURL]
	return ok
}

func (s *MonitorStateStore) ResetModelBreaches(modelURL string) {
	s.mu.RLock()
	modelState := s.models[modelURL]
	s.mu.RUnlock()
	if modelState == nil {
		return
	}

	modelState.mu.RLock()
	scopes := make([]*ScopeState, 0, len(modelState.scopes))
	for _, scope := range modelState.scopes {
		scopes = append(scopes, scope)
	}
	modelState.mu.RUnlock()

	for _, scope := range scopes {
		scope.ResetBreach()
	}
}

func (s *MonitorStateStore) GCExpiredScopes(now time.Time, ttl time.Duration) {
	if ttl <= 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for modelURL, modelState := range s.models {
		modelState.mu.Lock()
		for scopeKey, scopeState := range modelState.scopes {
			lastUpdate := scopeState.LastUpdate()
			if !lastUpdate.IsZero() && now.Sub(lastUpdate) > ttl {
				delete(modelState.scopes, scopeKey)
			}
		}
		empty := len(modelState.scopes) == 0
		modelState.mu.Unlock()
		if empty {
			delete(s.models, modelURL)
		}
	}
}
