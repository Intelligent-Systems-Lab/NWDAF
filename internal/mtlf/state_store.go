package mtlf

import (
	"math"
	"sort"
	"sync"
	"time"
)

type ScopeObservation struct {
	Timestamp             time.Time
	SampleCount           int
	TrafficScale          float64
	PredictedTrafficScale float64
	Metrics               map[string]float64
}

type observationBuffer struct {
	observations []ScopeObservation
	next         int
}

type hitWindow struct {
	values    []bool
	next      int
	trueCount int
}

func newObservationBuffer(size int) *observationBuffer {
	if size <= 0 {
		size = 1
	}
	return &observationBuffer{observations: make([]ScopeObservation, 0, size)}
}

func newHitWindow(size int) *hitWindow {
	if size <= 0 {
		size = 1
	}
	return &hitWindow{values: make([]bool, 0, size)}
}

func cloneObservation(observation ScopeObservation) ScopeObservation {
	cloned := observation
	if observation.Metrics != nil {
		cloned.Metrics = make(map[string]float64, len(observation.Metrics))
		for metric, value := range observation.Metrics {
			cloned.Metrics[metric] = value
		}
	}
	return cloned
}

func (b *observationBuffer) Add(observation ScopeObservation) {
	observation = cloneObservation(observation)
	if len(b.observations) < cap(b.observations) {
		b.observations = append(b.observations, observation)
		return
	}
	b.observations[b.next] = observation
	b.next = (b.next + 1) % cap(b.observations)
}

func (b *observationBuffer) Count() int {
	return len(b.observations)
}

func (b *observationBuffer) Snapshot() []ScopeObservation {
	if len(b.observations) == 0 {
		return nil
	}
	out := make([]ScopeObservation, 0, len(b.observations))
	if len(b.observations) < cap(b.observations) || b.next == 0 {
		for _, observation := range b.observations {
			out = append(out, cloneObservation(observation))
		}
		return out
	}
	for _, observation := range b.observations[b.next:] {
		out = append(out, cloneObservation(observation))
	}
	for _, observation := range b.observations[:b.next] {
		out = append(out, cloneObservation(observation))
	}
	return out
}

func observationMetricValue(observation ScopeObservation, metric string) (float64, bool) {
	switch metric {
	case trafficScaleMetricName:
		return observation.TrafficScale, true
	case predictedTrafficScaleMetricName:
		return observation.PredictedTrafficScale, true
	default:
		if observation.Metrics == nil {
			return 0, false
		}
		value, ok := observation.Metrics[metric]
		return value, ok
	}
}

func (b *observationBuffer) metricValues(metric string) []float64 {
	observations := b.Snapshot()
	if len(observations) == 0 {
		return nil
	}
	values := make([]float64, 0, len(observations))
	for _, observation := range observations {
		value, ok := observationMetricValue(observation, metric)
		if ok {
			values = append(values, value)
		}
	}
	return values
}

func (w *hitWindow) Add(hit bool) int {
	if len(w.values) < cap(w.values) {
		w.values = append(w.values, hit)
		if hit {
			w.trueCount++
		}
		return w.trueCount
	}

	if w.values[w.next] {
		w.trueCount--
	}
	w.values[w.next] = hit
	if hit {
		w.trueCount++
	}
	w.next = (w.next + 1) % cap(w.values)
	return w.trueCount
}

func (w *hitWindow) TrueCount() int {
	return w.trueCount
}

func (w *hitWindow) Reset() {
	w.values = w.values[:0]
	w.next = 0
	w.trueCount = 0
}

type ScopeState struct {
	scopeKey           string
	bufferSize         int
	decisionWindowSize int
	recentObservations *observationBuffer
	degradationRef     *observationBuffer
	degradationWindow  *hitWindow
	chronicWindow      *hitWindow
	lowTrafficWindow   *hitWindow
	lastUpdate         time.Time
	mu                 sync.RWMutex
}

func newScopeState(scopeKey string, bufferSize, decisionWindowSize int) *ScopeState {
	return &ScopeState{
		scopeKey:           scopeKey,
		bufferSize:         bufferSize,
		decisionWindowSize: decisionWindowSize,
		recentObservations: newObservationBuffer(bufferSize),
		degradationRef:     newObservationBuffer(bufferSize),
		degradationWindow:  newHitWindow(decisionWindowSize),
		chronicWindow:      newHitWindow(decisionWindowSize),
		lowTrafficWindow:   newHitWindow(decisionWindowSize),
	}
}

func (s *ScopeState) RecordObservation(observation ScopeObservation) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.recentObservations.Add(observation)
	s.lastUpdate = observation.Timestamp
}

func (s *ScopeState) RecordDegradationReference(observation ScopeObservation) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.degradationRef.Add(observation)
	s.lastUpdate = observation.Timestamp
}

func (s *ScopeState) SampleCount(metric string) int {
	return len(s.metricValuesFromBuffer(s.recentObservations, metric))
}

func (s *ScopeState) DegradationSampleCount(metric string) int {
	return len(s.metricValuesFromBuffer(s.degradationRef, metric))
}

func (s *ScopeState) RecentObservationCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.recentObservations.Count()
}

func (s *ScopeState) DegradationReferenceCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.degradationRef.Count()
}

func (s *ScopeState) Mean(metric string) float64 {
	values := s.metricValuesFromBuffer(s.recentObservations, metric)
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, value := range values {
		sum += value
	}
	return sum / float64(len(values))
}

func (s *ScopeState) DegradationMean(metric string) float64 {
	values := s.metricValuesFromBuffer(s.degradationRef, metric)
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
	values := s.metricValuesFromBuffer(s.recentObservations, metric)
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

func (s *ScopeState) DegradationStd(metric string) float64 {
	values := s.metricValuesFromBuffer(s.degradationRef, metric)
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

func (s *ScopeState) Percentile(metric string, percentile int) float64 {
	values := s.metricValuesFromBuffer(s.recentObservations, metric)
	if len(values) == 0 {
		return 0
	}
	sort.Float64s(values)

	if percentile <= 0 {
		return values[0]
	}
	if percentile >= 100 {
		return values[len(values)-1]
	}

	position := (float64(percentile) / 100.0) * float64(len(values)-1)
	lower := int(math.Floor(position))
	upper := int(math.Ceil(position))
	if lower == upper {
		return values[lower]
	}
	weight := position - float64(lower)
	return values[lower] + (values[upper]-values[lower])*weight
}

func (s *ScopeState) Min(metric string) float64 {
	values := s.metricValuesFromBuffer(s.recentObservations, metric)
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
	values := s.metricValuesFromBuffer(s.recentObservations, metric)
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
	return s.RecordDegradationOutcome(true)
}

func (s *ScopeState) ResetBreach() {
	s.ResetDegradationWindow()
}

func (s *ScopeState) BreachCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.degradationWindow.TrueCount()
}

func (s *ScopeState) RecordDegradationOutcome(hit bool) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.degradationWindow.Add(hit)
}

func (s *ScopeState) RecordChronicOutcome(hit bool) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.chronicWindow.Add(hit)
}

func (s *ScopeState) RecordLowTrafficOutcome(hit bool) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lowTrafficWindow.Add(hit)
}

func (s *ScopeState) ChronicHitCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.chronicWindow.TrueCount()
}

func (s *ScopeState) LowTrafficHitCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lowTrafficWindow.TrueCount()
}

func (s *ScopeState) ResetDecisionWindows() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.degradationWindow.Reset()
	s.chronicWindow.Reset()
	s.lowTrafficWindow.Reset()
}

func (s *ScopeState) ResetDegradationWindow() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.degradationWindow.Reset()
}

func (s *ScopeState) ResetChronicWindow() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.chronicWindow.Reset()
}

func (s *ScopeState) ResetLowTrafficWindow() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lowTrafficWindow.Reset()
}

func (s *ScopeState) LastUpdate() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastUpdate
}

func (s *ScopeState) metricValuesFromBuffer(buffer *observationBuffer, metric string) []float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if buffer == nil {
		return nil
	}
	return buffer.metricValues(metric)
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

func (s *MonitorStateStore) GetOrCreateScope(
	modelURL, scopeKey string,
	bufferSize, decisionWindowSize int,
) *ScopeState {
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
		scopeState = newScopeState(scopeKey, bufferSize, decisionWindowSize)
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
		scope.ResetDecisionWindows()
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
