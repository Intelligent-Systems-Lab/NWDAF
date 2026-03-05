package context

import (
	"context"
	"sync"
	"time"
)

// PredictionRecord stores a prediction for later ground truth comparison
// Per TS 23.288 §5C: comparing predictions against ground truth data
type PredictionRecord struct {
	ModelUrl    string    // Model URL this prediction belongs to
	PredictedAt time.Time // When prediction was made
	TargetTime  time.Time // Time the prediction refers to
	PredUlVol   int64     // Predicted UL volume
	PredDlVol   int64     // Predicted DL volume
	NwdafSubId  string    // Subscription that generated this prediction
}

// ModelAccuracyStore manages prediction records and accuracy state for one model.
// One store per unique modelUrl. Owns its own monitor goroutine lifecycle.
type ModelAccuracyStore struct {
	mu sync.RWMutex

	modelUrl     string
	predictions  []PredictionRecord // Pending predictions awaiting ground truth
	deviation    float64            // Latest computed deviation (NRMSE)
	inferenceNum int                // Total inferences since last check
	lastCheck    time.Time          // Last accuracy check time

	// Trigger strategy state
	consecutiveBreaches int     // Count of consecutive threshold breaches
	emaDeviation        float64 // Exponential moving average of deviation
	emaInitialized      bool    // Whether EMA has been seeded

	// Goroutine lifecycle (per-model)
	cancelFunc context.CancelFunc // Stops this model's monitor goroutine
	running    bool               // Whether monitor loop is active

	// Retraining guard — prevents duplicate retrain triggers while one is in flight
	retraining bool
}

// NewModelAccuracyStore creates a new per-model accuracy store
func NewModelAccuracyStore(modelUrl string) *ModelAccuracyStore {
	return &ModelAccuracyStore{
		modelUrl:  modelUrl,
		lastCheck: time.Now(),
	}
}

// AddPrediction stores a prediction for later ground truth comparison
func (s *ModelAccuracyStore) AddPrediction(record PredictionRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.predictions = append(s.predictions, record)
	s.inferenceNum++
}

// ConsumeMaturePredictions returns predictions where TargetTime+graceAfterTarget < now.
// graceAfterTarget should be at least 2×samplingInterval so the UPF reporting period
// has ended and the report has had time to arrive before ground truth is looked up.
func (s *ModelAccuracyStore) ConsumeMaturePredictions(graceAfterTarget time.Duration) []PredictionRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	deadline := time.Now().Add(-graceAfterTarget)
	var mature, pending []PredictionRecord
	for _, p := range s.predictions {
		if p.TargetTime.Before(deadline) {
			mature = append(mature, p)
		} else {
			pending = append(pending, p)
		}
	}
	s.predictions = pending
	return mature
}

// UpdateDeviation updates the latest computed deviation value
func (s *ModelAccuracyStore) UpdateDeviation(deviation float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deviation = deviation
	s.lastCheck = time.Now()
}

// GetDeviation returns the latest deviation value
func (s *ModelAccuracyStore) GetDeviation() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.deviation
}

// GetAndResetInferenceNum returns the inference count and resets it
func (s *ModelAccuracyStore) GetAndResetInferenceNum() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.inferenceNum
	s.inferenceNum = 0
	return n
}

// --- Goroutine Lifecycle ---

// IsMonitorRunning returns true if the monitor goroutine is active
func (s *ModelAccuracyStore) IsMonitorRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.running
}

// TryStartMonitor atomically checks whether the monitor is already running and,
// if not, marks it as running and stores cancel. Returns true if this caller
// won the race and must start the goroutine; false if already running (cancel
// is called internally so the caller need not clean it up).
func (s *ModelAccuracyStore) TryStartMonitor(cancel context.CancelFunc) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		cancel()
		return false
	}
	s.running = true
	s.cancelFunc = cancel
	return true
}

// SetRetraining marks whether a retrain is currently in flight.
// While true, HandleDeviationReport will skip further trigger evaluation.
func (s *ModelAccuracyStore) SetRetraining(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retraining = v
}

// IsRetraining returns true if a retrain is currently in flight.
func (s *ModelAccuracyStore) IsRetraining() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.retraining
}

// StopMonitor cancels the monitor goroutine
func (s *ModelAccuracyStore) StopMonitor() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancelFunc != nil {
		s.cancelFunc()
		s.cancelFunc = nil
	}
	s.running = false
}

// --- Trigger Strategy State ---

// IncrementBreaches increments consecutive breach counter, returns new count
func (s *ModelAccuracyStore) IncrementBreaches() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consecutiveBreaches++
	return s.consecutiveBreaches
}

// ResetBreaches resets the consecutive breach counter to zero
func (s *ModelAccuracyStore) ResetBreaches() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consecutiveBreaches = 0
}

// UpdateEMA updates the EMA deviation and returns the new value
func (s *ModelAccuracyStore) UpdateEMA(deviation, alpha float64) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.emaInitialized {
		s.emaDeviation = deviation
		s.emaInitialized = true
	} else {
		s.emaDeviation = alpha*deviation + (1-alpha)*s.emaDeviation
	}
	return s.emaDeviation
}

// GetEMA returns the current EMA deviation value
func (s *ModelAccuracyStore) GetEMA() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.emaDeviation
}
