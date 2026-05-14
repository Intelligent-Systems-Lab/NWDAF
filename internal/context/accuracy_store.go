package context

import (
	"context"
	"slices"
	"sync"
	"time"
)

// PredictionRecord stores a prediction for later ground truth comparison
// Per TS 23.288 §5C: comparing predictions against ground truth data
type PredictionRecord struct {
	ID             uint64    // Store-assigned unique identifier for pending lifecycle tracking
	ModelUrl       string    // Model URL this prediction belongs to
	PredictedAt    time.Time // When prediction was made
	TargetTime     time.Time // Semantic time the prediction refers to
	TargetSlotTime time.Time // Slot-aligned time used for pred/actual pairing
	MissCount      int       // Number of monitor rounds where this prediction did not find ground truth
	PredUlVol      int64     // Predicted UL volume
	PredDlVol      int64     // Predicted DL volume
	NwdafSubId     string    // Subscription that generated this prediction
	ScopeKey       string    // Canonical monitoring scope snapshotted at prediction time
}

// ModelAccuracyStore manages prediction records and accuracy state for one model.
// One store per unique modelUrl. Owns its own monitor goroutine lifecycle.
type ModelAccuracyStore struct {
	mu sync.RWMutex

	modelUrl     string
	nextID       uint64
	predictions  []PredictionRecord // Pending predictions awaiting ground truth
	deviation    float64            // Latest computed model-level deviation for debug/observability
	inferenceNum int                // Total inferences since last check
	lastCheck    time.Time          // Last accuracy check time

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
	s.nextID++
	record.ID = s.nextID
	s.predictions = append(s.predictions, record)
	s.inferenceNum++
}

// SnapshotPredictions returns a copy of the current pending predictions.
func (s *ModelAccuracyStore) SnapshotPredictions() []PredictionRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Clone(s.predictions)
}

// ResolvePredictions applies one monitor round's matching results.
// Predictions in matchedIDs are removed. Predictions in missedIDs have MissCount
// incremented and are discarded once MissCount reaches maxMissCount. Predictions
// absent from both sets are preserved as-is, which covers records added after the
// monitor took its snapshot.
func (s *ModelAccuracyStore) ResolvePredictions(
	matchedIDs map[uint64]struct{},
	missedIDs map[uint64]struct{},
	maxMissCount int,
) (matchedCount, discardedCount int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if maxMissCount <= 0 {
		maxMissCount = 1
	}

	updated := make([]PredictionRecord, 0, len(s.predictions))
	for _, pred := range s.predictions {
		if _, matched := matchedIDs[pred.ID]; matched {
			matchedCount++
			continue
		}
		if _, missed := missedIDs[pred.ID]; missed {
			pred.MissCount++
			if pred.MissCount >= maxMissCount {
				discardedCount++
				continue
			}
		}
		updated = append(updated, pred)
	}
	s.predictions = updated
	return matchedCount, discardedCount
}

// DiscardAllPredictions clears all pending predictions and returns the number removed.
func (s *ModelAccuracyStore) DiscardAllPredictions() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := len(s.predictions)
	s.predictions = nil
	return n
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
// While true, MTLF will skip further trigger evaluation for this model.
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
