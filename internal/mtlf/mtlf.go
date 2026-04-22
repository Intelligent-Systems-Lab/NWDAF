// Package mtlf implements the Model Training Logical Function (MTLF) of NWDAF.
// Per TS 23.288: MTLF handles model training, retraining decisions based on
// accuracy reports from AnLF, and model hot-swap after successful retraining.
package mtlf

import (
	"context"
	"sync"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
)

// NwdafApp provides app-level dependencies to MtlfService.
type NwdafApp interface {
	CancelContext() context.Context
	Consumer() *consumer.Consumer
}

// MtlfService is the MTLF entry point.
type MtlfService struct {
	nwdaf          NwdafApp
	wg             *sync.WaitGroup
	stateStore     *MonitorStateStore
	onModelSwapped func(modelUrl string, wg *sync.WaitGroup)
	// onModelSwapReady is called by swapModelAfterRetrain to delegate ML Service
	// operations (load new model, unload old model) to AnLF.
	// Returns the new model ID assigned by the ML Service, or an error.
	onModelSwapReady func(newModelUrl, oldModelId string) (string, error)
	// inFlight tracks async training tasks: taskId → *inFlightEntry.
	// Populated when an async training request is accepted by Daisy;
	// cleared when HandleTrainingComplete is called.
	inFlight sync.Map
	// activeJobs tracks in-progress ADRF-assisted retrain jobs.
	// Key: TID (string) → Value: *retrainJob
	activeJobs sync.Map
	// onRetrainTriggered allows tests to intercept retrain dispatch without
	// starting the external workflow.
	onRetrainTriggered func(modelUrl string, store *nwdaf_context.ModelAccuracyStore)
}

// NewMtlfService creates a new MtlfService instance.
func NewMtlfService(nwdaf NwdafApp) *MtlfService {
	return &MtlfService{
		nwdaf:      nwdaf,
		stateStore: NewMonitorStateStore(),
	}
}

// SetWaitGroup stores the application WaitGroup for goroutine lifecycle management.
func (m *MtlfService) SetWaitGroup(wg *sync.WaitGroup) {
	m.wg = wg
}

// SetOnModelSwapped registers a callback invoked after a successful model hot-swap.
// Used by the processor to wire MTLF → accuracy monitor restart (AnLF side).
func (m *MtlfService) SetOnModelSwapped(fn func(modelUrl string, wg *sync.WaitGroup)) {
	m.onModelSwapped = fn
}

// SetOnModelSwapReady registers a callback that MTLF calls to delegate ML Service
// operations to AnLF during a hot-swap. AnLF loads the new model, unloads the old
// one, and returns the new model ID.
func (m *MtlfService) SetOnModelSwapReady(fn func(newModelUrl, oldModelId string) (string, error)) {
	m.onModelSwapReady = fn
}
