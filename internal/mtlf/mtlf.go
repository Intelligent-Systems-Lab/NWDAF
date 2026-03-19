// Package mtlf implements the Model Training Logical Function (MTLF) of NWDAF.
// Per TS 23.288: MTLF handles model training, retraining decisions based on
// accuracy reports from AnLF, and model hot-swap after successful retraining.
package mtlf

import (
	"context"
	"sync"

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
	onModelSwapped func(modelUrl string, wg *sync.WaitGroup)
	// inFlight tracks async training tasks: taskId → *inFlightEntry.
	// Populated when an async training request is accepted by Daisy;
	// cleared when HandleTrainingComplete is called.
	inFlight sync.Map
}

// NewMtlfService creates a new MtlfService instance.
func NewMtlfService(nwdaf NwdafApp) *MtlfService {
	return &MtlfService{nwdaf: nwdaf}
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
