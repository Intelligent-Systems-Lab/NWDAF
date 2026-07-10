// Package mtlf implements the Model Training Logical Function (MTLF) of NWDAF.
// Per TS 23.288: MTLF handles model training, retraining decisions based on
// accuracy reports from AnLF, and model hot-swap after successful retraining.
package mtlf

import (
	"context"
	"sync"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/pkg/app"
	"github.com/free5gc/nwdaf/pkg/factory"
)

// NwdafApp provides app-level dependencies to MtlfService.
type NwdafApp interface {
	app.App
	CancelContext() context.Context
}

// MtlfService is the MTLF entry point.
type MtlfService struct {
	nwdaf                   NwdafApp
	daisyClient             DaisyAPI
	adrfClient              consumer.AdrfServiceAPI
	wg                      *sync.WaitGroup
	stateStore              *MonitorStateStore
	onModelProvisionUpdated func(oldModelReference, newModelReference string) error
	// inFlight tracks async training tasks: taskId → *inFlightEntry.
	// Populated when an async training request is accepted by Daisy;
	// cleared when the training-complete processor consumes the callback.
	inFlight sync.Map
	// activeJobs tracks in-progress ADRF-assisted retrain jobs.
	// Key: TID (string) → Value: *retrainJob
	activeJobs sync.Map
	// onRetrainTriggered allows tests to intercept retrain dispatch without
	// starting the external workflow.
	onRetrainTriggered func(modelUrl string, store *nwdaf_context.ModelAccuracyStore)
}

// NewMtlfService creates a new MtlfService instance.
func NewMtlfService(
	nwdaf NwdafApp,
	daisyClient DaisyAPI,
	adrfClient consumer.AdrfServiceAPI,
) *MtlfService {
	return &MtlfService{
		nwdaf:       nwdaf,
		daisyClient: daisyClient,
		adrfClient:  adrfClient,
		stateStore:  NewMonitorStateStore(),
	}
}

func (m *MtlfService) config() *factory.Config {
	if m == nil || m.nwdaf == nil {
		return nil
	}
	return m.nwdaf.Config()
}

func (m *MtlfService) buildMtlfURL(urlPath string) string {
	cfg := m.config()
	if cfg == nil {
		return ""
	}
	return cfg.GetMtlfServerURI() + urlPath
}

func (m *MtlfService) buildTrainingCompleteCallbackURL() string {
	return m.buildMtlfURL("/mtlf/training-complete")
}

func (m *MtlfService) buildCollectorRetrievalNotifyURL() string {
	cfg := m.config()
	if cfg == nil {
		return ""
	}
	return cfg.GetSbiUri() + "/collector/retrieval-notify"
}

// SetWaitGroup stores the application WaitGroup for goroutine lifecycle management.
func (m *MtlfService) SetWaitGroup(wg *sync.WaitGroup) {
	m.wg = wg
}

func (m *MtlfService) SetOnModelProvisionUpdated(
	fn func(oldModelReference, newModelReference string) error,
) {
	m.onModelProvisionUpdated = fn
}

func (m *MtlfService) launchOwnedTask(fn func()) {
	if fn == nil {
		return
	}
	if m.wg != nil {
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			fn()
		}()
		return
	}
	go fn()
}

func (m *MtlfService) shutdownStarted() bool {
	if m == nil || m.nwdaf == nil {
		return false
	}
	ctx := m.nwdaf.CancelContext()
	return ctx != nil && ctx.Err() != nil
}
