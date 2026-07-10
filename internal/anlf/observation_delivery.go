package anlf

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/pkg/factory"
)

type queuedObservationBatch struct {
	sourceID string
	batch    ObservationBatch
}

type ObservationDelivery struct {
	baseCtx        context.Context
	cancel         context.CancelFunc
	backend        AnlfBackendAPI
	queue          chan queuedObservationBatch
	requestTimeout time.Duration
	maxRetries     int
	retryInterval  time.Duration
	startOnce      sync.Once
	stopOnce       sync.Once
	wg             sync.WaitGroup
}

func NewObservationDelivery(
	baseCtx context.Context,
	backend AnlfBackendAPI,
	cfg *factory.ObservationDeliveryConfig,
) *ObservationDelivery {
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	workerCtx, cancel := context.WithCancel(baseCtx)
	return &ObservationDelivery{
		baseCtx:        workerCtx,
		cancel:         cancel,
		backend:        backend,
		queue:          make(chan queuedObservationBatch, cfg.QueueCapacityOrDefault()),
		requestTimeout: time.Duration(cfg.RequestTimeoutOrDefault()) * time.Second,
		maxRetries:     cfg.MaxRetriesOrDefault(),
		retryInterval:  time.Duration(cfg.RetryIntervalOrDefault()) * time.Second,
	}
}

func (d *ObservationDelivery) Start() {
	if d == nil || d.backend == nil {
		return
	}
	d.startOnce.Do(func() {
		d.wg.Add(1)
		go d.run()
	})
}

func (d *ObservationDelivery) Stop() {
	if d == nil {
		return
	}
	d.stopOnce.Do(func() {
		d.cancel()
		d.wg.Wait()
	})
}

func (d *ObservationDelivery) Enqueue(sourceID string, observations []SourceObservation) bool {
	if d == nil || d.backend == nil || sourceID == "" || len(observations) == 0 {
		return false
	}
	item := queuedObservationBatch{
		sourceID: sourceID,
		batch: ObservationBatch{
			BatchID:      uuid.NewString(),
			Observations: observations,
		},
	}
	select {
	case <-d.baseCtx.Done():
		return false
	case d.queue <- item:
		return true
	default:
		logger.AnlfLog.Warnf("Observation delivery queue full: source=%s", sourceID)
		return false
	}
}

func (d *ObservationDelivery) run() {
	defer d.wg.Done()
	for {
		select {
		case <-d.baseCtx.Done():
			return
		case item := <-d.queue:
			d.deliver(item)
		}
	}
}

func (d *ObservationDelivery) deliver(item queuedObservationBatch) {
	for attempt := 0; attempt <= d.maxRetries; attempt++ {
		if d.baseCtx.Err() != nil {
			return
		}
		requestCtx, cancel := context.WithTimeout(d.baseCtx, d.requestTimeout)
		err := d.backend.SendObservations(requestCtx, item.sourceID, item.batch)
		cancel()
		if err == nil {
			return
		}
		if attempt == d.maxRetries {
			logger.AnlfLog.Errorf(
				"Observation delivery exhausted retries: source=%s batch=%s err=%v",
				item.sourceID,
				item.batch.BatchID,
				err,
			)
			return
		}
		select {
		case <-d.baseCtx.Done():
			return
		case <-time.After(d.retryInterval):
		}
	}
}
