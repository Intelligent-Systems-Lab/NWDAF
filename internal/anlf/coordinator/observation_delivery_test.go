package coordinator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	"github.com/free5gc/nwdaf/pkg/factory"
)

func TestObservationDeliveryRetryKeepsBatchID(t *testing.T) {
	backend := &fakeAnlfBackendClient{sendErrors: []error{errors.New("temporary"), nil}}
	delivery := NewObservationDelivery(context.Background(), backend, &factory.ObservationDeliveryConfig{
		QueueCapacity:  2,
		RequestTimeout: 1,
		MaxRetries:     1,
		RetryInterval:  1,
	})
	delivery.retryInterval = time.Millisecond
	item := queuedObservationBatch{
		sourceID: "corr-1",
		batch: contract.ObservationBatch{
			BatchID:      "batch-stable",
			Observations: []contract.SourceObservation{{}},
		},
	}

	delivery.deliver(item)

	if backend.sendCalls != 2 {
		t.Fatalf("send calls = %d, want 2", backend.sendCalls)
	}
	if backend.sentBatches[0].BatchID != backend.sentBatches[1].BatchID {
		t.Fatalf(
			"batch ID changed across retry: %q != %q",
			backend.sentBatches[0].BatchID,
			backend.sentBatches[1].BatchID,
		)
	}
}
