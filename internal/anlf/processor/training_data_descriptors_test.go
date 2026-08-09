package processor

import (
	"context"
	"testing"

	"github.com/free5gc/nwdaf/internal/backend"
)

type descriptorRelayStub struct {
	putID    string
	deleteID string
}

func (s *descriptorRelayStub) PutTrainingDataDescriptor(
	_ context.Context,
	id string,
	_ []byte,
) (*backend.StandardResponse, error) {
	s.putID = id
	return &backend.StandardResponse{StatusCode: 204}, nil
}

func (s *descriptorRelayStub) DeleteTrainingDataDescriptor(
	_ context.Context,
	id string,
) (*backend.StandardResponse, error) {
	s.deleteID = id
	return &backend.StandardResponse{StatusCode: 204}, nil
}

type descriptorAvailabilityStub struct {
	admitted bool
	calls    int
}

func (s *descriptorAvailabilityStub) Acquire() (*backend.GenerationLease, bool) {
	s.calls++
	return nil, s.admitted
}

func TestTrainingDataDescriptorRelayUsesGenerationAdmission(t *testing.T) {
	relay := &descriptorRelayStub{}
	availability := &descriptorAvailabilityStub{admitted: true}
	processor := NewProcessor()
	processor.SetTrainingDataDescriptorRelay(relay, availability)

	if err := processor.PutTrainingDataDescriptor(
		t.Context(), "descriptor-a", []byte(`{}`),
	); err != nil {
		t.Fatalf("PutTrainingDataDescriptor() error = %v", err)
	}
	if err := processor.DeleteTrainingDataDescriptor(t.Context(), "descriptor-a"); err != nil {
		t.Fatalf("DeleteTrainingDataDescriptor() error = %v", err)
	}
	if availability.calls != 2 || relay.putID != "descriptor-a" || relay.deleteID != "descriptor-a" {
		t.Fatalf("availability calls=%d relay=%+v", availability.calls, relay)
	}
}

func TestTrainingDataDescriptorRelayRejectsClosedAdmission(t *testing.T) {
	relay := &descriptorRelayStub{}
	processor := NewProcessor()
	processor.SetTrainingDataDescriptorRelay(
		relay,
		&descriptorAvailabilityStub{admitted: false},
	)

	err := processor.PutTrainingDataDescriptor(t.Context(), "descriptor-a", nil)
	if err != ErrTrainingDataDescriptorUnavailable {
		t.Fatalf("PutTrainingDataDescriptor() error = %v", err)
	}
	if relay.putID != "" {
		t.Fatalf("closed admission reached relay: %q", relay.putID)
	}
}
