package processor

import (
	"errors"
	"testing"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	"github.com/free5gc/nwdaf/internal/anlf/coordinator"
)

type runtimeCompletionWorkflowStub struct {
	err   error
	event *contract.RuntimeCompletionEvent
}

func (s *runtimeCompletionWorkflowStub) CompleteSubscriptionRuntime(event *contract.RuntimeCompletionEvent) error {
	s.event = event
	return s.err
}

func TestHandleRuntimeCompletion(t *testing.T) {
	event := &contract.RuntimeCompletionEvent{SubscriptionID: "sub-1"}
	workflow := &runtimeCompletionWorkflowStub{}
	p := &Processor{completionWorkflow: workflow}

	if err := p.HandleRuntimeCompletion(event); err != nil {
		t.Fatalf("HandleRuntimeCompletion() error = %v", err)
	}
	if workflow.event != event {
		t.Fatal("runtime completion event was not forwarded")
	}
}

func TestHandleRuntimeCompletionNormalizesFutureRevision(t *testing.T) {
	p := &Processor{completionWorkflow: &runtimeCompletionWorkflowStub{err: coordinator.ErrFutureRuntimeRevision}}

	err := p.HandleRuntimeCompletion(&contract.RuntimeCompletionEvent{})
	if !errors.Is(err, ErrFutureRuntimeRevision) {
		t.Fatalf("error = %v, want future revision outcome", err)
	}
}
