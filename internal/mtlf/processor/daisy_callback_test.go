package processor

import (
	"testing"

	"github.com/free5gc/nwdaf/internal/mtlf"
)

type fakeTrainingCompleteWorkflow struct {
	completion       mtlf.TrainingCompletion
	known            bool
	takeCalls        int
	failureCalls     int
	successCalls     int
	lastTaskID       string
	lastModelURL     string
	lastFailureError string
}

func (f *fakeTrainingCompleteWorkflow) TakeTrainingCompletion(taskID string) (mtlf.TrainingCompletion, bool) {
	f.takeCalls++
	f.lastTaskID = taskID
	return f.completion, f.known
}

func (f *fakeTrainingCompleteWorkflow) HandleFailedTrainingCompletion(
	taskID string,
	completion mtlf.TrainingCompletion,
	errMsg string,
) {
	f.failureCalls++
	f.lastTaskID = taskID
	f.lastFailureError = errMsg
}

func (f *fakeTrainingCompleteWorkflow) HandleSuccessfulTrainingCompletion(
	taskID string,
	completion mtlf.TrainingCompletion,
	modelURL string,
) {
	f.successCalls++
	f.lastTaskID = taskID
	f.lastModelURL = modelURL
}

func TestHandleDaisyTrainingComplete_FailurePath(t *testing.T) {
	workflow := &fakeTrainingCompleteWorkflow{known: true}
	p := NewProcessor(workflow)

	p.HandleDaisyTrainingComplete("task-1", "", "failure", "boom")

	if workflow.takeCalls != 1 {
		t.Fatalf("take call count = %d, want 1", workflow.takeCalls)
	}
	if workflow.failureCalls != 1 {
		t.Fatalf("failure call count = %d, want 1", workflow.failureCalls)
	}
	if workflow.successCalls != 0 {
		t.Fatalf("success call count = %d, want 0", workflow.successCalls)
	}
}

func TestHandleDaisyTrainingComplete_SuccessPath(t *testing.T) {
	workflow := &fakeTrainingCompleteWorkflow{known: true}
	p := NewProcessor(workflow)

	p.HandleDaisyTrainingComplete("task-1", "http://example.com/model.onnx", "success", "")

	if workflow.successCalls != 1 {
		t.Fatalf("success call count = %d, want 1", workflow.successCalls)
	}
	if workflow.lastModelURL != "http://example.com/model.onnx" {
		t.Fatalf("modelURL = %q", workflow.lastModelURL)
	}
}

func TestHandleDaisyTrainingComplete_UnknownTask(t *testing.T) {
	workflow := &fakeTrainingCompleteWorkflow{}
	p := NewProcessor(workflow)

	p.HandleDaisyTrainingComplete("task-1", "", "failure", "boom")

	if workflow.takeCalls != 1 {
		t.Fatalf("take call count = %d, want 1", workflow.takeCalls)
	}
	if workflow.failureCalls != 0 || workflow.successCalls != 0 {
		t.Fatalf("unexpected downstream calls: failure=%d success=%d",
			workflow.failureCalls, workflow.successCalls)
	}
}
