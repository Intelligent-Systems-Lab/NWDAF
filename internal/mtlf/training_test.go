package mtlf

import (
	"testing"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
)

// TestHandleTrainingComplete_UnknownTaskId verifies that an unknown taskId is a no-op.
func TestHandleTrainingComplete_UnknownTaskId(t *testing.T) {
	m := &MtlfService{}
	// Should not panic
	m.HandleTrainingComplete("no-such-id", "model.npy", "success", "")
}

// TestHandleTrainingComplete_Failure_ClearsRetraining verifies that a failed
// training callback clears the store's retraining flag and removes the entry.
func TestHandleTrainingComplete_Failure_ClearsRetraining(t *testing.T) {
	m := &MtlfService{}
	store := nwdaf_context.NewModelAccuracyStore("test://old-model")
	store.SetRetraining(true)

	const taskId = "task-fail-001"
	m.inFlight.Store(taskId, &inFlightEntry{
		oldModelUrl: "old.npy",
		store:       store,
	})

	m.HandleTrainingComplete(taskId, "", "failure", "training error")

	if store.IsRetraining() {
		t.Error("IsRetraining should be cleared after training failure")
	}
	if _, ok := m.inFlight.Load(taskId); ok {
		t.Error("inFlight entry should be removed after HandleTrainingComplete")
	}
}

// TestHandleTrainingComplete_Failure_NilStore verifies that a nil store is handled safely.
func TestHandleTrainingComplete_Failure_NilStore(t *testing.T) {
	m := &MtlfService{}

	const taskId = "task-fail-002"
	m.inFlight.Store(taskId, &inFlightEntry{
		oldModelUrl: "old.npy",
		store:       nil,
	})

	// Should not panic even with nil store
	m.HandleTrainingComplete(taskId, "", "failure", "some error")

	if _, ok := m.inFlight.Load(taskId); ok {
		t.Error("inFlight entry should be removed after HandleTrainingComplete")
	}
}

// TestHandleTrainingComplete_Success_ClearsInFlight verifies that a successful
// callback removes the in-flight entry. swapModelAfterRetrain will return early
// because factory.NwdafConfig is nil in unit tests.
func TestHandleTrainingComplete_Success_ClearsInFlight(t *testing.T) {
	m := &MtlfService{}

	const taskId = "task-ok-001"
	m.inFlight.Store(taskId, &inFlightEntry{
		oldModelUrl: "old.npy",
		store:       nil,
	})

	m.HandleTrainingComplete(taskId, "new.npy", "success", "")

	if _, ok := m.inFlight.Load(taskId); ok {
		t.Error("inFlight entry should be removed on success")
	}
}

// TestHandleTrainingComplete_DuplicateCallback verifies that a second callback
// for the same taskId is silently ignored (already deleted by LoadAndDelete).
func TestHandleTrainingComplete_DuplicateCallback(t *testing.T) {
	m := &MtlfService{}
	store := nwdaf_context.NewModelAccuracyStore("test://dup")
	store.SetRetraining(true)

	const taskId = "task-dup-001"
	m.inFlight.Store(taskId, &inFlightEntry{
		oldModelUrl: "old.npy",
		store:       store,
	})

	m.HandleTrainingComplete(taskId, "", "failure", "err")
	// Second call with same taskId — should be a no-op
	m.HandleTrainingComplete(taskId, "", "failure", "err")
}
