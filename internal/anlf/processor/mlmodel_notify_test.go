package processor

import (
	"testing"

	"github.com/free5gc/nwdaf/internal/anlf"
	"github.com/free5gc/openapi/models"
)

type fakeMlModelProvisionWorkflow struct {
	plannedNotifications []string
	plannedActions       []anlf.ModelProvisionAction
	startedBatches       [][]anlf.ModelProvisionAction
}

func (f *fakeMlModelProvisionWorkflow) PlanModelProvisionActions(
	notif *models.NwdafMlModelProvNotif,
) []anlf.ModelProvisionAction {
	if notif != nil {
		f.plannedNotifications = append(f.plannedNotifications, notif.SubscriptionId)
	}
	if len(f.plannedActions) == 0 {
		return nil
	}
	actions := make([]anlf.ModelProvisionAction, len(f.plannedActions))
	copy(actions, f.plannedActions)
	return actions
}

func (f *fakeMlModelProvisionWorkflow) StartModelProvisionActions(actions []anlf.ModelProvisionAction) {
	copied := make([]anlf.ModelProvisionAction, len(actions))
	copy(copied, actions)
	f.startedBatches = append(f.startedBatches, copied)
}

func TestHandleMlModelProvisionNotify_PlansAndStartsActions(t *testing.T) {
	workflow := &fakeMlModelProvisionWorkflow{
		plannedActions: []anlf.ModelProvisionAction{
			{Request: anlf.ApplySubscriptionRuntimeRequest{
				Subscription: anlf.SubscriptionRuntimeContext{SubscriptionID: "sub-1"},
			}},
			{Request: anlf.ApplySubscriptionRuntimeRequest{
				Subscription: anlf.SubscriptionRuntimeContext{SubscriptionID: "sub-2"},
			}},
		},
	}
	p := NewProcessor(workflow)

	p.HandleMlModelProvisionNotify([]models.NwdafMlModelProvNotif{
		{SubscriptionId: "notif-1"},
		{SubscriptionId: "notif-2"},
	})

	if len(workflow.plannedNotifications) != 2 {
		t.Fatalf("planned notification count = %d, want 2", len(workflow.plannedNotifications))
	}
	if len(workflow.startedBatches) != 1 {
		t.Fatalf("started batch count = %d, want 1", len(workflow.startedBatches))
	}
	if len(workflow.startedBatches[0]) != 4 {
		t.Fatalf("flattened batch size = %d, want 4", len(workflow.startedBatches[0]))
	}
	firstID := workflow.startedBatches[0][0].Request.Subscription.SubscriptionID
	lastID := workflow.startedBatches[0][3].Request.Subscription.SubscriptionID
	if firstID != "sub-1" || lastID != "sub-2" {
		t.Fatalf("flattened batch order was not preserved: first=%q last=%q",
			firstID, lastID)
	}
}

func TestHandleMlModelProvisionNotify_IgnoresNilWorkflow(t *testing.T) {
	p := NewProcessor(nil)
	p.HandleMlModelProvisionNotify([]models.NwdafMlModelProvNotif{{SubscriptionId: "notif-1"}})
}
