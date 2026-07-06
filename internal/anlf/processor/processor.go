package processor

import (
	"github.com/free5gc/nwdaf/internal/anlf"
	"github.com/free5gc/openapi/models"
)

type mlModelProvisionWorkflow interface {
	PlanModelProvisionActions(notif *models.NwdafMlModelProvNotif) []anlf.ModelProvisionAction
	StartModelProvisionActions(actions []anlf.ModelProvisionAction)
}

type Processor struct {
	workflow mlModelProvisionWorkflow
}

func NewProcessor(workflow mlModelProvisionWorkflow) *Processor {
	return &Processor{workflow: workflow}
}
