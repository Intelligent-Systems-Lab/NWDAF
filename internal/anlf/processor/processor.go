package processor

import "github.com/free5gc/openapi/models"

type mlModelProvisionWorkflow interface {
	ProcessMlModelProvisionNotifications(notifications []models.NwdafMlModelProvNotif)
}

type Processor struct {
	workflow mlModelProvisionWorkflow
}

func NewProcessor(workflow mlModelProvisionWorkflow) *Processor {
	return &Processor{workflow: workflow}
}

func (p *Processor) HandleMlModelProvisionNotify(notifications []models.NwdafMlModelProvNotif) {
	if p == nil || p.workflow == nil {
		return
	}
	p.workflow.ProcessMlModelProvisionNotifications(notifications)
}
