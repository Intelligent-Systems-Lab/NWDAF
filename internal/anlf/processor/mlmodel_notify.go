package processor

import (
	"github.com/free5gc/nwdaf/internal/anlf/contract"
	"github.com/free5gc/nwdaf/internal/anlf/coordinator"
)

func (p *Processor) HandleMlModelProvisionNotify(notifications []contract.ModelProvisionNotification) {
	if p == nil || p.workflow == nil {
		return
	}

	allActions := make([]coordinator.ModelProvisionAction, 0)
	for i := range notifications {
		actions := p.workflow.PlanModelProvisionActions(&notifications[i])
		if len(actions) > 0 {
			allActions = append(allActions, actions...)
		}
	}

	if len(allActions) == 0 {
		return
	}
	p.workflow.StartModelProvisionActions(allActions)
}
