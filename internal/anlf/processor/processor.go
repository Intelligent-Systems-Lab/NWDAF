package processor

import (
	"github.com/free5gc/nwdaf/internal/anlf/contract"
	"github.com/free5gc/nwdaf/internal/anlf/coordinator"
)

type mlModelProvisionWorkflow interface {
	PlanModelProvisionActions(notif *contract.ModelProvisionNotification) []coordinator.ModelProvisionAction
	StartModelProvisionActions(actions []coordinator.ModelProvisionAction)
}

type modelAccuracyWorkflow interface {
	HandleModelAccuracyReport(report *contract.ModelAccuracyReport) error
}

type analyticsReportDispatcher interface {
	DispatchAnalyticsReport(subscriptionID string, report *contract.AnalyticsReport) error
}

type runtimeCompletionWorkflow interface {
	CompleteSubscriptionRuntime(event *contract.RuntimeCompletionEvent) error
}

type Processor struct {
	workflow           mlModelProvisionWorkflow
	accuracyWorkflow   modelAccuracyWorkflow
	reportDispatcher   analyticsReportDispatcher
	completionWorkflow runtimeCompletionWorkflow
}

func (p *Processor) SetModelAccuracyWorkflow(workflow modelAccuracyWorkflow) {
	p.accuracyWorkflow = workflow
}

func NewProcessor(
	workflow mlModelProvisionWorkflow,
	dispatchers ...analyticsReportDispatcher,
) *Processor {
	processor := &Processor{workflow: workflow}
	if completionWorkflow, ok := workflow.(runtimeCompletionWorkflow); ok {
		processor.completionWorkflow = completionWorkflow
	}
	if len(dispatchers) > 0 {
		processor.reportDispatcher = dispatchers[0]
	}
	return processor
}
