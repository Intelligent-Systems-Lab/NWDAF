package processor

import (
	"github.com/free5gc/nwdaf/internal/anlf"
	"github.com/free5gc/openapi/models"
)

type mlModelProvisionWorkflow interface {
	PlanModelProvisionActions(notif *models.NwdafMlModelProvNotif) []anlf.ModelProvisionAction
	StartModelProvisionActions(actions []anlf.ModelProvisionAction)
}

type analyticsReportDispatcher interface {
	DispatchAnalyticsReport(subscriptionID string, report *anlf.AnalyticsReport) error
}

type Processor struct {
	workflow         mlModelProvisionWorkflow
	reportDispatcher analyticsReportDispatcher
}

func NewProcessor(
	workflow mlModelProvisionWorkflow,
	dispatchers ...analyticsReportDispatcher,
) *Processor {
	processor := &Processor{workflow: workflow}
	if len(dispatchers) > 0 {
		processor.reportDispatcher = dispatchers[0]
	}
	return processor
}
