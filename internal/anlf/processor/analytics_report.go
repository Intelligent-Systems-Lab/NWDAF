package processor

import (
	"fmt"

	"github.com/free5gc/nwdaf/internal/anlf"
)

func (p *Processor) HandleAnalyticsReport(
	subscriptionID string,
	report *anlf.AnalyticsReport,
) error {
	if p.reportDispatcher == nil {
		return fmt.Errorf("analytics report dispatcher is not configured")
	}
	return p.reportDispatcher.DispatchAnalyticsReport(subscriptionID, report)
}
