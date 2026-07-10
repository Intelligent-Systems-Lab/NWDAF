package processor

import (
	"errors"
	"fmt"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	"github.com/free5gc/nwdaf/internal/sbi/notifier"
)

func (p *Processor) HandleAnalyticsReport(
	subscriptionID string,
	report *contract.AnalyticsReport,
) error {
	if p.reportDispatcher == nil {
		return fmt.Errorf("analytics report dispatcher is not configured")
	}
	err := p.reportDispatcher.DispatchAnalyticsReport(subscriptionID, report)
	switch {
	case errors.Is(err, notifier.ErrSubscriptionNotFound):
		return preserveReportOutcome(ErrSubscriptionNotFound, err)
	case errors.Is(err, notifier.ErrStaleAnalyticsReport):
		return preserveReportOutcome(ErrStaleAnalyticsReport, err)
	case errors.Is(err, notifier.ErrInvalidAnalyticsReport):
		return preserveReportOutcome(ErrInvalidAnalyticsReport, err)
	case errors.Is(err, notifier.ErrAnalyticsReportInFlight):
		return preserveReportOutcome(ErrAnalyticsReportInFlight, err)
	case errors.Is(err, notifier.ErrExternalDelivery):
		return preserveReportOutcome(ErrExternalDelivery, err)
	default:
		return err
	}
}
