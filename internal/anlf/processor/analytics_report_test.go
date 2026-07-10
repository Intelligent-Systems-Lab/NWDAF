package processor

import (
	"errors"
	"fmt"
	"testing"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	"github.com/free5gc/nwdaf/internal/sbi/notifier"
)

type analyticsReportDispatcherStub struct {
	err error
}

func TestHandleAnalyticsReportPreservesDispatcherErrorDetail(t *testing.T) {
	dispatchErr := fmt.Errorf("%w: unsupported event %q", notifier.ErrInvalidAnalyticsReport, "NF_LOAD")
	processor := NewProcessor(nil, analyticsReportDispatcherStub{err: dispatchErr})

	err := processor.HandleAnalyticsReport("sub-1", &contract.AnalyticsReport{})
	if !errors.Is(err, ErrInvalidAnalyticsReport) {
		t.Fatalf("HandleAnalyticsReport() error = %v, want invalid report outcome", err)
	}
	if err.Error() != dispatchErr.Error() {
		t.Fatalf("error detail = %q, want %q", err.Error(), dispatchErr.Error())
	}
}

func (s analyticsReportDispatcherStub) DispatchAnalyticsReport(
	string,
	*contract.AnalyticsReport,
) error {
	return s.err
}

func TestHandleAnalyticsReportMapsDispatcherErrors(t *testing.T) {
	tests := []struct {
		name string
		in   error
		want error
	}{
		{name: "not found", in: notifier.ErrSubscriptionNotFound, want: ErrSubscriptionNotFound},
		{name: "stale", in: notifier.ErrStaleAnalyticsReport, want: ErrStaleAnalyticsReport},
		{name: "invalid", in: notifier.ErrInvalidAnalyticsReport, want: ErrInvalidAnalyticsReport},
		{name: "in flight", in: notifier.ErrAnalyticsReportInFlight, want: ErrAnalyticsReportInFlight},
		{name: "delivery", in: notifier.ErrExternalDelivery, want: ErrExternalDelivery},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			processor := NewProcessor(nil, analyticsReportDispatcherStub{err: test.in})
			err := processor.HandleAnalyticsReport("sub-1", &contract.AnalyticsReport{})
			if !errors.Is(err, test.want) {
				t.Fatalf("HandleAnalyticsReport() error = %v, want %v", err, test.want)
			}
		})
	}
}
