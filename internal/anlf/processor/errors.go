package processor

import "errors"

var (
	ErrSubscriptionNotFound    = errors.New("subscription not found")
	ErrStaleAnalyticsReport    = errors.New("stale analytics report")
	ErrInvalidAnalyticsReport  = errors.New("invalid analytics report")
	ErrExternalDelivery        = errors.New("external analytics delivery failed")
	ErrAnalyticsReportInFlight = errors.New("analytics report delivery in progress")
)

type reportOutcomeError struct {
	outcome error
	cause   error
}

func (e *reportOutcomeError) Error() string {
	return e.cause.Error()
}

func (e *reportOutcomeError) Unwrap() []error {
	return []error{e.outcome, e.cause}
}

func preserveReportOutcome(outcome, cause error) error {
	if cause == nil {
		return nil
	}
	return &reportOutcomeError{outcome: outcome, cause: cause}
}
