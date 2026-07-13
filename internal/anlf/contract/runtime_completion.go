package contract

import (
	"fmt"
	"strings"
	"time"
)

type RuntimeCompletionReason string

const (
	RuntimeCompletionMaxReportsReached      RuntimeCompletionReason = "MAX_REPORTS_REACHED"
	RuntimeCompletionMonitoringDurationDone RuntimeCompletionReason = "MONITORING_DURATION_EXPIRED"
)

type RuntimeCompletionEvent struct {
	CompletionID       string                  `json:"completion_id"`
	SubscriptionID     string                  `json:"subscription_id"`
	RuntimeRevision    int64                   `json:"runtime_revision"`
	Reason             RuntimeCompletionReason `json:"reason"`
	CompletedAt        time.Time               `json:"completed_at"`
	LastReportSequence int64                   `json:"last_report_sequence"`
}

func (e *RuntimeCompletionEvent) Validate() error {
	if e == nil || strings.TrimSpace(e.CompletionID) == "" || strings.TrimSpace(e.SubscriptionID) == "" {
		return fmt.Errorf("completion and subscription identity are required")
	}
	if e.RuntimeRevision <= 0 {
		return fmt.Errorf("runtime revision must be greater than zero")
	}
	if e.CompletedAt.IsZero() {
		return fmt.Errorf("completion timestamp is required")
	}
	if e.LastReportSequence < 0 {
		return fmt.Errorf("last report sequence must not be negative")
	}
	switch e.Reason {
	case RuntimeCompletionMaxReportsReached, RuntimeCompletionMonitoringDurationDone:
		return nil
	default:
		return fmt.Errorf("unsupported runtime completion reason %q", e.Reason)
	}
}
