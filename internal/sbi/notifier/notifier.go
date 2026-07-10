// Package notifier delivers standard SBI analytics notifications.
package notifier

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
)

type predictionRecorder interface {
	RecordAnalyticsReport(subscriptionID string, report *contract.AnalyticsReport)
}

var (
	ErrSubscriptionNotFound    = errors.New("subscription not found")
	ErrStaleAnalyticsReport    = errors.New("stale analytics report")
	ErrInvalidAnalyticsReport  = errors.New("invalid analytics report")
	ErrExternalDelivery        = errors.New("external analytics delivery failed")
	ErrAnalyticsReportInFlight = errors.New("analytics report delivery in progress")
)

type ReportDispatcher struct {
	baseCtx  context.Context
	client   *http.Client
	recorder predictionRecorder
}

func NewReportDispatcher(baseCtx context.Context, recorder predictionRecorder) *ReportDispatcher {
	return &ReportDispatcher{
		baseCtx:  baseCtx,
		client:   &http.Client{Timeout: 10 * time.Second},
		recorder: recorder,
	}
}
