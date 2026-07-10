package accuracy

import (
	"time"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/openapi/models"
)

// Report is the internal AnLF output for one monitor round and one scope.
// MTLF consumes these per-scope metrics for retrain policy evaluation.
type Report struct {
	ModelURL              string
	ScopeKey              string
	NwdafSubID            string
	Metrics               map[string]float64
	TrafficScale          float64
	PredictedTrafficScale float64
	SampleCount           int
	InferenceNum          int
	WindowStart           time.Time
	WindowEnd             time.Time
}

type AccuracyReport = Report

// RecordAnalyticsReport keeps the Phase 4 transitional accuracy correlation in Go.
func (m *Monitor) RecordAnalyticsReport(subscriptionID string, report *contract.AnalyticsReport) {
	if report == nil || !isAccuracyMonitorEnabled(m.config()) {
		return
	}
	ctx := nwdaf_context.GetSelf()
	mlInfo := ctx.GetMlModelInfo(subscriptionID)
	if mlInfo == nil || mlInfo.GetModelURL() == "" {
		return
	}
	store := ctx.GetModelAccuracyStore(mlInfo.GetModelURL())
	if store == nil {
		return
	}
	scopeKey, _ := resolveMonitoringScope(subscriptionID, ctx)
	for _, event := range report.EventNotifications {
		if event.Event != string(models.NwdafEvent_UE_COMMUNICATION) {
			continue
		}
		for _, communication := range event.UeCommunications {
			store.AddPrediction(nwdaf_context.PredictionRecord{
				ModelUrl:       mlInfo.GetModelURL(),
				PredictedAt:    report.GeneratedAt,
				TargetTime:     communication.Timestamp,
				TargetSlotTime: communication.Timestamp,
				PredUlVol:      communication.TrafficCharacterization.UplinkVolume,
				PredDlVol:      communication.TrafficCharacterization.DownlinkVolume,
				NwdafSubId:     subscriptionID,
				ScopeKey:       scopeKey,
			})
		}
	}
}
