// Package anlf provides the AnLF inference pipeline for NWDAF analytics.
package anlf

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

var anlfLog = logger.AnlfLog

// errNoHistoricalData is returned when no UPF traffic data is available yet.
// This is expected during startup before SMF delivers the first measurements.
var errNoHistoricalData = fmt.Errorf("no historical data available yet")

// TrafficObservation for ML prediction request
type TrafficObservation = consumer.TrafficObservation

// getMlServiceClient returns a new ML service client if configured
func getMlServiceClient() *consumer.MlServiceClient {
	cfg := factory.NwdafConfig
	if cfg == nil || cfg.Configuration == nil ||
		cfg.Configuration.MlService == nil || !cfg.Configuration.MlService.Enabled {
		return nil
	}

	endpoint := cfg.Configuration.MlService.Endpoint
	if endpoint == "" {
		return nil
	}

	return consumer.NewMlServiceClient(endpoint)
}

// GenerateMockAbnormalBehaviours generates mock DDoS detection analytics data
// TODO: Replace with real analytics from ML model and data collection
func GenerateMockAbnormalBehaviours() []models.AbnormalBehaviour {
	now := time.Now()

	return []models.AbnormalBehaviour{
		{
			Excep: &models.Exception{
				ExcepId:    models.ExceptionId_SUSPICION_OF_DDOS_ATTACK,
				ExcepLevel: 3,
				ExcepTrend: models.ExceptionTrend_UP,
			},
			Supis:      []string{"imsi-123456789012345"},
			Dnn:        "internet",
			Ratio:      15, // 15% abnormal ratio
			Confidence: 85, // 85% confidence
			AddtMeasInfo: &models.AdditionalMeasurement{
				DdosAttack: &models.AddressList{
					Ipv4Addrs: []string{"192.168.1.100", "192.168.1.101"},
				},
				Circums: []models.CircumstanceDescription{
					{
						Tm: &now,
					},
				},
			},
		},
	}
}

// GenerateUeCommunicationAnalytics generates UE Communication analytics
// Per TS 23.288 §6.7.3: Analytics based on collected UPF traffic data
// Uses ML-based prediction if model is ready, returns 0 confidence when insufficient resources
func GenerateUeCommunicationAnalytics(nwdafSubId string) models.UeCommunication {
	ctx := nwdaf_context.GetSelf()
	now := time.Now()

	// Check if ML model is available for this subscription
	mlInfo := ctx.GetMlModelInfo(nwdafSubId)
	if mlInfo != nil && mlInfo.IsReady() {
		result, err := generateMlBasedUeCommunication(nwdafSubId, mlInfo, ctx)
		if err == nil {
			return result
		}
		if err == errNoHistoricalData {
			anlfLog.Debugf("ML prediction skipped for %s: %v", nwdafSubId, err)
		} else {
			anlfLog.Warnf("ML prediction failed for %s: %v", nwdafSubId, err)
		}
	}

	// Per TS 23.288: Return 0 confidence when insufficient resources for analytics
	anlfLog.Debugf("Insufficient resources for ML analytics, returning 0 confidence for %s", nwdafSubId)
	return models.UeCommunication{
		CommDur:    0,
		Ts:         &now,
		TrafChar:   &models.TrafficCharacterization{Dnn: "internet"},
		Confidence: 0,
	}
}

// generateMlBasedUeCommunication uses ML service for prediction
func generateMlBasedUeCommunication(
	nwdafSubId string,
	mlInfo *nwdaf_context.MlModelInfo,
	ctx *nwdaf_context.NWDAFContext,
) (models.UeCommunication, error) {
	now := time.Now()

	// Get ML service client
	mlClient := getMlServiceClient()
	if mlClient == nil {
		return models.UeCommunication{}, fmt.Errorf("ML service client not available")
	}

	// Resolve model params (with defaults)
	params := getUeCommunicationModelParams()
	outputWindow := params.OutputWindowOrDefault()
	samplingInterval := params.SamplingIntervalOrDefault()

	// Snap now to the current period boundary so TargetTime aligns with UPF startTime,
	// and so fetchHistoricalData can derive clean output timestamps.
	si64 := int64(samplingInterval)
	snappedNow := time.Unix((now.Unix()/si64)*si64, 0)

	// Fetch historical data — prefer MongoDB, fall back to in-memory
	historicalData, dnn := fetchHistoricalData(nwdafSubId, ctx, params, snappedNow)
	if len(historicalData) == 0 {
		return models.UeCommunication{}, errNoHistoricalData
	}

	// Call ML service for prediction
	modelId := mlInfo.GetModelId()
	resp, err := mlClient.Predict(modelId, historicalData)
	if err != nil {
		return models.UeCommunication{}, err
	}

	if len(resp.PredictedData) == 0 {
		return models.UeCommunication{}, fmt.Errorf("no prediction data returned")
	}

	// Aggregate predicted steps
	var totalUl, totalDl int64
	var totalConfidence int32
	for i, pred := range resp.PredictedData {
		totalUl += pred.TrafChar.UlVol
		totalDl += pred.TrafChar.DlVol
		totalConfidence += pred.Confidence

		// Record individual predictions for accuracy monitoring
		// Per TS 23.288 §5C: store predictions for ground truth comparison
		if isAccuracyMonitorEnabled() {
			store := ctx.GetModelAccuracyStore(mlInfo.ModelUrl)
			if store != nil {
				store.AddPrediction(nwdaf_context.PredictionRecord{
					ModelUrl:    mlInfo.ModelUrl,
					PredictedAt: now,
					// Target: i-th step ahead from the snapped period boundary,
					// so TargetTime always aligns with UPF period startTime.
					TargetTime: snappedNow.Add(time.Duration((i+1)*samplingInterval) * time.Second),
					PredUlVol:  pred.TrafChar.UlVol,
					PredDlVol:  pred.TrafChar.DlVol,
					NwdafSubId: nwdafSubId,
				})
			}
		}
	}
	avgConfidence := totalConfidence / int32(len(resp.PredictedData))

	// commDur = total prediction horizon in seconds
	commDur := int32(outputWindow * samplingInterval)

	anlfLog.Infof("ML inference: sub=%s steps=%d ulVol=%d dlVol=%d confidence=%d commDur=%ds",
		nwdafSubId, len(resp.PredictedData), totalUl, totalDl, avgConfidence, commDur)

	return models.UeCommunication{
		CommDur: commDur,
		Ts:      &now,
		TrafChar: &models.TrafficCharacterization{
			Dnn:   dnn,
			UlVol: totalUl,
			DlVol: totalDl,
		},
		Confidence: avgConfidence,
	}, nil
}

// getUeCommunicationModelParams returns the configured ModelParams for UE_COMMUNICATION.
// Returns a zero-value ModelParams (all defaults) when not configured.
func getUeCommunicationModelParams() *factory.ModelParams {
	cfg := factory.NwdafConfig
	if cfg != nil && cfg.Configuration != nil &&
		cfg.Configuration.Analytics != nil &&
		cfg.Configuration.Analytics.UeCommunication != nil {
		return cfg.Configuration.Analytics.UeCommunication
	}
	return &factory.ModelParams{} // zero value → all helpers return defaults
}

// trafficPoint holds the numeric fields of a single UPF measurement.
// Used as the common intermediate type for sequence alignment.
type trafficPoint struct {
	TotalVol, UlVol, DlVol           float64
	TotalNbPkts, UlNbPkts, DlNbPkts  float64
	UlThr, DlThr, UlPktThr, DlPktThr float64
}

// fetchHistoricalData retrieves recent UPF traffic data for ML prediction from the
// in-memory ring buffer. The buffer is the sole source for inference; MongoDB is
// only consulted by the accuracy monitor for ground truth.
//
// Streams are grouped by correlationId, trimmed to the most recent inputWindow
// points, then zipped by sequence position (index from the end). Output Ts values
// are derived from snappedNow for perfect grid alignment.
func fetchHistoricalData(
	nwdafSubId string,
	ctx *nwdaf_context.NWDAFContext,
	params *factory.ModelParams,
	snappedNow time.Time,
) ([]TrafficObservation, string) {
	inputWindow := params.InputWindowOrDefault()
	samplingInterval := params.SamplingIntervalOrDefault()
	corrIds := ctx.GetCorrelationIdsByNwdafSubId(nwdafSubId)
	return alignAndZipInMemory(corrIds, ctx, inputWindow, samplingInterval, snappedNow)
}

// alignAndZipInMemory reads from the in-memory TrafficData store,
func alignAndZipInMemory(
	corrIds []string,
	ctx *nwdaf_context.NWDAFContext,
	inputWindow, samplingInterval int,
	snappedNow time.Time,
) ([]TrafficObservation, string) {
	dnn := "internet"
	streams := make([][]trafficPoint, 0, len(corrIds))

	for _, corrId := range corrIds {
		allData := ctx.GetAllTrafficDataForCorrelation(corrId)
		var pts []trafficPoint
		for _, td := range allData {
			td.Lock()
			if td.Dnn != "" {
				dnn = td.Dnn
			}
			// RawUpfData is appended in arrival order (ascending timestamp).
			for _, dp := range td.RawUpfData {
				pts = append(pts, trafficPoint{
					TotalVol:    float64(dp.TotalVolume),
					UlVol:       float64(dp.UlVolume),
					DlVol:       float64(dp.DlVolume),
					TotalNbPkts: float64(dp.TotalNbOfPackets),
					UlNbPkts:    float64(dp.UlNbOfPackets),
					DlNbPkts:    float64(dp.DlNbOfPackets),
					UlThr:       dp.UlThroughput,
					DlThr:       dp.DlThroughput,
					UlPktThr:    dp.UlPacketThroughput,
					DlPktThr:    dp.DlPacketThroughput,
				})
			}
			td.Unlock()
		}
		if len(pts) > inputWindow {
			pts = pts[len(pts)-inputWindow:]
		}
		streams = append(streams, pts)
	}

	return zipStreams(streams, inputWindow, samplingInterval, snappedNow), dnn
}

// zipStreams zips multiple per-corrId trafficPoint streams by sequence position from
// the end, sums values across streams at each position, and assigns derived Ts values.
// Streams shorter than outputLen are left-padded with zeros (no data → contributes 0).
// outputLen = min(inputWindow, maxStreamLen).
func zipStreams(
	streams [][]trafficPoint,
	inputWindow, samplingInterval int,
	snappedNow time.Time,
) []TrafficObservation {
	if len(streams) == 0 {
		return nil
	}
	maxLen := 0
	for _, s := range streams {
		if len(s) > maxLen {
			maxLen = len(s)
		}
	}
	outputLen := min(maxLen, inputWindow)
	if outputLen == 0 {
		return nil
	}

	counts := make([]string, outputLen)
	result := make([]TrafficObservation, outputLen)
	for pos := range outputLen {
		var agg trafficPoint
		contributing := 0
		for _, s := range streams {
			// Align from the end: pos 0 → oldest, pos outputLen-1 → most recent.
			// idx < 0 means this stream is shorter; it contributes 0 at this position.
			idx := len(s) - outputLen + pos
			if idx < 0 {
				continue
			}
			p := s[idx]
			agg.TotalVol += p.TotalVol
			agg.UlVol += p.UlVol
			agg.DlVol += p.DlVol
			agg.TotalNbPkts += p.TotalNbPkts
			agg.UlNbPkts += p.UlNbPkts
			agg.DlNbPkts += p.DlNbPkts
			agg.UlThr += p.UlThr
			agg.DlThr += p.DlThr
			agg.UlPktThr += p.UlPktThr
			agg.DlPktThr += p.DlPktThr
			contributing++
		}
		counts[pos] = strconv.Itoa(contributing)
		// Derive Ts from snappedNow: pos 0 is (outputLen-1) steps before snappedNow.
		ts := snappedNow.Add(time.Duration((pos-outputLen+1)*samplingInterval) * time.Second)
		result[pos] = TrafficObservation{
			Ts:          ts.UTC().Format(time.RFC3339),
			TotalVol:    agg.TotalVol,
			UlVol:       agg.UlVol,
			DlVol:       agg.DlVol,
			TotalNbPkts: agg.TotalNbPkts,
			UlNbPkts:    agg.UlNbPkts,
			DlNbPkts:    agg.DlNbPkts,
			UlThr:       agg.UlThr,
			DlThr:       agg.DlThr,
			UlPktThr:    agg.UlPktThr,
			DlPktThr:    agg.DlPktThr,
		}
	}
	anlfLog.Debugf("Aligned streams [streams=%d, outputLen=%d, contributors/pos]: %s",
		len(streams), outputLen, strings.Join(counts, ","))
	return result
}

// isAccuracyMonitorEnabled checks if accuracy monitoring is configured and enabled
func isAccuracyMonitorEnabled() bool {
	cfg := factory.NwdafConfig
	return cfg != nil && cfg.Configuration != nil &&
		cfg.Configuration.Mtlf != nil && cfg.Configuration.Mtlf.Enabled &&
		cfg.Configuration.Mtlf.AccuracyMonitor != nil && cfg.Configuration.Mtlf.AccuracyMonitor.Enabled
}
