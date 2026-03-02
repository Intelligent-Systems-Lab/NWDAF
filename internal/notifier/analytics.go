// Package notifier provides subscription notification functionality for NWDAF
package notifier

import (
	"fmt"
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

var notifierLog = logger.NotifierLog

// TrafficCharacterization for ML prediction request
type TrafficCharacterization = consumer.TrafficCharacterization

// TrafficObservation for ML prediction request
type TrafficObservation = consumer.TrafficObservation

// getMlServiceClient returns a new ML service client if configured
func getMlServiceClient() *consumer.MlServiceClient {
	// Get ML service configuration
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

// generateMockAbnormalBehaviours generates mock DDoS detection analytics data
// TODO: Replace with real analytics from ML model and data collection
func generateMockAbnormalBehaviours() []models.AbnormalBehaviour {
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

// generateUeCommunicationAnalytics generates UE Communication analytics
// Per TS 23.288 §6.7.3: Analytics based on collected UPF traffic data
// Uses ML-based prediction if model is ready, returns 0 confidence when insufficient resources
func generateUeCommunicationAnalytics(nwdafSubId string) models.UeCommunication {
	ctx := nwdaf_context.GetSelf()
	now := time.Now()

	// Check if ML model is available for this subscription
	mlInfo := ctx.GetMlModelInfo(nwdafSubId)
	if mlInfo != nil && mlInfo.IsReady() {
		result, err := generateMlBasedUeCommunication(nwdafSubId, mlInfo, ctx)
		if err == nil {
			notifierLog.Infof("Using ML-based analytics for subscription %s", nwdafSubId)
			return result
		}
		notifierLog.Warnf("ML prediction failed for %s: %v", nwdafSubId, err)
	}

	// Per TS 23.288: Return 0 confidence when insufficient resources for analytics
	notifierLog.Debugf("Insufficient resources for ML analytics, returning 0 confidence for %s", nwdafSubId)
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

	// Fetch historical data — prefer MongoDB, fall back to in-memory
	historicalData, dnn := fetchHistoricalData(nwdafSubId, ctx, params)

	// Call ML service for prediction
	modelId := mlInfo.GetModelId()
	resp, err := mlClient.Predict(modelId, historicalData, outputWindow)
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
					// Target: i-th step ahead in sampling-interval increments
					TargetTime: now.Add(time.Duration((i+1)*samplingInterval) * time.Second),
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

// fetchHistoricalData retrieves recent UPF traffic data for ML prediction.
// Primary source: MongoDB time-series collection (time-aligned, bounded query).
// Fallback: in-memory RawUpfData (available even when MongoDB is not configured).
func fetchHistoricalData(
	nwdafSubId string,
	ctx *nwdaf_context.NWDAFContext,
	params *factory.ModelParams,
) ([]TrafficObservation, string) {
	cfg := factory.NwdafConfig
	dbName := ""
	if cfg != nil && cfg.Configuration != nil && cfg.Configuration.Mongodb != nil {
		dbName = cfg.Configuration.Mongodb.Name
	}

	inputWindow := params.InputWindowOrDefault()
	queryLookback := time.Duration(params.QueryLookback()) * time.Second

	// --- Primary: MongoDB ---
	if dbName != "" && nwdaf_context.IsMongoAvailable() {
		corrIds := ctx.GetCorrelationIdsByNwdafSubId(nwdafSubId)
		if len(corrIds) > 0 {
			since := time.Now().Add(-queryLookback)
			records, err := nwdaf_context.QueryTrafficByMultipleCorrelationIds(
				dbName, corrIds, since, inputWindow,
			)
			if err == nil && len(records) > 0 {
				notifierLog.Debugf("Using %d MongoDB records for ML prediction (nwdafSubId=%s)",
					len(records), nwdafSubId)
				return upfRecordsToObservations(records)
			}
			if err != nil {
				notifierLog.Warnf(
					"MongoDB query failed for ML prediction, falling back to in-memory: %v", err)
			}
		}
	}

	// --- Fallback: in-memory ---
	notifierLog.Debugf("Using in-memory data for ML prediction (nwdafSubId=%s)", nwdafSubId)
	return inMemoryToObservations(ctx, nwdafSubId, inputWindow)
}

// upfRecordsToObservations converts MongoDB UpfTrafficRecord slice to ML input format.
func upfRecordsToObservations(records []nwdaf_context.UpfTrafficRecord) ([]TrafficObservation, string) {
	obs := make([]TrafficObservation, 0, len(records))
	dnn := "internet"
	for _, r := range records {
		obs = append(obs, TrafficObservation{
			Ts: r.Timestamp.Format(time.RFC3339),
			TrafChar: TrafficCharacterization{
				UlVol: r.UlVolume,
				DlVol: r.DlVolume,
			},
		})
		if r.Metadata.Dnn != "" {
			dnn = r.Metadata.Dnn
		}
	}
	return obs, dnn
}

// inMemoryToObservations reads from in-memory store (fallback when MongoDB unavailable).
func inMemoryToObservations(
	ctx *nwdaf_context.NWDAFContext,
	nwdafSubId string,
	inputWindow int,
) ([]TrafficObservation, string) {
	trafficDataList := ctx.GetTrafficDataByNwdafSubId(nwdafSubId)
	obs := make([]TrafficObservation, 0)
	dnn := "internet"

	for _, trafficData := range trafficDataList {
		trafficData.Lock()
		for _, dp := range trafficData.RawUpfData {
			obs = append(obs, TrafficObservation{
				Ts: dp.Timestamp.Format(time.RFC3339),
				TrafChar: TrafficCharacterization{
					UlVol: dp.UlVolume,
					DlVol: dp.DlVolume,
				},
			})
		}
		if trafficData.Dnn != "" {
			dnn = trafficData.Dnn
		}
		trafficData.Unlock()
	}

	// Trim to inputWindow (most recent points)
	if len(obs) > inputWindow {
		obs = obs[len(obs)-inputWindow:]
	}
	return obs, dnn
}

// isAccuracyMonitorEnabled checks if accuracy monitoring is configured and enabled
func isAccuracyMonitorEnabled() bool {
	cfg := factory.NwdafConfig
	return cfg != nil && cfg.Configuration != nil &&
		cfg.Configuration.Mtlf != nil && cfg.Configuration.Mtlf.Enabled &&
		cfg.Configuration.Mtlf.AccuracyMonitor != nil && cfg.Configuration.Mtlf.AccuracyMonitor.Enabled
}
