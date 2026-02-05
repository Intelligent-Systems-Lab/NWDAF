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
	cfg := factory.NwdafConfig
	if cfg == nil || cfg.Configuration == nil || cfg.Configuration.DataCollection == nil ||
		cfg.Configuration.DataCollection.MlService == nil || !cfg.Configuration.DataCollection.MlService.Enabled {
		return nil
	}

	endpoint := cfg.Configuration.DataCollection.MlService.Endpoint
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

	// Prepare historical data for prediction
	trafficDataList := ctx.GetTrafficDataByNwdafSubId(nwdafSubId)
	historicalData := []TrafficObservation{}
	dnn := "internet"

	for _, trafficData := range trafficDataList {
		trafficData.Lock()
		for _, dp := range trafficData.RawUpfData {
			historicalData = append(historicalData, TrafficObservation{
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

	// Call ML service for prediction
	modelId := mlInfo.GetModelId()
	resp, err := mlClient.Predict(modelId, historicalData, 1)
	if err != nil {
		return models.UeCommunication{}, err
	}

	if len(resp.PredictedData) == 0 {
		return models.UeCommunication{}, fmt.Errorf("no prediction data returned")
	}

	pred := resp.PredictedData[0]
	return models.UeCommunication{
		CommDur: 60,
		Ts:      &now,
		TrafChar: &models.TrafficCharacterization{
			Dnn:   dnn,
			UlVol: pred.TrafChar.UlVol,
			DlVol: pred.TrafChar.DlVol,
		},
		Confidence: pred.Confidence,
	}, nil
}
