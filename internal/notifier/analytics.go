// Package notifier provides subscription notification functionality for NWDAF
package notifier

import (
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/openapi/models"
)

var notifierLog = logger.NotifierLog

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

// generateUeCommunicationAnalytics generates UE Communication analytics using rule-based logic
// Per TS 23.288 §6.7.3: Analytics based on collected UPF traffic data
// Uses unified query: nwdafSubId → correlationIds → TrafficDataBuckets
func generateUeCommunicationAnalytics(nwdafSubId string) models.UeCommunication {
	now := time.Now()
	ctx := nwdaf_context.GetSelf()

	// Default values
	var ulVol, dlVol int64 = 0, 0
	var commDur int32 = 60 // Default 1 minute
	var confidence int32 = 50
	dnn := "internet"

	// Track collected data points
	dataPointCount := 0

	// Get all traffic data for this NWDAF subscription
	// Unified query: nwdafSubId → correlationIds → buckets → data
	trafficDataList := ctx.GetTrafficDataByNwdafSubId(nwdafSubId)

	for _, trafficData := range trafficDataList {
		trafficData.Lock()

		// Aggregate from raw data points
		for _, dp := range trafficData.RawUpfData {
			ulVol += dp.UlVolume
			dlVol += dp.DlVolume
		}
		dataPointCount += len(trafficData.RawUpfData)

		// Calculate communication duration from timestamps
		if !trafficData.CreatedAt.IsZero() && !trafficData.LastUpdate.IsZero() {
			duration := int32(trafficData.LastUpdate.Sub(trafficData.CreatedAt).Seconds())
			if duration > commDur {
				commDur = duration
			}
		}

		// Use DNN from data if available
		if trafficData.Dnn != "" {
			dnn = trafficData.Dnn
		}

		trafficData.Unlock()
		notifierLog.Debugf("Using collected data (ip=%s): ulVol=%d, dlVol=%d",
			trafficData.IpAddress, ulVol, dlVol)
	}

	// Rule-based confidence calculation
	if ulVol == 0 && dlVol == 0 {
		confidence = 0
		notifierLog.Debugf("No collected data for nwdafSubId=%s, confidence=0", nwdafSubId)
	} else {
		confidence = calculateConfidence(dataPointCount, ulVol, dlVol)
	}

	return models.UeCommunication{
		CommDur: commDur,
		Ts:      &now,
		TrafChar: &models.TrafficCharacterization{
			Dnn:   dnn,
			UlVol: ulVol,
			DlVol: dlVol,
		},
		Confidence: confidence,
	}
}

// calculateConfidence computes confidence score based on data quality
// Per TS 23.288 §6.7.3.3: Confidence indicates prediction reliability
func calculateConfidence(dataPointCount int, ulVol, dlVol int64) int32 {
	// Base confidence starts at 50
	confidence := int32(50)

	// More data points = higher confidence
	if dataPointCount >= 5 {
		confidence += 20
	} else if dataPointCount >= 2 {
		confidence += 10
	} else if dataPointCount >= 1 {
		confidence += 5
	}

	// Having actual traffic data increases confidence
	if ulVol > 0 || dlVol > 0 {
		confidence += 10
	}

	// Significant traffic volume indicates active usage
	totalVol := ulVol + dlVol
	if totalVol > 10*1024*1024 { // > 10MB
		confidence += 10
	} else if totalVol > 1*1024*1024 { // > 1MB
		confidence += 5
	}

	// Cap at 100
	if confidence > 100 {
		confidence = 100
	}

	return confidence
}
