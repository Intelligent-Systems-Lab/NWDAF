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

// GenerateAnalytics is a placeholder for future ML model integration
// Currently returns mock data
func GenerateAnalytics(eventSub *models.NwdafEventsSubscriptionEventSubscription) interface{} {
	switch eventSub.Event {
	case models.NwdafEvent_ABNORMAL_BEHAVIOUR:
		return generateMockAbnormalBehaviours()
	case models.NwdafEvent_UE_COMMUNICATION:
		return generateMockUeCommunication(eventSub)
	default:
		return nil
	}
}

// generateMockUeCommunication generates data for UE Communication analytics
// Per YAML spec: commDur, trafChar, ts are REQUIRED.
// Uses collected data from context when available, otherwise returns mock data.
func generateMockUeCommunication(eventSub *models.NwdafEventsSubscriptionEventSubscription) models.UeCommunication {
	now := time.Now()

	// Try to get collected data from context
	var ulVol, dlVol int64 = 1024000, 5120000 // Default mock values
	var commDur int32 = 300                   // Default 5 minutes

	// Check if we have collected data for target UEs
	if eventSub.TgtUe != nil && len(eventSub.TgtUe.Supis) > 0 {
		ctx := nwdaf_context.GetSelf()
		for _, supi := range eventSub.TgtUe.Supis {
			if ueData, ok := ctx.GetUeData(supi); ok {
				// Use real collected data
				ulVol = ueData.TotalUlVolume
				dlVol = ueData.TotalDlVolume
				if !ueData.StartTime.IsZero() && !ueData.LastUpdate.IsZero() {
					commDur = int32(ueData.LastUpdate.Sub(ueData.StartTime).Seconds())
				}
				notifierLog.Debugf("Using collected data for UE: %s", supi)
				break
			}
		}
	}

	return models.UeCommunication{
		CommDur: commDur,
		Ts:      &now,
		TrafChar: &models.TrafficCharacterization{
			Dnn:   "internet",
			UlVol: ulVol,
			DlVol: dlVol,
		},
		Confidence: 90,
	}
}
