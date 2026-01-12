// Package notifier provides subscription notification functionality for NWDAF
package notifier

import (
	"time"

	"github.com/free5gc/openapi/models"
)

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

// generateMockUeCommunication generates mock data for UE Communication analytics
// Per YAML spec: commDur, trafChar, ts are REQUIRED. sessInactTimer is OPTIONAL.
// sessInactTimer is included ONLY when N4_SESS_INACT_TIMER_FOR_UE_COMM is in listOfAnaSubsets.
func generateMockUeCommunication(eventSub *models.NwdafEventsSubscriptionEventSubscription) models.UeCommunication {
	now := time.Now()

	ueComm := models.UeCommunication{
		CommDur: int32(300), // 5 minutes communication duration
		Ts:      &now,
		TrafChar: &models.TrafficCharacterization{
			Dnn:   "internet",
			UlVol: 1024000, // 1MB uplink
			DlVol: 5120000, // 5MB downlink
		},
		Confidence: 90,
	}

	// Conditionally include sessInactTimer based on requested subsets
	for _, subset := range eventSub.ListOfAnaSubsets {
		if subset == models.AnalyticsSubset_N4_SESS_INACT_TIMER_FOR_UE_COMM {
			ueComm.SessInactTimer = &models.SessInactTimerForUeComm{
				N4SessId:          1,
				SessInactiveTimer: 120, // 2 minutes inactivity timer
			}
			break
		}
	}

	return ueComm
}
