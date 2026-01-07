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
	default:
		return nil
	}
}
