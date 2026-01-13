// Package collector provides data collection management for NWDAF
package collector

import (
	"fmt"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

// DataCollectionManager manages data collection from source NFs (SMF, AMF, etc.)
// Per 3GPP TS 23.288 §6.2: NWDAF invokes Nnf_EventExposure_Subscribe to collect data
type DataCollectionManager struct {
	collectorCtx *CollectorContext
	config       *factory.DataCollection
	nwdafUri     string
}

// NewDataCollectionManager creates a new DataCollectionManager
func NewDataCollectionManager() *DataCollectionManager {
	cfg := factory.NwdafConfig
	if cfg == nil || cfg.Configuration == nil {
		return nil
	}

	var nwdafUri string
	if cfg.Configuration.Sbi != nil {
		nwdafUri = fmt.Sprintf("%s://%s:%d/collector/notify",
			cfg.Configuration.Sbi.Scheme,
			cfg.Configuration.Sbi.BindingIPv4,
			cfg.Configuration.Sbi.Port)
	}

	return &DataCollectionManager{
		collectorCtx: GetSelf(),
		config:       cfg.Configuration.DataCollection,
		nwdafUri:     nwdafUri,
	}
}

// TriggerForSubscription triggers data collection based on consumer subscription
// Called when a consumer subscribes to NWDAF for analytics
func (m *DataCollectionManager) TriggerForSubscription(
	eventSubs []models.NwdafEventsSubscriptionEventSubscription,
	consumerSubId string,
) {
	if m == nil || m.config == nil {
		logger.CollectorLog.Debugf("Data collection not configured, skipping")
		return
	}

	for _, eventSub := range eventSubs {
		switch eventSub.Event {
		case models.NwdafEvent_UE_COMMUNICATION:
			m.triggerUeCommunication(&eventSub, consumerSubId)
		// Future: Add other event types here
		// case models.NwdafEvent_ABNORMAL_BEHAVIOUR:
		// case models.NwdafEvent_UE_MOBILITY:
		default:
			logger.CollectorLog.Debugf("No data collection needed for event: %s", eventSub.Event)
		}
	}
}

// triggerUeCommunication handles data collection for UE_COMMUNICATION analytics
func (m *DataCollectionManager) triggerUeCommunication(
	eventSub *models.NwdafEventsSubscriptionEventSubscription,
	consumerSubId string,
) {
	// Check SMF configuration
	smfConfig := m.config.Smf
	if smfConfig == nil || !smfConfig.Enabled || len(smfConfig.Endpoints) == 0 {
		logger.CollectorLog.Debugf("SMF data collection not enabled or no endpoints configured")
		return
	}

	// Extract target UEs from subscription
	if eventSub.TgtUe == nil {
		logger.CollectorLog.Warnf("UE_COMMUNICATION: No target UE specified, skipping SMF subscription")
		return
	}

	supis := eventSub.TgtUe.Supis
	if len(supis) == 0 {
		// For group subscriptions, would need to resolve group members via UDM
		logger.CollectorLog.Warnf("UE_COMMUNICATION: No SUPIs in target UE, skipping SMF subscription")
		return
	}

	// Subscribe to SMF for each target UE
	for _, supi := range supis {
		// Use the first available SMF endpoint (in production, would use NRF discovery)
		smfEndpoint := smfConfig.Endpoints[0]

		logger.CollectorLog.Infof("Subscribing to SMF for UE Communication: supi=%s, smf=%s, consumerSub=%s",
			supi, smfEndpoint, consumerSubId)

		// Trigger async SMF subscription (non-blocking)
		go m.subscribeToSmf(smfEndpoint, supi)
	}
}

// subscribeToSmf performs the actual SMF subscription
func (m *DataCollectionManager) subscribeToSmf(smfEndpoint, supi string) {
	subId, err := m.collectorCtx.SubscribeForUeCommunication(smfEndpoint, supi, m.nwdafUri)
	if err != nil {
		logger.CollectorLog.Errorf("Failed to subscribe to SMF for %s: %v", supi, err)
		return
	}
	logger.CollectorLog.Infof("SMF subscription created: smfSubId=%s, supi=%s", subId, supi)
}
