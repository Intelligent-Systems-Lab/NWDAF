package processor

import (
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/openapi/models"
)

// TriggerDataCollection triggers data collection subscriptions for source NFs
// Per 3GPP TS 23.288 §6.2: NWDAF invokes Nnf_EventExposure_Subscribe to collect data
func (p *Processor) TriggerDataCollection(
	eventSubs []models.NwdafEventsSubscriptionEventSubscription,
	subscriptionId string,
) {
	for _, eventSub := range eventSubs {
		switch eventSub.Event {
		case models.NwdafEvent_UE_COMMUNICATION:
			p.triggerUeCommunicationCollection(&eventSub, subscriptionId)
		case models.NwdafEvent_ABNORMAL_BEHAVIOUR:
			// Currently not supported - skip
			logger.ProcLog.Debugf("ABNORMAL_BEHAVIOUR data collection not implemented")
		default:
			logger.ProcLog.Debugf("Data collection not implemented for event: %s", eventSub.Event)
		}
	}
}

// triggerUeCommunicationCollection subscribes to SMF for UE communication data
// Per TS 23.288: NWDAF subscribes to SMF via Nsmf_EventExposure for UE communication analytics
func (p *Processor) triggerUeCommunicationCollection(
	eventSub *models.NwdafEventsSubscriptionEventSubscription,
	subscriptionId string,
) {
	if eventSub.TgtUe == nil {
		logger.ProcLog.Warnf("TgtUe is nil, cannot trigger data collection")
		return
	}

	// Extract target SUPIs
	supis := eventSub.TgtUe.Supis
	if len(supis) == 0 && len(eventSub.TgtUe.IntGroupIds) > 0 {
		// TODO: Resolve intGroupIds to SUPIs via UDM
		logger.ProcLog.Infof("IntGroupIds data collection not yet implemented: %v", eventSub.TgtUe.IntGroupIds)
		return
	}

	if len(supis) == 0 {
		logger.ProcLog.Warnf("No target SUPIs for data collection")
		return
	}

	// Get consumer for SMF subscription
	consumer := p.nwdaf.Consumer()
	if consumer == nil {
		logger.ProcLog.Warnf("Consumer not available for data collection")
		return
	}

	// TODO: Get SMF endpoint from NRF discovery or configuration
	// smfEndpoint := "http://localhost:8081"
	// smfNotifUri := "http://localhost:8080/collector/notify"
	// upfNotifUri := "http://localhost:8080/collector/upf-notify"

	logger.ProcLog.Infof("Data collection trigger prepared for subscription %s (SMF connection pending)", subscriptionId)
	logger.ProcLog.Debugf("Target SUPIs for collection: %v", supis)

	// TODO: Enable when SMF is available
	// for _, supi := range supis {
	//     subId, err := consumer.SubscribeForUeCommunication(
	//         smfEndpoint, supi, smfNotifUri, upfNotifUri,
	//     )
	//     if err != nil {
	//         logger.ProcLog.Errorf("Failed to subscribe SMF for SUPI %s: %v", supi, err)
	//         continue
	//     }
	//     logger.ProcLog.Infof("SMF subscription created: supi=%s, subId=%s", supi, subId)
	// }

	// Suppress unused variable warning
	_ = consumer
}
