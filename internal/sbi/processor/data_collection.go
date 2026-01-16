package processor

import (
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

// TriggerDataCollection triggers data collection subscriptions for source NFs
// Per 3GPP TS 23.288 §6.2: NWDAF invokes Nnf_EventExposure_Subscribe to collect data
func (p *Processor) TriggerDataCollection(
	eventSubs []models.NwdafEventsSubscriptionEventSubscription,
	evtReq *models.ReportingInformation,
	subscriptionId string,
) {
	for _, eventSub := range eventSubs {
		switch eventSub.Event {
		case models.NwdafEvent_UE_COMMUNICATION:
			p.triggerUeCommunicationCollection(&eventSub, evtReq, subscriptionId)
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
	evtReq *models.ReportingInformation,
	subscriptionId string,
) {
	// Check if SMF data collection is enabled in config
	cfg := factory.NwdafConfig
	if cfg == nil || cfg.Configuration == nil || cfg.Configuration.DataCollection == nil ||
		cfg.Configuration.DataCollection.Smf == nil || !cfg.Configuration.DataCollection.Smf.Enabled {
		logger.ProcLog.Debugf("SMF data collection is disabled in config")
		return
	}

	smfConfig := cfg.Configuration.DataCollection.Smf
	if len(smfConfig.Endpoints) == 0 {
		logger.ProcLog.Warnf("SMF data collection enabled but no endpoints configured")
		return
	}

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

	// Get notification URIs from config
	smfNotifUri := "http://localhost:8080/collector/notify"
	upfNotifUri := "http://localhost:8080/collector/upf-notify"
	if smfConfig.NotifUris != nil {
		if smfConfig.NotifUris.Smf != "" {
			smfNotifUri = smfConfig.NotifUris.Smf
		}
		if smfConfig.NotifUris.Upf != "" {
			upfNotifUri = smfConfig.NotifUris.Upf
		}
	}

	// Calculate SMF report period (slightly faster than consumer)
	// Use 75% of consumer period to ensure data freshness
	var smfRepPeriod int32 = 10 // Default: 10s
	if evtReq != nil && evtReq.RepPeriod > 0 {
		smfRepPeriod = evtReq.RepPeriod * 3 / 4 // 75%
		if smfRepPeriod < 5 {
			smfRepPeriod = 5 // Minimum 5 seconds
		}
	}

	// Subscribe to each SMF endpoint for each target SUPI
	for _, smfEndpoint := range smfConfig.Endpoints {
		for _, supi := range supis {
			subId, err := consumer.SubscribeForUeCommunication(
				smfEndpoint, supi, smfNotifUri, upfNotifUri, smfRepPeriod,
			)
			if err != nil {
				logger.ProcLog.Errorf("Failed to subscribe SMF for SUPI %s: %v", supi, err)
				continue
			}
			logger.ProcLog.Infof("SMF subscription created: supi=%s, subId=%s, endpoint=%s", supi, subId, smfEndpoint)
		}
	}
}
