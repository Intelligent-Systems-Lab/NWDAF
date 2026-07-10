package processor

import (
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/openapi/models"
)

// TriggerDataCollection triggers data collection subscriptions for source NFs
// Per 3GPP TS 23.288 §6.2: NWDAF invokes Nnf_EventExposure_Subscribe to collect data
func (p *Processor) TriggerDataCollection(
	eventSubs []models.NwdafEventsSubscriptionEventSubscription,
	subscriptionId string,
) {
	if cancelCtx := p.nwdaf.CancelContext(); cancelCtx != nil && cancelCtx.Err() != nil {
		logger.ProcLog.Infof("TriggerDataCollection: skipped sub=%s reason=shutdown", subscriptionId)
		return
	}

	// Guard: subscription may have been deleted during async execution
	ctx := nwdaf_context.GetSelf()
	if ctx.GetSubscription(subscriptionId) == nil {
		logger.ProcLog.Debugf("TriggerDataCollection: skipped sub=%s reason=deleted", subscriptionId)
		return
	}

	for i := range eventSubs {
		eventSub := &eventSubs[i]
		switch eventSub.Event {
		case models.NwdafEvent_UE_COMMUNICATION:
			// Trigger ML Model provisioning (async - starts first for parallel init)
			go p.triggerMlModelProvisioning(eventSub, subscriptionId)
			// Trigger SMF data collection (sync)
			p.triggerUeCommunicationCollection(eventSub, subscriptionId)
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
// Supports both SUPI-based and Group ID subscriptions (unified architecture)
func (p *Processor) triggerUeCommunicationCollection(
	eventSub *models.NwdafEventsSubscriptionEventSubscription,
	subscriptionId string,
) {
	// Check if SMF data collection is enabled in config
	cfg := p.config()
	if cfg == nil || cfg.Configuration == nil ||
		cfg.Configuration.Smf == nil || !cfg.Configuration.Smf.Enabled {
		logger.ProcLog.Debugf("SMF data collection is disabled in config")
		return
	}

	smfConfig := cfg.Configuration.Smf
	if len(smfConfig.Endpoints) == 0 {
		logger.ProcLog.Warnf("SMF data collection enabled but no endpoints configured")
		return
	}

	if eventSub.TgtUe == nil {
		logger.ProcLog.Warnf("TgtUe is nil, cannot trigger data collection")
		return
	}

	// Get consumer for SMF subscription
	smfConsumer := p.nwdaf.Consumer()
	if smfConsumer == nil {
		logger.ProcLog.Warnf("Consumer not available for data collection")
		return
	}

	// Get notification URIs from config
	sbiBaseURI := cfg.GetSbiUri()
	smfNotifUri := sbiBaseURI + "/collector/notify"
	upfNotifUri := sbiBaseURI + "/collector/upf-notify"
	if smfConfig.NotifUris != nil {
		if smfConfig.NotifUris.Smf != "" {
			smfNotifUri = smfConfig.NotifUris.Smf
		}
		if smfConfig.NotifUris.Upf != "" {
			upfNotifUri = smfConfig.NotifUris.Upf
		}
	}

	// SMF report period: driven by analytics model config (default: 10s)
	smfRepPeriod := int32(10)
	if cfg.Configuration.Analytics != nil &&
		cfg.Configuration.Analytics.UeCommunication != nil {
		smfRepPeriod = int32(
			cfg.Configuration.Analytics.UeCommunication.SamplingIntervalOrDefault(),
		)
	}

	ctx := nwdaf_context.GetSelf()

	// Build targets from TgtUe
	var targets []DataCollectionTarget

	// Handle SUPI-based subscriptions (direct)
	for _, supi := range eventSub.TgtUe.Supis {
		targets = append(targets, DataCollectionTarget{
			Supi: supi,
		})
	}

	// Handle Group ID subscriptions per TS 23.502 §4.15.4.5.2
	// NWDAF must resolve Group ID → SUPI list, then subscribe per-SUPI
	resolver := ctx.GetGroupResolver()
	if resolver == nil {
		logger.ProcLog.Warnf("GroupResolver not available, cannot resolve Group IDs")
	} else {
		for _, groupId := range eventSub.TgtUe.IntGroupIds {
			supis, err := resolver.ResolveGroupId(groupId)
			if err != nil {
				logger.ProcLog.Warnf("Failed to resolve groupId %s: %v", groupId, err)
				continue
			}
			logger.ProcLog.Debugf("ResolveGroupTargets: members=%d", len(supis))
			for _, supi := range supis {
				targets = append(targets, DataCollectionTarget{
					Supi:            supi,
					OriginalGroupId: groupId, // Track for analytics aggregation
				})
			}
		}
	}

	if len(targets) > 0 {
		p.triggerTargetDataCollection(
			ctx, smfConsumer, smfConfig.Endpoints,
			targets, subscriptionId,
			smfNotifUri, upfNotifUri, smfRepPeriod,
		)
	}
}

// DataCollectionTarget represents a SUPI target for SMF subscription
// Per TS 23.502 §4.15.4.5.2: Group ID is resolved to SUPIs before SMF subscription
type DataCollectionTarget struct {
	Supi            string // Target SUPI for SMF subscription
	OriginalGroupId string // Source Group ID (if resolved from group subscription)
}

// Identifier returns human-readable target identifier for logging/mapping
func (t DataCollectionTarget) Identifier() string {
	return "supi=" + t.Supi
}

// triggerTargetDataCollection handles SUPI-based SMF subscriptions
// Per TS 23.502 §4.15.4.5.2: Group IDs are already resolved to SUPIs before this function
func (p *Processor) triggerTargetDataCollection(
	ctx *nwdaf_context.NWDAFContext,
	smfConsumer consumer.ConsumerAPI,
	endpoints []string,
	targets []DataCollectionTarget,
	subscriptionId string,
	smfNotifUri, upfNotifUri string,
	smfRepPeriod int32,
) {
	for _, smfEndpoint := range endpoints {
		for _, target := range targets {
			targetId := target.Identifier()
			correlationId, found := ctx.GetSmfCorrelationId(targetId, smfEndpoint)

			if !found {
				correlationId = ctx.NewCorrelationId()
				// Store mapping optimistically so other threads might use it
				ctx.StoreSmfCorrelationId(targetId, smfEndpoint, correlationId)
			}

			// Get or create SMF subscription (with reference counting)
			sub, isNew := ctx.GetOrCreateSmfSubscription(correlationId, subscriptionId)

			if !isNew {
				_, _, refCount := sub.GetInfo()
				logger.ProcLog.Infof("CreateSmfSubscription: reused corr=%s refCount=%d",
					correlationId, refCount)
			} else {
				// Build SMF subscription options (always SUPI-based)
				eventSubs := consumer.BuildUpfEventSubs(upfNotifUri, true, true)
				opts := consumer.SmfSubscriptionOptions{
					NotifUri:    smfNotifUri,
					NotifId:     correlationId,
					EventSubs:   eventSubs,
					NotifMethod: "PERIODIC",
					RepPeriod:   smfRepPeriod,
					Supi:        target.Supi, // Always SUPI after Group ID resolution
				}

				subId, err := smfConsumer.SubscribeToSmf(p.nwdaf.CancelContext(), smfEndpoint, opts)
				if err != nil {
					ctx.ReleaseSmfSubscription(correlationId, subscriptionId)
					logger.ProcLog.Errorf("CreateSmfSubscription failed: corr=%s err=%v",
						correlationId, err)
					continue
				}

				// Update subscription with details
				sub.Lock()
				sub.TargetType = nwdaf_context.TargetType_SUPI
				sub.Supi = target.Supi
				sub.SmfEndpoint = smfEndpoint
				sub.SmfSubId = subId
				sub.Unlock()

				logger.ProcLog.Infof("CreateSmfSubscription: created corr=%s sub=%s",
					correlationId, subId)
			}

			// Record SMF subscription parameters for ADRF storage
			ctx.StoreAdrfSmfInfo(correlationId, &nwdaf_context.AdrfSmfInfo{
				Supi:        target.Supi,
				NotifId:     correlationId,
				NotifUri:    smfNotifUri,
				UpfNotifUri: upfNotifUri,
				NotifMethod: "PERIODIC",
				RepPeriod:   smfRepPeriod,
			})

			// Store cleanup tracking
			ctx.AddNwdafSubResource(subscriptionId, nwdaf_context.NwdafSubResource{
				SmfEndpoint:     smfEndpoint,
				TargetType:      nwdaf_context.TargetType_SUPI,
				Supi:            target.Supi,
				CorrelationId:   correlationId,
				OriginalGroupId: target.OriginalGroupId,
				CreatedAt:       time.Now(),
			})
		}
	}
}

// triggerMlModelProvisioning subscribes to MTLF for ML model provisioning
// Per TS 23.288 §6.2A: NWDAF(AnLF) subscribes to NWDAF(MTLF) for ML models
func (p *Processor) triggerMlModelProvisioning(
	eventSub *models.NwdafEventsSubscriptionEventSubscription,
	subscriptionId string,
) {
	if cancelCtx := p.nwdaf.CancelContext(); cancelCtx != nil && cancelCtx.Err() != nil {
		logger.ProcLog.Infof("CreateMtlfSubscription: skipped sub=%s reason=shutdown", subscriptionId)
		return
	}

	cfg := p.config()
	if cfg == nil || cfg.Configuration == nil {
		logger.ProcLog.Debugf("Configuration missing, skipping ML model provisioning")
		return
	}

	ctx := nwdaf_context.GetSelf()
	externalMtlf := cfg.Configuration.ExternalMtlf
	mtlfEndpoint := ""
	if externalMtlf != nil && len(externalMtlf.Endpoints) > 0 {
		mtlfEndpoint = externalMtlf.Endpoints[0]
	}
	mlInfo := nwdaf_context.NewMlModelInfo(eventSub.Event, mtlfEndpoint)
	ctx.SetMlModelInfo(subscriptionId, mlInfo)

	if externalMtlf == nil || !externalMtlf.Enabled {
		if cfg.Configuration.AnlfBackend != nil && cfg.Configuration.AnlfBackend.Enabled {
			if err := p.anlf.ApplySubscriptionRegistration(subscriptionId); err != nil {
				logger.ProcLog.Errorf("ApplyAnlfRuntime failed: sub=%s err=%v", subscriptionId, err)
			}
		}
		return
	}

	if len(externalMtlf.Endpoints) == 0 {
		logger.ProcLog.Warnf("External MTLF enabled but no endpoints configured")
		return
	}

	// Get consumer for MTLF subscription
	mtlfConsumer := p.nwdaf.Consumer()
	if mtlfConsumer == nil {
		logger.ProcLog.Warnf("Consumer not available for MTLF subscription")
		return
	}

	notifUri := p.anlf.BuildProvisionNotificationURI()
	if notifUri == "" {
		logger.ProcLog.Warnf("External MTLF subscription skipped: AnLF provision notification URI is empty")
		return
	}

	// Subscribe to first External MTLF endpoint
	opts := consumer.MtlfSubscriptionOptions{
		NotifUri: notifUri,
		NotifId:  subscriptionId, // Use NWDAF subscription ID as correlation
		Event:    eventSub.Event,
		TgtUe:    eventSub.TgtUe,
	}

	subId, err := mtlfConsumer.SubscribeToMtlf(p.nwdaf.CancelContext(), mtlfEndpoint, opts)
	if err != nil {
		logger.ProcLog.Errorf("CreateMtlfSubscription failed: sub=%s err=%v", subscriptionId, err)
		mlInfo.SetModelFailed(err)
		return
	}

	mlInfo.SetMtlfSubscription(subId)
	logger.ProcLog.Infof("CreateMtlfSubscription: created sub=%s mtlfSub=%s", subscriptionId, subId)
}
