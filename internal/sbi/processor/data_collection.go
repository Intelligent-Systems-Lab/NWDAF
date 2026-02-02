package processor

import (
	"time"

	"github.com/google/uuid"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

// TriggerDataCollection triggers data collection subscriptions for source NFs
// Per 3GPP TS 23.288 §6.2: NWDAF invokes Nnf_EventExposure_Subscribe to collect data
func (p *Processor) TriggerDataCollection(
	eventSubs []models.NwdafEventsSubscriptionEventSubscription,
	subscriptionId string,
) {
	// Guard: subscription may have been deleted during async execution
	ctx := nwdaf_context.GetSelf()
	if ctx.GetSubscription(subscriptionId) == nil {
		logger.ProcLog.Warnf("Subscription %s not found, skipping data collection", subscriptionId)
		return
	}

	for i := range eventSubs {
		eventSub := &eventSubs[i]
		switch eventSub.Event {
		case models.NwdafEvent_UE_COMMUNICATION:
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

	// Get consumer for SMF subscription
	smfConsumer := p.nwdaf.Consumer()
	if smfConsumer == nil {
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

	// Fixed SMF report period for data collection
	var smfRepPeriod int32 = 10 // Fixed: 10s

	ctx := nwdaf_context.GetSelf()

	// Build targets from TgtUe
	var targets []DataCollectionTarget

	// Handle SUPI-based subscriptions
	for _, supi := range eventSub.TgtUe.Supis {
		targets = append(targets, DataCollectionTarget{
			TargetType: nwdaf_context.TargetType_SUPI,
			Supi:       supi,
		})
	}

	// Handle Group ID subscriptions
	for _, groupId := range eventSub.TgtUe.IntGroupIds {
		targets = append(targets, DataCollectionTarget{
			TargetType: nwdaf_context.TargetType_GROUP_ID,
			GroupId:    groupId,
		})
	}

	if len(targets) > 0 {
		p.triggerTargetDataCollection(
			ctx, smfConsumer, smfConfig.Endpoints,
			targets, subscriptionId,
			smfNotifUri, upfNotifUri, smfRepPeriod,
		)
	}
}

// DataCollectionTarget abstracts SUPI vs Group ID for unified collection
type DataCollectionTarget struct {
	TargetType nwdaf_context.TargetType
	Supi       string
	GroupId    string
}

// Identifier returns human-readable target identifier
func (t DataCollectionTarget) Identifier() string {
	if t.TargetType == nwdaf_context.TargetType_GROUP_ID {
		return "groupId=" + t.GroupId
	}
	return "supi=" + t.Supi
}

// triggerTargetDataCollection handles both SUPI and Group ID subscriptions
func (p *Processor) triggerTargetDataCollection(
	ctx *nwdaf_context.NWDAFContext,
	smfConsumer *consumer.Consumer,
	endpoints []string,
	targets []DataCollectionTarget,
	subscriptionId string,
	smfNotifUri, upfNotifUri string,
	smfRepPeriod int32,
) {
	for _, smfEndpoint := range endpoints {
		for _, target := range targets {
			correlationId := uuid.New().String()

			// Get or create SMF subscription (with reference counting)
			sub, isNew := ctx.GetOrCreateSmfSubscription(correlationId, subscriptionId)

			if !isNew {
				_, _, refCount := sub.GetInfo()
				logger.ProcLog.Infof("Reusing SMF subscription for %s (refCount=%d)",
					target.Identifier(), refCount)
			} else {
				// Build SMF subscription options based on target type
				eventSubs := consumer.BuildUpfEventSubs(upfNotifUri, true, true)
				opts := consumer.SmfSubscriptionOptions{
					NotifUri:    smfNotifUri,
					NotifId:     correlationId,
					EventSubs:   eventSubs,
					NotifMethod: "PERIODIC",
					RepPeriod:   smfRepPeriod,
				}

				// Set target (SUPI or GroupId)
				if target.TargetType == nwdaf_context.TargetType_GROUP_ID {
					opts.GroupId = target.GroupId
				} else {
					opts.Supi = target.Supi
				}

				subId, err := smfConsumer.SubscribeToSmf(smfEndpoint, opts)
				if err != nil {
					ctx.ReleaseSmfSubscription(correlationId, subscriptionId)
					logger.ProcLog.Errorf("Failed to subscribe SMF for %s: %v",
						target.Identifier(), err)
					continue
				}

				// Update subscription with details
				sub.Lock()
				sub.TargetType = target.TargetType
				sub.Supi = target.Supi
				sub.GroupId = target.GroupId
				sub.SmfEndpoint = smfEndpoint
				sub.SmfSubId = subId
				sub.Unlock()

				logger.ProcLog.Infof("SMF subscription created: %s, subId=%s, corrId=%s",
					target.Identifier(), subId, correlationId)
			}

			// Store cleanup tracking
			ctx.AddNwdafSubResource(subscriptionId, nwdaf_context.NwdafSubResource{
				SmfEndpoint:   smfEndpoint,
				TargetType:    target.TargetType,
				Supi:          target.Supi,
				GroupId:       target.GroupId,
				CorrelationId: correlationId,
				CreatedAt:     time.Now(),
			})
		}
	}
}
