package processor

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
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
) error {
	if cancelCtx := p.nwdaf.CancelContext(); cancelCtx != nil && cancelCtx.Err() != nil {
		logger.ProcLog.Infof("TriggerDataCollection: skipped sub=%s reason=shutdown", subscriptionId)
		return fmt.Errorf("NWDAF is shutting down")
	}

	// Guard: subscription may have been deleted during async execution
	ctx := nwdaf_context.GetSelf()
	if ctx.GetSubscription(subscriptionId) == nil {
		logger.ProcLog.Debugf("TriggerDataCollection: skipped sub=%s reason=deleted", subscriptionId)
		return fmt.Errorf("subscription %s no longer exists", subscriptionId)
	}

	for i := range eventSubs {
		eventSub := &eventSubs[i]
		switch eventSub.Event {
		case models.NwdafEvent_UE_COMMUNICATION:
			p.triggerMlModelProvisioning(eventSub, subscriptionId)
			// Trigger SMF data collection (sync)
			p.triggerUeCommunicationCollection(eventSub, subscriptionId)
		case models.NwdafEvent_ABNORMAL_BEHAVIOUR:
			// Currently not supported - skip
			logger.ProcLog.Debugf("ABNORMAL_BEHAVIOUR data collection not implemented")
		default:
			logger.ProcLog.Debugf("Data collection not implemented for event: %s", eventSub.Event)
		}
	}
	staleResources := p.reconcileDataCollectionResources(subscriptionId)
	if err := p.syncObservationBindings(subscriptionId); err != nil {
		logger.ProcLog.Errorf("SyncObservationBindings failed: sub=%s err=%v", subscriptionId, err)
		for _, resource := range staleResources {
			nwdaf_context.GetSelf().AddNwdafSubResource(subscriptionId, resource)
		}
		return err
	}
	for _, resource := range staleResources {
		p.releaseDataCollectionResource(subscriptionId, resource)
	}
	return nil
}

func (p *Processor) reconcileDataCollectionResources(
	subscriptionID string,
) []nwdaf_context.NwdafSubResource {
	ctx := nwdaf_context.GetSelf()
	subscription := ctx.GetSubscription(subscriptionID)
	if subscription == nil {
		return nil
	}
	requirements := subscription.CollectionRequirementsSnapshot()
	profileKey := canonicalCollectionProfileKey(
		int32(requirements.SamplingIntervalSeconds),
		requirements.RequiredMeasurements,
	)
	targets := make(map[string]struct{})
	for i := range subscription.EventSubs {
		event := &subscription.EventSubs[i]
		if event.Event != models.NwdafEvent_UE_COMMUNICATION || event.TgtUe == nil {
			continue
		}
		for _, supi := range event.TgtUe.Supis {
			targets[supi] = struct{}{}
		}
		if resolver := ctx.GetGroupResolver(); resolver != nil {
			for _, groupID := range event.TgtUe.IntGroupIds {
				if supis, err := resolver.ResolveGroupId(groupID); err == nil {
					for _, supi := range supis {
						targets[supi] = struct{}{}
					}
				}
			}
		}
	}
	endpoints := make(map[string]struct{})
	if cfg := p.config(); cfg != nil && cfg.Configuration != nil && cfg.Configuration.Smf != nil {
		for _, endpoint := range cfg.Configuration.Smf.Endpoints {
			endpoints[endpoint] = struct{}{}
		}
	}
	resources := ctx.GetNwdafSubResources(subscriptionID)
	kept := make([]nwdaf_context.NwdafSubResource, 0, len(resources))
	stale := make([]nwdaf_context.NwdafSubResource, 0)
	for _, resource := range resources {
		smfSubscription := ctx.GetSmfSubscription(resource.CorrelationId)
		_, targetOK := targets[resource.Supi]
		_, endpointOK := endpoints[resource.SmfEndpoint]
		if targetOK && endpointOK && smfSubscription != nil && smfSubscription.ProfileKey == profileKey {
			kept = append(kept, resource)
			continue
		}
		stale = append(stale, resource)
	}
	ctx.SetNwdafSubResources(subscriptionID, kept)
	return stale
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

	ctx := nwdaf_context.GetSelf()
	subscription := ctx.GetSubscription(subscriptionId)
	if subscription == nil {
		return
	}
	requirements := subscription.CollectionRequirementsSnapshot()
	smfRepPeriod := int32(requirements.SamplingIntervalSeconds)
	if smfRepPeriod <= 0 {
		logger.ProcLog.Errorf("Collection requirements missing: sub=%s", subscriptionId)
		return
	}

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
	requirements := nwdaf_context.CollectionRequirements{
		SamplingIntervalSeconds: int(smfRepPeriod),
		RequiredMeasurements:    []string{"TOTAL_VOLUME", "UL_VOLUME", "DL_VOLUME"},
	}
	if subscription := ctx.GetSubscription(subscriptionId); subscription != nil {
		candidate := subscription.CollectionRequirementsSnapshot()
		if candidate.SamplingIntervalSeconds > 0 && len(candidate.RequiredMeasurements) > 0 {
			requirements = candidate
		}
	}
	profileKey := canonicalCollectionProfileKey(smfRepPeriod, requirements.RequiredMeasurements)
	for _, smfEndpoint := range endpoints {
		for _, target := range targets {
			targetId := target.Identifier()
			correlationId, found := ctx.GetSmfCorrelationIdForProfile(targetId, smfEndpoint, profileKey)

			if !found {
				correlationId = ctx.NewCorrelationId()
				// Store mapping optimistically so other threads might use it
				ctx.StoreSmfCorrelationIdForProfile(targetId, smfEndpoint, profileKey, correlationId)
			}

			// Get or create SMF subscription (with reference counting)
			sub, isNew := ctx.GetOrCreateSmfSubscription(correlationId, subscriptionId)

			if !isNew {
				_, _, refCount := sub.GetInfo()
				logger.ProcLog.Infof("CreateSmfSubscription: reused corr=%s refCount=%d",
					correlationId, refCount)
			} else {
				sub.Lock()
				sub.TargetType = nwdaf_context.TargetType_SUPI
				sub.Supi = target.Supi
				sub.SmfEndpoint = smfEndpoint
				sub.ProfileKey = profileKey
				sub.Unlock()
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

func canonicalCollectionProfileKey(samplingInterval int32, measurements []string) string {
	normalized := append([]string(nil), measurements...)
	for i := range normalized {
		normalized[i] = strings.ToUpper(strings.TrimSpace(normalized[i]))
	}
	sort.Strings(normalized)
	return fmt.Sprintf("periodic:%d:%s", samplingInterval, strings.Join(normalized, ","))
}

func (p *Processor) syncObservationBindings(subscriptionID string) error {
	return p.anlf.SyncCurrentObservationBindings(subscriptionID)
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
	mlInfo := ctx.GetMlModelInfo(subscriptionId)
	if mlInfo == nil {
		mlInfo = nwdaf_context.NewMlModelInfo(eventSub.Event, mtlfEndpoint)
		ctx.SetMlModelInfo(subscriptionId, mlInfo)
	}

	if externalMtlf == nil || !externalMtlf.Enabled {
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
	runtimeRevision, requirements, sourceIDs, active := ctx.GetSubscription(subscriptionId).RuntimeSnapshot()
	_ = requirements
	_ = sourceIDs
	_ = active
	binding := contract.ModelProvisionBinding{
		RuntimeRevision:           runtimeRevision,
		NotificationCorrelationID: subscriptionId,
		ProviderID:                mtlfEndpoint,
	}
	if err := p.anlf.SyncModelProvisionBinding(subscriptionId, binding); err != nil {
		logger.ProcLog.Errorf("CreateMtlfSubscription pre-sync failed: sub=%s err=%v", subscriptionId, err)
		mlInfo.SetModelFailed(err)
		return
	}

	subId, err := mtlfConsumer.SubscribeToMtlf(p.nwdaf.CancelContext(), mtlfEndpoint, opts)
	if err != nil {
		logger.ProcLog.Errorf("CreateMtlfSubscription failed: sub=%s err=%v", subscriptionId, err)
		mlInfo.SetModelFailed(err)
		return
	}
	binding.MtlfSubscriptionID = subId
	if syncErr := p.anlf.SyncModelProvisionBinding(subscriptionId, binding); syncErr != nil {
		logger.ProcLog.Errorf(
			"CreateMtlfSubscription binding sync failed: sub=%s mtlfSub=%s err=%v",
			subscriptionId,
			subId,
			syncErr,
		)
		if rollbackErr := mtlfConsumer.UnsubscribeFromMtlf(p.nwdaf.CancelContext(), mtlfEndpoint, subId); rollbackErr != nil {
			logger.ProcLog.Errorf("CreateMtlfSubscription rollback failed: mtlfSub=%s err=%v", subId, rollbackErr)
		}
		mlInfo.SetModelFailed(syncErr)
		return
	}

	mlInfo.SetMtlfSubscription(subId)
	logger.ProcLog.Infof("CreateMtlfSubscription: created sub=%s mtlfSub=%s", subscriptionId, subId)
}
