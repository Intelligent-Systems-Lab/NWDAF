package processor

import (
	"math/big"
	"strings"
	"time"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/openapi/models"
)

type analyticsFailureDispatcher func(
	[]models.NnwdafEventsSubscriptionNotification,
	[]byte,
) error

// ResetAnalyticsGeneration sends one terminal data-unavailable report for
// each affected subscription, then tombstones the local route. The tombstone
// makes a late consumer DELETE idempotently successful without reviving state.
func (p *Processor) ResetAnalyticsGeneration(
	generation string,
	dispatch analyticsFailureDispatcher,
) []string {
	if p == nil || generation == "" || p.nwdaf == nil || p.nwdaf.Context() == nil {
		return nil
	}
	p.eventsMu.Lock()
	defer p.eventsMu.Unlock()

	ctx := p.nwdaf.Context()
	removed := make([]string, 0)
	for _, route := range ctx.GetAllAnalyticsSubscriptionRoutes() {
		if route.ProcessGeneration != generation {
			continue
		}
		events := make([]models.NwdafEventsSubscriptionEventNotification, 0,
			len(route.AcceptedSubscription.EventSubscriptions))
		for index := range route.AcceptedSubscription.EventSubscriptions {
			requested := &route.AcceptedSubscription.EventSubscriptions[index]
			now := time.Now()
			events = append(events, models.NwdafEventsSubscriptionEventNotification{
				Event:          requested.Event,
				TimeStampGen:   &now,
				FailNotifyCode: models.NwdafFailureCode_UNAVAILABLE_DATA,
			})
		}
		if dispatch != nil && len(events) > 0 &&
			supportsFeature(route.AcceptedSubscription.SupportedFeatures, 11) &&
			supportsFeature(route.AcceptedSubscription.SupportedFeatures, 53) {
			err := dispatch([]models.NnwdafEventsSubscriptionNotification{{
				SubscriptionId:     route.SubscriptionID,
				NotifCorrId:        route.AcceptedSubscription.NotifCorrId,
				EventNotifications: events,
			}}, nil)
			if err != nil {
				logger.ProcLog.Warnf(
					"Failed to deliver backend-loss notification: subscriptionId=%s err=%v",
					route.SubscriptionID,
					err,
				)
			}
		}
		ctx.TombstoneAnalyticsSubscription(route.SubscriptionID)
		removed = append(removed, route.SubscriptionID)
	}
	return removed
}

func supportsFeature(mask string, number uint) bool {
	mask = strings.TrimSpace(mask)
	if mask == "" || number == 0 {
		return false
	}
	value := new(big.Int)
	if _, ok := value.SetString(mask, 16); !ok {
		return false
	}
	return value.Bit(int(number-1)) == 1
}
