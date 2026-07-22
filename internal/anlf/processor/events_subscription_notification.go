package processor

import (
	"fmt"

	"github.com/free5gc/openapi/models"
)

func (p *Processor) HandleEventsSubscriptionNotification(
	notifications []models.NnwdafEventsSubscriptionNotification,
	rawBody []byte,
) error {
	if p.notificationDispatcher == nil {
		return fmt.Errorf("analytics notification dispatcher is not configured")
	}
	return p.notificationDispatcher.DispatchEventsSubscriptionNotifications(notifications, rawBody)
}
