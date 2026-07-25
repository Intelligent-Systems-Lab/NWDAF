package processor

import (
	"context"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/openapi/models"
)

type eventsSubscriptionNotificationDispatcher interface {
	DispatchEventsSubscriptionNotifications(
		[]models.NnwdafEventsSubscriptionNotification,
		[]byte,
	) error
}

type nfDiscoveryProxy interface {
	DiscoverNFInstances(context.Context, consumer.NFDiscoveryQuery) (*consumer.NFDiscoveryResult, error)
}

type smfEventExposureProxy interface {
	CreateSmfEventExposure(context.Context, string, []byte) (*consumer.StandardSmfResponse, error)
	ReadSmfEventExposure(context.Context, string, string) (*consumer.StandardSmfResponse, error)
	ReplaceSmfEventExposure(context.Context, string, string, []byte) (*consumer.StandardSmfResponse, error)
	DeleteSmfEventExposure(context.Context, string, string) (*consumer.StandardSmfResponse, error)
}

type adrfStorageProxy interface {
	StoreAdrfDataRecord(context.Context, string, []byte) (*consumer.StandardAdrfResponse, error)
}

type Processor struct {
	notificationDispatcher eventsSubscriptionNotificationDispatcher
	nfDiscovery            nfDiscoveryProxy
	smfEventExposure       smfEventExposureProxy
	adrfStorage            adrfStorageProxy
	nwdafContext           *nwdaf_context.NWDAFContext
	availability           availabilitySnapshot
	mtlfSyncRefresher      syncRefresher
}

func (p *Processor) SetNFDiscoveryProxy(proxy nfDiscoveryProxy) {
	p.nfDiscovery = proxy
}

func (p *Processor) SetSmfEventExposureProxy(proxy smfEventExposureProxy) {
	p.smfEventExposure = proxy
}

func (p *Processor) SetAdrfStorageProxy(proxy adrfStorageProxy) {
	p.adrfStorage = proxy
}

func NewProcessor(dispatchers ...eventsSubscriptionNotificationDispatcher) *Processor {
	processor := &Processor{}
	if len(dispatchers) > 0 {
		processor.notificationDispatcher = dispatchers[0]
	}
	return processor
}
