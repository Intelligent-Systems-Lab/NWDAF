package processor

import (
	"context"

	"github.com/free5gc/nwdaf/internal/backend"
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
	DiscoverNFInstances(context.Context, backend.NFDiscoveryQuery) (*consumer.NFDiscoveryResult, error)
}

type smfEventExposureProxy interface {
	CreateSmfEventExposure(context.Context, string, []byte) (*consumer.StandardSmfResponse, error)
	ReadSmfEventExposure(context.Context, string, string) (*consumer.StandardSmfResponse, error)
	ReplaceSmfEventExposure(context.Context, string, string, []byte) (*consumer.StandardSmfResponse, error)
	DeleteSmfEventExposure(context.Context, string, string) (*consumer.StandardSmfResponse, error)
}

type adrfStorageProxy interface {
	StoreAdrfDataRecord(context.Context, string, []byte) (*consumer.StandardAdrfResponse, error)
	RetrieveAdrfMLModelRecord(context.Context, string, string, []int64) (*consumer.StandardAdrfResponse, error)
}

type udmCollectionProxy interface {
	GetUdmGroupIdentifiers(context.Context, string, string, bool) (*consumer.StandardUdmResponse, error)
	GetUdmSmfRegistration(context.Context, string, string, *models.Snssai, string) (*consumer.StandardUdmResponse, error)
}

type trainingDataDescriptorRelay interface {
	PutTrainingDataDescriptor(context.Context, string, []byte) (*backend.StandardResponse, error)
	DeleteTrainingDataDescriptor(context.Context, string) (*backend.StandardResponse, error)
}

type Processor struct {
	notificationDispatcher eventsSubscriptionNotificationDispatcher
	nfDiscovery            nfDiscoveryProxy
	smfEventExposure       smfEventExposureProxy
	adrfStorage            adrfStorageProxy
	udmCollection          udmCollectionProxy
	trainingDataRelay      trainingDataDescriptorRelay
	mtlfAvailability       interface {
		Acquire() (*backend.GenerationLease, bool)
	}
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

func (p *Processor) SetUdmCollectionProxy(proxy udmCollectionProxy) {
	p.udmCollection = proxy
}

func (p *Processor) SetTrainingDataDescriptorRelay(
	relay trainingDataDescriptorRelay,
	availability interface {
		Acquire() (*backend.GenerationLease, bool)
	},
) {
	p.trainingDataRelay = relay
	p.mtlfAvailability = availability
}

func NewProcessor(dispatchers ...eventsSubscriptionNotificationDispatcher) *Processor {
	processor := &Processor{}
	if len(dispatchers) > 0 {
		processor.notificationDispatcher = dispatchers[0]
	}
	return processor
}
