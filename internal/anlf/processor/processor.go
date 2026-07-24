package processor

import (
	"context"
	"sync"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	"github.com/free5gc/nwdaf/internal/anlf/coordinator"
	"github.com/free5gc/nwdaf/internal/backend"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/openapi/models"
)

type mlModelProvisionWorkflow interface {
	PlanModelProvisionActions(notif *contract.ModelProvisionNotification) []coordinator.ModelProvisionAction
	StartModelProvisionActions(actions []coordinator.ModelProvisionAction)
}

type modelAccuracyWorkflow interface {
	HandleModelAccuracyReport(report *contract.ModelAccuracyReport) error
}

type analyticsReportDispatcher interface {
	DispatchAnalyticsReport(subscriptionID string, report *contract.AnalyticsReport) error
}

type eventsSubscriptionNotificationDispatcher interface {
	DispatchEventsSubscriptionNotifications(
		[]models.NnwdafEventsSubscriptionNotification,
		[]byte,
	) error
}

type runtimeCompletionWorkflow interface {
	CompleteSubscriptionRuntime(event *contract.RuntimeCompletionEvent) error
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

type adrfRetrievalProxy interface {
	CreateAdrfRetrievalSubscription(context.Context, string, []byte) (*consumer.StandardAdrfResponse, error)
	DeleteAdrfRetrievalSubscription(context.Context, string, string) (*consumer.StandardAdrfResponse, error)
}

type adrfRetrievalRoute struct {
	TargetAPIBaseURI string
	ResourceLocation string
	NotifCorrID      string
}

type smfAssociationMirror interface {
	ReplaceSmfResourceAssociations(backend.SmfResourceAssociationUpdate) error
}

type Processor struct {
	workflow               mlModelProvisionWorkflow
	accuracyWorkflow       modelAccuracyWorkflow
	reportDispatcher       analyticsReportDispatcher
	notificationDispatcher eventsSubscriptionNotificationDispatcher
	completionWorkflow     runtimeCompletionWorkflow
	nfDiscovery            nfDiscoveryProxy
	smfEventExposure       smfEventExposureProxy
	adrfStorage            adrfStorageProxy
	adrfRetrieval          adrfRetrievalProxy
	adrfRetrievalMu        sync.RWMutex
	adrfRetrievalRoutes    map[string]adrfRetrievalRoute
	associationMirror      smfAssociationMirror
}

func (p *Processor) SetModelAccuracyWorkflow(workflow modelAccuracyWorkflow) {
	p.accuracyWorkflow = workflow
}

func (p *Processor) SetNFDiscoveryProxy(proxy nfDiscoveryProxy) {
	p.nfDiscovery = proxy
}

func (p *Processor) SetSmfEventExposureProxy(proxy smfEventExposureProxy) {
	p.smfEventExposure = proxy
}

func (p *Processor) SetAdrfStorageProxy(proxy adrfStorageProxy) {
	p.adrfStorage = proxy
	if retrieval, ok := proxy.(adrfRetrievalProxy); ok {
		p.adrfRetrieval = retrieval
	}
}

func NewProcessor(
	workflow mlModelProvisionWorkflow,
	dispatchers ...analyticsReportDispatcher,
) *Processor {
	processor := &Processor{
		workflow:            workflow,
		adrfRetrievalRoutes: make(map[string]adrfRetrievalRoute),
	}
	if associationMirror, ok := workflow.(smfAssociationMirror); ok {
		processor.associationMirror = associationMirror
	}
	if completionWorkflow, ok := workflow.(runtimeCompletionWorkflow); ok {
		processor.completionWorkflow = completionWorkflow
	}
	if len(dispatchers) > 0 {
		processor.reportDispatcher = dispatchers[0]
		if notificationDispatcher, ok := dispatchers[0].(eventsSubscriptionNotificationDispatcher); ok {
			processor.notificationDispatcher = notificationDispatcher
		}
	}
	return processor
}

func (p *Processor) ReplaceSmfResourceAssociations(
	update backend.SmfResourceAssociationUpdate,
) error {
	if p.associationMirror == nil {
		return coordinator.ErrBackendUnavailable
	}
	return p.associationMirror.ReplaceSmfResourceAssociations(update)
}
