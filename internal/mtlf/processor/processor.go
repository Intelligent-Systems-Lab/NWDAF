// Package processor implements the thin MTLF auxiliary-edge use cases.
package processor

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"sync"

	"github.com/free5gc/nwdaf/internal/backend"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/openapi/models"
)

var (
	ErrAdrfRetrievalUnavailable   = errors.New("ADRF retrieval proxy is unavailable")
	ErrAdrfRetrievalRouteNotFound = errors.New("ADRF retrieval route was not found")
	ErrNFDiscoveryUnavailable     = errors.New("NF discovery proxy is unavailable")
)

type mlModelGateway interface {
	HandleMLModelProvisionNotification(
		context.Context,
		string,
		[]byte,
	) (*backend.StandardResponse, *models.ProblemDetails)
	HandleCreateMLModelMonitorSubscriptionFromBackend(
		context.Context,
		[]byte,
		string,
		*backend.SelectedTarget,
	) (*backend.StandardResponse, *models.ProblemDetails)
	HandleReplaceMLModelMonitorSubscriptionFromBackend(
		context.Context,
		string,
		[]byte,
	) (*backend.StandardResponse, *models.ProblemDetails)
	HandleDeleteMLModelMonitorSubscriptionFromBackend(
		context.Context,
		string,
	) (*backend.StandardResponse, *models.ProblemDetails)
}

type nfDiscoveryProxy interface {
	DiscoverNFInstances(context.Context, backend.NFDiscoveryQuery) (*consumer.NFDiscoveryResult, error)
}

type adrfRetrievalProxy interface {
	CreateAdrfRetrievalSubscription(context.Context, string, []byte) (*consumer.StandardAdrfResponse, error)
	DeleteAdrfRetrievalSubscription(context.Context, string, string) (*consumer.StandardAdrfResponse, error)
}

type adrfRetrievalRoute struct {
	targetAPIBaseURI string
	resourceLocation string
}

type Processor struct {
	mlModel     mlModelGateway
	nfDiscovery nfDiscoveryProxy
	adrf        adrfRetrievalProxy

	adrfMu     sync.RWMutex
	adrfRoutes map[string]adrfRetrievalRoute
}

func New(
	mlModel mlModelGateway,
	nfDiscovery nfDiscoveryProxy,
	adrf adrfRetrievalProxy,
) *Processor {
	return &Processor{
		mlModel:     mlModel,
		nfDiscovery: nfDiscovery,
		adrf:        adrf,
		adrfRoutes:  make(map[string]adrfRetrievalRoute),
	}
}

func (p *Processor) HandleMLModelProvisionNotification(
	ctx context.Context,
	subscriptionID string,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	if p.mlModel == nil {
		return nil, nil
	}
	return p.mlModel.HandleMLModelProvisionNotification(ctx, subscriptionID, body)
}

func (p *Processor) HandleCreateMLModelMonitorSubscriptionFromBackend(
	ctx context.Context,
	body []byte,
	ownerRegistrationID string,
	target *backend.SelectedTarget,
) (*backend.StandardResponse, *models.ProblemDetails) {
	if p.mlModel == nil {
		return nil, nil
	}
	return p.mlModel.HandleCreateMLModelMonitorSubscriptionFromBackend(
		ctx,
		body,
		ownerRegistrationID,
		target,
	)
}

func (p *Processor) HandleReplaceMLModelMonitorSubscriptionFromBackend(
	ctx context.Context,
	subscriptionID string,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	if p.mlModel == nil {
		return nil, nil
	}
	return p.mlModel.HandleReplaceMLModelMonitorSubscriptionFromBackend(ctx, subscriptionID, body)
}

func (p *Processor) HandleDeleteMLModelMonitorSubscriptionFromBackend(
	ctx context.Context,
	subscriptionID string,
) (*backend.StandardResponse, *models.ProblemDetails) {
	if p.mlModel == nil {
		return nil, nil
	}
	return p.mlModel.HandleDeleteMLModelMonitorSubscriptionFromBackend(ctx, subscriptionID)
}

func (p *Processor) HandleNFDiscovery(
	ctx context.Context,
	query backend.NFDiscoveryQuery,
) (*consumer.NFDiscoveryResult, error) {
	if p.nfDiscovery == nil {
		return nil, ErrNFDiscoveryUnavailable
	}
	return p.nfDiscovery.DiscoverNFInstances(ctx, query)
}

func (p *Processor) CreateAdrfRetrievalSubscription(
	ctx context.Context,
	targetAPIBaseURI string,
	body []byte,
) (*consumer.StandardAdrfResponse, error) {
	if p.adrf == nil {
		return nil, ErrAdrfRetrievalUnavailable
	}
	var subscription consumer.NadrfDataRetrievalSubscription
	if err := json.Unmarshal(body, &subscription); err != nil {
		return nil, err
	}
	response, err := p.adrf.CreateAdrfRetrievalSubscription(ctx, targetAPIBaseURI, body)
	if err != nil {
		return response, err
	}
	subscriptionID, resourceLocation, err := validateAdrfRetrievalLocation(
		targetAPIBaseURI,
		response.Location,
	)
	if err != nil {
		return nil, err
	}
	p.adrfMu.Lock()
	p.adrfRoutes[subscriptionID] = adrfRetrievalRoute{
		targetAPIBaseURI: targetAPIBaseURI,
		resourceLocation: resourceLocation,
	}
	p.adrfMu.Unlock()
	return response, nil
}

func (p *Processor) DeleteAdrfRetrievalSubscription(
	ctx context.Context,
	subscriptionID string,
) (*consumer.StandardAdrfResponse, error) {
	p.adrfMu.RLock()
	route, ok := p.adrfRoutes[subscriptionID]
	p.adrfMu.RUnlock()
	if !ok {
		return nil, ErrAdrfRetrievalRouteNotFound
	}
	response, err := p.adrf.DeleteAdrfRetrievalSubscription(
		ctx,
		route.targetAPIBaseURI,
		route.resourceLocation,
	)
	if err == nil || response != nil && response.StatusCode == 404 {
		p.adrfMu.Lock()
		delete(p.adrfRoutes, subscriptionID)
		p.adrfMu.Unlock()
	}
	return response, err
}

func validateAdrfRetrievalLocation(
	targetAPIBaseURI string,
	location string,
) (string, string, error) {
	target, err := url.Parse(strings.TrimRight(targetAPIBaseURI, "/") + "/")
	if err != nil {
		return "", "", errors.New("ADRF target API root is invalid")
	}
	reference, err := url.Parse(strings.TrimSpace(location))
	if err != nil || location == "" {
		return "", "", errors.New("ADRF retrieval Location is invalid")
	}
	resolved := target.ResolveReference(reference)
	if resolved.Scheme != target.Scheme ||
		!strings.EqualFold(resolved.Hostname(), target.Hostname()) ||
		resolved.Port() != target.Port() ||
		resolved.User != nil || resolved.RawQuery != "" || resolved.Fragment != "" {
		return "", "", errors.New("ADRF retrieval Location must stay on the selected ADRF origin")
	}
	resourcePath := strings.TrimSuffix(resolved.Path, "/")
	prefix := consumer.AdrfDataRetrievalSubscriptionsPath + "/"
	if !strings.HasPrefix(resourcePath, prefix) {
		return "", "", errors.New("ADRF retrieval Location has an unexpected resource path")
	}
	subscriptionID := strings.TrimPrefix(resourcePath, prefix)
	if subscriptionID == "" || strings.Contains(subscriptionID, "/") {
		return "", "", errors.New("ADRF retrieval Location has no unambiguous resource identifier")
	}
	return subscriptionID, resolved.String(), nil
}
