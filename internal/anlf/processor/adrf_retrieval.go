package processor

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"

	"github.com/free5gc/nwdaf/internal/sbi/consumer"
)

var (
	ErrAdrfRetrievalUnavailable   = errors.New("ADRF retrieval proxy is unavailable")
	ErrAdrfRetrievalRouteNotFound = errors.New("ADRF retrieval route was not found")
)

func (p *Processor) CreateAdrfRetrievalSubscription(
	ctx context.Context,
	targetAPIBaseURI string,
	body []byte,
) (*consumer.StandardAdrfResponse, error) {
	if p.adrfRetrieval == nil {
		return nil, ErrAdrfRetrievalUnavailable
	}
	var subscription consumer.NadrfDataRetrievalSubscription
	if err := json.Unmarshal(body, &subscription); err != nil {
		return nil, err
	}
	response, err := p.adrfRetrieval.CreateAdrfRetrievalSubscription(
		ctx, targetAPIBaseURI, body,
	)
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
	p.adrfRetrievalMu.Lock()
	p.adrfRetrievalRoutes[subscriptionID] = adrfRetrievalRoute{
		TargetAPIBaseURI: targetAPIBaseURI,
		ResourceLocation: resourceLocation,
		NotifCorrID:      subscription.NotifCorrId,
	}
	p.adrfRetrievalMu.Unlock()
	return response, nil
}

func (p *Processor) DeleteAdrfRetrievalSubscription(
	ctx context.Context,
	subscriptionID string,
) (*consumer.StandardAdrfResponse, error) {
	p.adrfRetrievalMu.RLock()
	route, ok := p.adrfRetrievalRoutes[subscriptionID]
	p.adrfRetrievalMu.RUnlock()
	if !ok {
		return nil, ErrAdrfRetrievalRouteNotFound
	}
	response, err := p.adrfRetrieval.DeleteAdrfRetrievalSubscription(
		ctx, route.TargetAPIBaseURI, route.ResourceLocation,
	)
	if err == nil || response != nil && response.StatusCode == 404 {
		p.adrfRetrievalMu.Lock()
		delete(p.adrfRetrievalRoutes, subscriptionID)
		p.adrfRetrievalMu.Unlock()
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
