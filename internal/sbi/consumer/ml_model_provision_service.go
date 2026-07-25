package consumer

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/free5gc/openapi/models"
	MLModelProvision "github.com/free5gc/openapi/nwdaf/MLModelProvision"
)

const (
	// Nnwdaf_MLModelProvision API path
	MLModelProvisionSubscriptionsPath = "/nnwdaf-mlmodelprovision/v1/subscriptions"
)

// MLModelProvisionService is a standard NWDAF-to-NWDAF ML Model Provision consumer.
// Per TS 29.520 §5.4: Nnwdaf_MLModelProvision Service API.
type MLModelProvisionService struct {
	httpClient *http.Client

	mu         sync.Mutex
	apiClients map[string]*MLModelProvision.APIClient
}

// NewMLModelProvisionService creates a standard ML Model Provision consumer.
func NewMLModelProvisionService() *MLModelProvisionService {
	return &MLModelProvisionService{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        50,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		apiClients: make(map[string]*MLModelProvision.APIClient),
	}
}

// MLModelProvisionSubscriptionOptions configures a peer NWDAF subscription.
type MLModelProvisionSubscriptionOptions struct {
	NotifUri string                      // Callback URI for ML model notifications
	NotifId  string                      // Notification correlation ID
	Event    models.NwdafEvent           // Analytics event type
	TgtUe    *models.TargetUeInformation // Target UE information
}

// SubscribeToNWDAF creates an ML Model Provision subscription to a peer NWDAF.
// Per TS 29.520 §5.4.3.2.3.1: POST to /subscriptions
func (s *MLModelProvisionService) SubscribeToNWDAF(
	ctx context.Context,
	nwdafEndpoint string,
	opts MLModelProvisionSubscriptionOptions,
) (string, error) {
	requestModel := models.NwdafMlModelProvSubsc{
		MLEventSubscs: []models.MlEventSubscription{
			{
				MLEvent: opts.Event,
				TgtUe:   opts.TgtUe,
			},
		},
		NotifUri:     opts.NotifUri,
		NotifCorreId: opts.NotifId,
	}

	request := &MLModelProvision.CreateNWDAFMLModelProvisionSubcriptionRequest{}
	request.SetNwdafMlModelProvSubsc(requestModel)

	ctx, cancel, err := timeoutContextFromParent(
		ctx,
		10*time.Second,
		"ML Model Provision subscription",
	)
	if err != nil {
		return "", err
	}
	defer cancel()

	response, err := s.apiClient(nwdafEndpoint).SubscriptionsCollectionApi.
		CreateNWDAFMLModelProvisionSubcription(ctx, request)
	if err != nil {
		return "", fmt.Errorf("failed to create ML Model Provision subscription: %w", err)
	}

	subscriptionID := parseResourceID(response.Location)
	if subscriptionID == "" {
		return "", fmt.Errorf(
			"ML Model Provision subscription response has an invalid Location",
		)
	}

	return subscriptionID, nil
}

// UnsubscribeFromNWDAF deletes a peer NWDAF ML Model Provision subscription.
// Per TS 29.520 §5.4.3.3.3.2: DELETE /subscriptions/{subscriptionId}
func (s *MLModelProvisionService) UnsubscribeFromNWDAF(
	ctx context.Context,
	nwdafEndpoint string,
	subscriptionId string,
) error {
	request := &MLModelProvision.DeleteNWDAFMLModelProvisionSubcriptionRequest{}
	request.SetSubscriptionId(subscriptionId)

	ctx, cancel, err := timeoutContextFromParent(
		ctx,
		10*time.Second,
		"ML Model Provision unsubscription",
	)
	if err != nil {
		return err
	}
	defer cancel()

	if _, deleteErr := s.apiClient(nwdafEndpoint).IndividualNWDAFMLModelProvisionSubscriptionDocumentApi.
		DeleteNWDAFMLModelProvisionSubcription(ctx, request); deleteErr != nil {
		return fmt.Errorf("failed to delete ML Model Provision subscription: %w", deleteErr)
	}

	return nil
}

func (s *MLModelProvisionService) apiClient(nwdafEndpoint string) *MLModelProvision.APIClient {
	s.mu.Lock()
	defer s.mu.Unlock()

	if client, ok := s.apiClients[nwdafEndpoint]; ok {
		return client
	}

	configuration := MLModelProvision.NewConfiguration()
	configuration.SetBasePath(nwdafEndpoint)
	configuration.SetHTTPClient(s.httpClient)

	client := MLModelProvision.NewAPIClient(configuration)
	s.apiClients[nwdafEndpoint] = client
	return client
}

// HTTPClient returns the underlying HTTP client for testing
func (s *MLModelProvisionService) HTTPClient() *http.Client {
	return s.httpClient
}

func parseResourceID(location string) string {
	parsed, err := url.Parse(strings.TrimSpace(location))
	if err != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return ""
	}
	resourcePath := strings.TrimSuffix(parsed.Path, "/")
	prefix := MLModelProvisionSubscriptionsPath + "/"
	if !strings.HasPrefix(resourcePath, prefix) {
		return ""
	}
	escapedID := strings.TrimPrefix(resourcePath, prefix)
	if escapedID == "" || strings.Contains(escapedID, "/") {
		return ""
	}
	subscriptionID, err := url.PathUnescape(escapedID)
	if err != nil || subscriptionID == "" || strings.Contains(subscriptionID, "/") {
		return ""
	}
	return subscriptionID
}
