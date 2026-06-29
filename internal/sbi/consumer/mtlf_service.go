package consumer

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/free5gc/openapi/models"
	MLModelProvision "github.com/free5gc/openapi/nwdaf/MLModelProvision"
)

const (
	// Nnwdaf_MLModelProvision API path
	MtlfMLModelProvisionPath = "/nnwdaf-mlmodelprovision/v1/subscriptions"
)

// NmtlfService handles MTLF ML Model Provision API interactions
// Per TS 29.520 §5.4: Nnwdaf_MLModelProvision Service API
type NmtlfService struct {
	httpClient *http.Client

	mu         sync.Mutex
	apiClients map[string]*MLModelProvision.APIClient
}

// NewNmtlfService creates a new NmtlfService with HTTP client
func NewNmtlfService() *NmtlfService {
	return &NmtlfService{
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

// MtlfSubscriptionOptions configures ML Model Provision subscription
type MtlfSubscriptionOptions struct {
	NotifUri string                      // Callback URI for ML model notifications
	NotifId  string                      // Notification correlation ID
	Event    models.NwdafEvent           // Analytics event type
	TgtUe    *models.TargetUeInformation // Target UE information
}

// SubscribeToMtlf creates an ML Model Provision subscription to MTLF
// Per TS 29.520 §5.4.3.2.3.1: POST to /subscriptions
func (s *NmtlfService) SubscribeToMtlf(
	ctx context.Context,
	mtlfEndpoint string,
	opts MtlfSubscriptionOptions,
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

	ctx, cancel, err := timeoutContextFromParent(ctx, 10*time.Second, "MTLF subscription")
	if err != nil {
		return "", err
	}
	defer cancel()

	response, err := s.apiClient(mtlfEndpoint).SubscriptionsCollectionApi.
		CreateNWDAFMLModelProvisionSubcription(ctx, request)
	if err != nil {
		return "", fmt.Errorf("failed to create MTLF subscription: %w", err)
	}

	subscriptionID := parseResourceID(response.Location)
	if subscriptionID == "" {
		subscriptionID = opts.NotifId
	}

	return subscriptionID, nil
}

// UnsubscribeFromMtlf deletes an ML Model Provision subscription from MTLF
// Per TS 29.520 §5.4.3.3.3.2: DELETE /subscriptions/{subscriptionId}
func (s *NmtlfService) UnsubscribeFromMtlf(
	ctx context.Context,
	mtlfEndpoint string,
	subscriptionId string,
) error {
	request := &MLModelProvision.DeleteNWDAFMLModelProvisionSubcriptionRequest{}
	request.SetSubscriptionId(subscriptionId)

	ctx, cancel, err := timeoutContextFromParent(ctx, 10*time.Second, "MTLF unsubscription")
	if err != nil {
		return err
	}
	defer cancel()

	if _, deleteErr := s.apiClient(mtlfEndpoint).IndividualNWDAFMLModelProvisionSubscriptionDocumentApi.
		DeleteNWDAFMLModelProvisionSubcription(ctx, request); deleteErr != nil {
		return fmt.Errorf("failed to delete MTLF subscription: %w", deleteErr)
	}

	return nil
}

func (s *NmtlfService) apiClient(mtlfEndpoint string) *MLModelProvision.APIClient {
	s.mu.Lock()
	defer s.mu.Unlock()

	if client, ok := s.apiClients[mtlfEndpoint]; ok {
		return client
	}

	configuration := MLModelProvision.NewConfiguration()
	configuration.SetBasePath(mtlfEndpoint)
	configuration.SetHTTPClient(s.httpClient)

	client := MLModelProvision.NewAPIClient(configuration)
	s.apiClients[mtlfEndpoint] = client
	return client
}

// HTTPClient returns the underlying HTTP client for testing
func (s *NmtlfService) HTTPClient() *http.Client {
	return s.httpClient
}

func parseResourceID(location string) string {
	if location == "" {
		return ""
	}
	if idx := strings.LastIndex(location, "/"); idx >= 0 {
		return location[idx+1:]
	}
	return location
}
