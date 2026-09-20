package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/free5gc/nwdaf/internal/backend"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

const (
	testProvisionID    = "11111111-1111-4111-8111-111111111111"
	testRegistrationID = "22222222-2222-4222-8222-222222222222"
	testMonitorID      = "33333333-3333-4333-8333-333333333333"
)

type mlModelAvailabilityStub struct {
	usable     bool
	marked     int
	refreshed  int
	generation string
}

func (s *mlModelAvailabilityStub) Usable() bool { return s.usable }

func (s *mlModelAvailabilityStub) Acquire() (*backend.GenerationLease, bool) {
	return &backend.GenerationLease{}, s.usable
}

func (s *mlModelAvailabilityStub) MarkUnavailable(string) {
	s.marked++
}

func (s *mlModelAvailabilityStub) Refresh() { s.refreshed++ }

func (s *mlModelAvailabilityStub) Snapshot() backend.Snapshot {
	return backend.Snapshot{
		State:             backend.StateUsable,
		ProcessInstanceID: s.generation,
	}
}

type mtlfMLModelBackendStub struct {
	provisionBody           []byte
	registrationBody        []byte
	response                *backend.StandardResponse
	err                     error
	deletedProvision        string
	deletedRegistration     string
	trainingBody            []byte
	trainingCreateID        string
	trainingNotification    []byte
	trainingCreateResponse  *backend.StandardResponse
	trainingCreateError     error
	trainingResponse        *backend.StandardResponse
	trainingError           error
	trainingPatchFunc       func(context.Context, string, []byte) (*backend.StandardResponse, error)
	trainingReplaceResponse *backend.StandardResponse
	trainingReplaceError    error
	trainingDeleteResponse  *backend.StandardResponse
	trainingDeleteError     error
	deletedTrainingBackend  string
}

func (s *mtlfMLModelBackendStub) CreateMLModelProvisionSubscription(
	_ context.Context, body []byte,
) (*backend.StandardResponse, error) {
	s.provisionBody = append([]byte(nil), body...)
	if s.response != nil || s.err != nil {
		return s.response, s.err
	}
	return &backend.StandardResponse{
		StatusCode:  http.StatusCreated,
		Location:    "http://mtlf.internal/internal/v1/ml-model-provision/subscriptions/" + testProvisionID,
		ContentType: "application/json",
		Body:        append([]byte(nil), body...),
	}, nil
}

func (s *mtlfMLModelBackendStub) ReplaceMLModelProvisionSubscription(
	_ context.Context, _ string, body []byte,
) (*backend.StandardResponse, error) {
	s.provisionBody = append([]byte(nil), body...)
	if s.response != nil || s.err != nil {
		return s.response, s.err
	}
	return &backend.StandardResponse{StatusCode: http.StatusNoContent}, nil
}

func (s *mtlfMLModelBackendStub) DeleteMLModelProvisionSubscription(
	_ context.Context, id string,
) (*backend.StandardResponse, error) {
	s.deletedProvision = id
	if s.response != nil || s.err != nil {
		return s.response, s.err
	}
	return &backend.StandardResponse{StatusCode: http.StatusNoContent}, nil
}

func (s *mtlfMLModelBackendStub) CreateMLModelMonitorRegistration(
	_ context.Context, body []byte,
) (*backend.StandardResponse, error) {
	s.registrationBody = append([]byte(nil), body...)
	if s.response != nil || s.err != nil {
		return s.response, s.err
	}
	return &backend.StandardResponse{
		StatusCode:  http.StatusCreated,
		Location:    "http://mtlf.internal/internal/v1/ml-model-monitor/registrations/" + testRegistrationID,
		ContentType: "application/json",
		Body:        append([]byte(nil), body...),
	}, nil
}

func (s *mtlfMLModelBackendStub) DeleteMLModelMonitorRegistration(
	_ context.Context, id string,
) (*backend.StandardResponse, error) {
	s.deletedRegistration = id
	if s.response != nil || s.err != nil {
		return s.response, s.err
	}
	return &backend.StandardResponse{StatusCode: http.StatusNoContent}, nil
}

func (s *mtlfMLModelBackendStub) DeliverMLModelMonitorNotification(
	_ context.Context,
	body []byte,
) (*backend.StandardResponse, error) {
	s.registrationBody = append([]byte(nil), body...)
	if s.response != nil || s.err != nil {
		return s.response, s.err
	}
	return &backend.StandardResponse{StatusCode: http.StatusNoContent}, nil
}

func (s *mtlfMLModelBackendStub) DeliverAdrfRetrievalNotification(
	_ context.Context,
	body []byte,
) (*backend.StandardResponse, error) {
	s.registrationBody = append([]byte(nil), body...)
	if s.response != nil || s.err != nil {
		return s.response, s.err
	}
	return &backend.StandardResponse{StatusCode: http.StatusNoContent}, nil
}

func (s *mtlfMLModelBackendStub) CreateMLModelTrainingSubscription(
	_ context.Context, body []byte, subscriptionID string,
) (*backend.StandardResponse, error) {
	s.trainingBody = append([]byte(nil), body...)
	s.trainingCreateID = subscriptionID
	if s.trainingCreateResponse != nil || s.trainingCreateError != nil {
		if s.trainingCreateResponse == nil {
			return nil, s.trainingCreateError
		}
		response := *s.trainingCreateResponse
		if response.StatusCode == http.StatusCreated && strings.HasPrefix(
			response.Location, "http://mtlf.internal/internal/v1/ml-model-training/subscriptions/",
		) {
			response.Location = "http://mtlf.internal/internal/v1/ml-model-training/subscriptions/" + subscriptionID
		}
		return &response, s.trainingCreateError
	}
	return &backend.StandardResponse{
		StatusCode: http.StatusCreated,
		Location: "http://mtlf.internal/internal/v1/ml-model-training/subscriptions/" +
			subscriptionID,
		ContentType: "application/json",
		Body:        append([]byte(nil), body...),
	}, nil
}

func (s *mtlfMLModelBackendStub) ReplaceMLModelTrainingSubscription(
	_ context.Context, _ string, body []byte,
) (*backend.StandardResponse, error) {
	s.trainingBody = append([]byte(nil), body...)
	if s.trainingReplaceResponse != nil || s.trainingReplaceError != nil {
		return s.trainingReplaceResponse, s.trainingReplaceError
	}
	return &backend.StandardResponse{StatusCode: http.StatusNoContent}, nil
}

func (s *mtlfMLModelBackendStub) PatchMLModelTrainingSubscription(
	ctx context.Context, id string, body []byte,
) (*backend.StandardResponse, error) {
	s.trainingBody = append([]byte(nil), body...)
	if s.trainingPatchFunc != nil {
		return s.trainingPatchFunc(ctx, id, body)
	}
	if s.trainingResponse != nil || s.trainingError != nil {
		return s.trainingResponse, s.trainingError
	}
	return &backend.StandardResponse{StatusCode: http.StatusNoContent}, nil
}

func (s *mtlfMLModelBackendStub) DeleteMLModelTrainingSubscription(
	_ context.Context, id string,
) (*backend.StandardResponse, error) {
	s.deletedTrainingBackend = id
	if s.trainingDeleteResponse != nil || s.trainingDeleteError != nil {
		return s.trainingDeleteResponse, s.trainingDeleteError
	}
	return &backend.StandardResponse{StatusCode: http.StatusNoContent}, nil
}

func (s *mtlfMLModelBackendStub) DeliverMLModelTrainingNotification(
	_ context.Context, body []byte,
) (*backend.StandardResponse, error) {
	s.trainingNotification = append([]byte(nil), body...)
	return &backend.StandardResponse{StatusCode: http.StatusNoContent}, nil
}

type anlfMLModelBackendStub struct {
	body     []byte
	response *backend.StandardResponse
	err      error
	deleted  string
}

type mlModelPeerConsumerStub struct {
	provisionTarget          backend.SelectedTarget
	provisionBody            []byte
	provisionLocation        string
	registrationTarget       backend.SelectedTarget
	registrationBody         []byte
	monitorTarget            backend.SelectedTarget
	monitorBody              []byte
	monitorLocation          string
	deletedProvision         string
	deletedRegistration      string
	deletedMonitor           string
	deletedTraining          []string
	trainingBody             []byte
	trainingCreateResponse   *backend.StandardResponse
	trainingCreateHook       func()
	trainingCreateFunc       func(backend.SelectedTarget, []byte) (*backend.StandardResponse, error)
	trainingCreateError      error
	trainingReplaceBody      []byte
	trainingPatchBody        []byte
	trainingPatchLocation    string
	provisionResponse        *backend.StandardResponse
	provisionError           error
	provisionReplaceResponse *backend.StandardResponse
	provisionReplaceError    error
	registrationResponse     *backend.StandardResponse
	registrationError        error
	monitorResponse          *backend.StandardResponse
	monitorError             error
	deleteProvisionErrors    []error
	deleteRegistrationErrors []error
	deleteMonitorErrors      []error
	trainingReplaceResponse  *backend.StandardResponse
	trainingReplaceError     error
}

type crossNodeMLModelPeerConsumer struct {
	*mlModelPeerConsumerStub
	deleteProvision func(context.Context, string) (*backend.StandardResponse, error)
	deleteMonitor   func(context.Context, string) (*backend.StandardResponse, error)
}

func (s *crossNodeMLModelPeerConsumer) DeletePeerMLModelProvision(
	ctx context.Context,
	location string,
) (*backend.StandardResponse, error) {
	if s.deleteProvision != nil {
		return s.deleteProvision(ctx, location)
	}
	return s.mlModelPeerConsumerStub.DeletePeerMLModelProvision(ctx, location)
}

func (s *crossNodeMLModelPeerConsumer) DeletePeerMLModelMonitorSubscription(
	ctx context.Context,
	location string,
) (*backend.StandardResponse, error) {
	if s.deleteMonitor != nil {
		return s.deleteMonitor(ctx, location)
	}
	return s.mlModelPeerConsumerStub.DeletePeerMLModelMonitorSubscription(ctx, location)
}

type isolatedMLModelTestApp struct {
	*subscriptionTestApp
	nwdafContext *nwdaf_context.NWDAFContext
}

func (a *isolatedMLModelTestApp) Context() *nwdaf_context.NWDAFContext {
	return a.nwdafContext
}

func (s *mlModelPeerConsumerStub) CreatePeerMLModelTraining(
	_ context.Context, target backend.SelectedTarget, body []byte,
) (*backend.StandardResponse, error) {
	s.trainingBody = append([]byte(nil), body...)
	if s.trainingCreateFunc != nil {
		return s.trainingCreateFunc(target, body)
	}
	if s.trainingCreateHook != nil {
		s.trainingCreateHook()
	}
	if s.trainingCreateResponse != nil || s.trainingCreateError != nil {
		return s.trainingCreateResponse, s.trainingCreateError
	}
	return &backend.StandardResponse{
		StatusCode:   http.StatusCreated,
		Location:     "/nnwdaf-mlmodeltraining/v1/subscriptions/" + testProvisionID,
		EffectiveURI: "http://nwdaf-a.example/nnwdaf-mlmodeltraining/v1/subscriptions",
		ContentType:  "application/json",
		Body:         append([]byte(nil), body...),
	}, nil
}

func (s *mlModelPeerConsumerStub) ReplacePeerMLModelTraining(
	_ context.Context, _ string, body []byte,
) (*backend.StandardResponse, error) {
	s.trainingReplaceBody = append([]byte(nil), body...)
	if s.trainingReplaceResponse != nil || s.trainingReplaceError != nil {
		return s.trainingReplaceResponse, s.trainingReplaceError
	}
	return &backend.StandardResponse{StatusCode: http.StatusNoContent}, nil
}

func (s *mlModelPeerConsumerStub) PatchPeerMLModelTraining(
	_ context.Context, location string, body []byte,
) (*backend.StandardResponse, error) {
	s.trainingPatchBody = append([]byte(nil), body...)
	s.trainingPatchLocation = location
	return &backend.StandardResponse{StatusCode: http.StatusNoContent}, nil
}

func (s *mlModelPeerConsumerStub) DeletePeerMLModelTraining(
	_ context.Context, location string,
) (*backend.StandardResponse, error) {
	s.deletedTraining = append(s.deletedTraining, location)
	return &backend.StandardResponse{StatusCode: http.StatusNoContent}, nil
}

func (s *mlModelPeerConsumerStub) CreatePeerMLModelProvision(
	_ context.Context,
	target backend.SelectedTarget,
	body []byte,
) (*backend.StandardResponse, error) {
	s.provisionTarget = target
	s.provisionBody = append([]byte(nil), body...)
	if s.provisionResponse != nil || s.provisionError != nil {
		return s.provisionResponse, s.provisionError
	}
	return &backend.StandardResponse{
		StatusCode:   http.StatusCreated,
		Location:     "/nnwdaf-mlmodelprovision/v1/subscriptions/peer-provision",
		EffectiveURI: "http://nwdaf-c.example/nnwdaf-mlmodelprovision/v1/subscriptions",
		ContentType:  "application/json",
		Body:         append([]byte(nil), body...),
	}, nil
}

func (s *mlModelPeerConsumerStub) ReplacePeerMLModelProvision(
	_ context.Context,
	location string,
	body []byte,
) (*backend.StandardResponse, error) {
	s.provisionLocation = location
	s.provisionBody = append([]byte(nil), body...)
	if s.provisionReplaceResponse != nil || s.provisionReplaceError != nil {
		return s.provisionReplaceResponse, s.provisionReplaceError
	}
	return &backend.StandardResponse{
		StatusCode:   http.StatusOK,
		EffectiveURI: location,
		ContentType:  "application/json",
		Body:         append([]byte(nil), body...),
	}, nil
}

func (s *mlModelPeerConsumerStub) DeletePeerMLModelProvision(
	_ context.Context,
	location string,
) (*backend.StandardResponse, error) {
	s.deletedProvision = location
	if len(s.deleteProvisionErrors) > 0 {
		err := s.deleteProvisionErrors[0]
		s.deleteProvisionErrors = s.deleteProvisionErrors[1:]
		if err != nil {
			return nil, err
		}
	}
	return &backend.StandardResponse{StatusCode: http.StatusNoContent}, nil
}

func (s *mlModelPeerConsumerStub) CreatePeerMLModelMonitorRegistration(
	_ context.Context,
	target backend.SelectedTarget,
	body []byte,
) (*backend.StandardResponse, error) {
	s.registrationTarget = target
	s.registrationBody = append([]byte(nil), body...)
	if s.registrationResponse != nil || s.registrationError != nil {
		return s.registrationResponse, s.registrationError
	}
	return &backend.StandardResponse{
		StatusCode:   http.StatusCreated,
		Location:     "peer-registration",
		EffectiveURI: "http://nwdaf-c.example/nnwdaf-mlmodelmonitor/v1/registrations",
		ContentType:  "application/json",
		Body:         append([]byte(nil), body...),
	}, nil
}

func (s *mlModelPeerConsumerStub) DeletePeerMLModelMonitorRegistration(
	_ context.Context,
	location string,
) (*backend.StandardResponse, error) {
	s.deletedRegistration = location
	if len(s.deleteRegistrationErrors) > 0 {
		err := s.deleteRegistrationErrors[0]
		s.deleteRegistrationErrors = s.deleteRegistrationErrors[1:]
		if err != nil {
			return nil, err
		}
	}
	return &backend.StandardResponse{StatusCode: http.StatusNoContent}, nil
}

func (s *mlModelPeerConsumerStub) CreatePeerMLModelMonitorSubscription(
	_ context.Context,
	target backend.SelectedTarget,
	body []byte,
) (*backend.StandardResponse, error) {
	s.monitorTarget = target
	s.monitorBody = append([]byte(nil), body...)
	if s.monitorResponse != nil || s.monitorError != nil {
		return s.monitorResponse, s.monitorError
	}
	return &backend.StandardResponse{
		StatusCode:   http.StatusCreated,
		Location:     "peer-monitor",
		EffectiveURI: "http://nwdaf-a.example/nnwdaf-mlmodelmonitor/v1/subscriptions",
		ContentType:  "application/json",
		Body:         append([]byte(nil), body...),
	}, nil
}

func (s *mlModelPeerConsumerStub) ReplacePeerMLModelMonitorSubscription(
	_ context.Context,
	location string,
	body []byte,
) (*backend.StandardResponse, error) {
	s.monitorLocation = location
	s.monitorBody = append([]byte(nil), body...)
	return &backend.StandardResponse{
		StatusCode:   http.StatusOK,
		EffectiveURI: location,
		ContentType:  "application/json",
		Body:         append([]byte(nil), body...),
	}, nil
}

func (s *mlModelPeerConsumerStub) DeletePeerMLModelMonitorSubscription(
	_ context.Context,
	location string,
) (*backend.StandardResponse, error) {
	s.deletedMonitor = location
	if len(s.deleteMonitorErrors) > 0 {
		err := s.deleteMonitorErrors[0]
		s.deleteMonitorErrors = s.deleteMonitorErrors[1:]
		if err != nil {
			return nil, err
		}
	}
	return &backend.StandardResponse{StatusCode: http.StatusNoContent}, nil
}

func (s *anlfMLModelBackendStub) DeliverMLModelProvisionNotification(
	_ context.Context,
	_ string,
	body []byte,
) (*backend.StandardResponse, error) {
	s.body = append([]byte(nil), body...)
	if s.response != nil || s.err != nil {
		return s.response, s.err
	}
	return &backend.StandardResponse{StatusCode: http.StatusNoContent}, nil
}

func (s *anlfMLModelBackendStub) CreateMLModelMonitorSubscription(
	_ context.Context, body []byte,
) (*backend.StandardResponse, error) {
	s.body = append([]byte(nil), body...)
	if s.response != nil || s.err != nil {
		return s.response, s.err
	}
	return &backend.StandardResponse{
		StatusCode:  http.StatusCreated,
		Location:    "http://anlf.internal/internal/v1/ml-model-monitor/subscriptions/" + testMonitorID,
		ContentType: "application/json",
		Body:        append([]byte(nil), body...),
	}, nil
}

func (s *anlfMLModelBackendStub) ReplaceMLModelMonitorSubscription(
	_ context.Context, _ string, body []byte,
) (*backend.StandardResponse, error) {
	s.body = append([]byte(nil), body...)
	if s.response != nil || s.err != nil {
		return s.response, s.err
	}
	return &backend.StandardResponse{StatusCode: http.StatusNoContent}, nil
}

func (s *anlfMLModelBackendStub) DeleteMLModelMonitorSubscription(
	_ context.Context, id string,
) (*backend.StandardResponse, error) {
	s.deleted = id
	if s.response != nil || s.err != nil {
		return s.response, s.err
	}
	return &backend.StandardResponse{StatusCode: http.StatusNoContent}, nil
}

func newMLModelProcessorTestSubject() (
	*Processor,
	*nwdaf_context.NWDAFContext,
	*mtlfMLModelBackendStub,
	*anlfMLModelBackendStub,
	*mlModelAvailabilityStub,
	*mlModelAvailabilityStub,
) {
	ctx := setupTestContext()
	app := &subscriptionTestApp{
		ctx: context.Background(),
		cfg: &factory.Config{Configuration: &factory.Configuration{
			Sbi: &factory.Sbi{Scheme: "http", RegisterIPv4: "192.0.2.10", Port: 8080},
			Anlf: &factory.AnlfConfig{Server: &factory.AuxiliaryServerConfig{
				RegisterIPv4: "192.0.2.20", Port: 8090,
			}},
			Mtlf: &factory.MtlfConfig{Server: &factory.AuxiliaryServerConfig{
				RegisterIPv4: "192.0.2.21", Port: 8091,
			}},
		}},
	}
	mtlfBackend := &mtlfMLModelBackendStub{}
	anlfBackend := &anlfMLModelBackendStub{}
	mtlfAvailability := &mlModelAvailabilityStub{
		usable: true, generation: "44444444-4444-4444-8444-444444444444",
	}
	anlfAvailability := &mlModelAvailabilityStub{
		usable: true, generation: "55555555-5555-4555-8555-555555555555",
	}
	processor := &Processor{nwdaf: app}
	processor.SetMLModelBackends(mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability)
	return processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability
}

func TestMLModelProvisionRoutingPreservesUnknownFieldsAndExternalURI(t *testing.T) {
	processor, ctx, mtlfBackend, _, availability, _ := newMLModelProcessorTestSubject()
	body := []byte(`{
		"mLEventSubscs":[{"mLEvent":"UE_COMMUNICATION","mLEventFilter":{},"futureNested":true}],
		"notifUri":"http://consumer.example/provision",
		"notifCorreId":"corr-1",
		"futureTopLevel":{"release":18}
	}`)
	response, problem := processor.HandleCreateMLModelProvision(context.Background(), body)
	if problem != nil {
		t.Fatalf("HandleCreateMLModelProvision() problem = %+v", problem)
	}
	localProvisionID, err := backend.ResourceIDFromLocation(response.Location)
	if err != nil || response.StatusCode != http.StatusCreated || localProvisionID == testProvisionID {
		t.Fatalf("response = %+v", response)
	}
	if strings.Contains(string(mtlfBackend.provisionBody), "consumer.example") ||
		!strings.Contains(string(mtlfBackend.provisionBody), "192.0.2.21:8091") {
		t.Fatalf("backend body did not internalize callback URI: %s", mtlfBackend.provisionBody)
	}
	if !strings.Contains(string(response.Body), "consumer.example") ||
		!strings.Contains(string(response.Body), "futureTopLevel") {
		t.Fatalf("external response lost URI or unknown field: %s", response.Body)
	}
	route, found := ctx.GetMLModelProvisionSubscriptionRoute(localProvisionID)
	if !found || route.DestinationNotificationURI != "http://consumer.example/provision" ||
		route.PeerRoute.BackendResourceID != testProvisionID ||
		!strings.Contains(string(route.BackendRepresentation), "192.0.2.21:8091") {
		t.Fatalf("route = %+v found=%v", route, found)
	}
	if availability.refreshed != 1 {
		t.Fatalf("refresh count = %d, want 1", availability.refreshed)
	}

	replacement := []byte(`{
		"mLEventSubscs":[{"mLEvent":"UE_COMMUNICATION","mLEventFilter":{}}],
		"notifUri":"http://consumer.example/replaced"
	}`)
	response, problem = processor.HandleReplaceMLModelProvision(
		context.Background(), localProvisionID, replacement,
	)
	if problem != nil || response.StatusCode != http.StatusNoContent || len(response.Body) != 0 {
		t.Fatalf("replace response=%+v problem=%+v", response, problem)
	}
	route, _ = ctx.GetMLModelProvisionSubscriptionRoute(localProvisionID)
	if route.DestinationNotificationURI != "http://consumer.example/replaced" {
		t.Fatalf("updated route = %+v", route)
	}

	response, problem = processor.HandleDeleteMLModelProvision(context.Background(), localProvisionID)
	if problem != nil || response.StatusCode != http.StatusNoContent || mtlfBackend.deletedProvision != testProvisionID {
		t.Fatalf("delete response=%+v problem=%+v backend=%+v", response, problem, mtlfBackend)
	}
	if _, found = ctx.GetMLModelProvisionSubscriptionRoute(localProvisionID); found {
		t.Fatal("provision route remains after delete")
	}
}

func TestMLModelProvisionBackendInitiatorAndNotificationRouting(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
	_ = mtlfBackend
	_ = mtlfAvailability
	_ = anlfAvailability
	body := []byte(`{
		"mLEventSubscs":[{"mLEvent":"UE_COMMUNICATION","mLEventFilter":{}}],
		"notifUri":"http://anlf.backend/standard-provision",
		"notifCorreId":"corr-internal"
	}`)
	response, problem := processor.HandleCreateMLModelProvisionFromBackend(
		context.Background(),
		body,
		nil,
	)
	if problem != nil || response.StatusCode != http.StatusCreated {
		t.Fatalf("create response=%+v problem=%+v", response, problem)
	}
	localProvisionID, err := backend.ResourceIDFromLocation(response.Location)
	if err != nil {
		t.Fatal(err)
	}
	route, found := ctx.GetMLModelProvisionSubscriptionRoute(localProvisionID)
	if !found ||
		route.Initiator != nwdaf_context.MLModelRoutePartyAnLFBackend ||
		route.Destination != nwdaf_context.MLModelRoutePartyAnLFBackend {
		t.Fatalf("internal route=%+v found=%v", route, found)
	}
	notification := []byte(`[{
		"subscriptionId":"` + testProvisionID + `",
		"eventNotifs":[{
			"event":"UE_COMMUNICATION",
			"notifCorreId":"corr-internal",
			"modelUniqueId":1,
			"mLFileAddr":{"mLModelUrl":"http://mtlf.example/artifacts/1"}
		}]
	}]`)
	response, problem = processor.HandleMLModelProvisionNotification(
		context.Background(),
		"",
		notification,
	)
	if problem != nil || response.StatusCode != http.StatusNoContent {
		t.Fatalf("notification response=%+v problem=%+v", response, problem)
	}
	if !bytes.Equal(anlfBackend.body, notification) {
		var delivered []map[string]any
		if decodeErr := json.Unmarshal(anlfBackend.body, &delivered); decodeErr != nil ||
			delivered[0]["subscriptionId"] != localProvisionID {
			t.Fatalf("AnLF backend notification=%s", anlfBackend.body)
		}
	}

	updatedNotification := []byte(`[{
		"subscriptionId":"` + testProvisionID + `",
		"eventNotifs":[{
			"event":"UE_COMMUNICATION",
			"notifCorreId":"corr-internal",
			"modelUniqueId":2,
			"mLFileAddr":{"mLModelUrl":"http://mtlf.example/artifacts/2"},
			"validityPeriod":{
				"startTime":"2026-07-25T00:00:00Z",
				"stopTime":"2026-07-26T00:00:00Z"
			}
		}]
	}]`)
	response, problem = processor.HandleMLModelProvisionNotification(
		context.Background(),
		"",
		updatedNotification,
	)
	if problem != nil || response.StatusCode != http.StatusNoContent {
		t.Fatalf("updated notification response=%+v problem=%+v", response, problem)
	}
	var delivered []map[string]any
	if decodeErr := json.Unmarshal(anlfBackend.body, &delivered); decodeErr != nil ||
		delivered[0]["subscriptionId"] != localProvisionID {
		t.Fatalf("updated AnLF backend notification=%s", anlfBackend.body)
	}
}

func TestMLModelProvisionNotificationReachesExternalURI(t *testing.T) {
	delivered := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(
		response http.ResponseWriter,
		request *http.Request,
	) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read callback: %v", err)
		}
		delivered <- body
		response.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
	_ = ctx
	_ = mtlfBackend
	_ = anlfBackend
	_ = mtlfAvailability
	_ = anlfAvailability
	processor.SetMLModelHTTPClient(server.Client())
	createBody := []byte(`{
		"mLEventSubscs":[{"mLEvent":"UE_COMMUNICATION","mLEventFilter":{}}],
		"notifUri":"` + server.URL + `",
		"notifCorreId":"corr-external"
	}`)
	created, problem := processor.HandleCreateMLModelProvision(
		context.Background(),
		createBody,
	)
	if problem != nil {
		t.Fatal(problem)
	}
	localProvisionID, err := backend.ResourceIDFromLocation(created.Location)
	if err != nil {
		t.Fatal(err)
	}
	notification := []byte(`[{
		"subscriptionId":"` + testProvisionID + `",
		"eventNotifs":[{
			"event":"UE_COMMUNICATION",
			"notifCorreId":"corr-external",
			"mLFileAddr":{"mLModelUrl":"http://mtlf.example/artifacts/1"}
		}]
	}]`)
	response, problem := processor.HandleMLModelProvisionNotification(
		context.Background(),
		"",
		notification,
	)
	if problem != nil || response.StatusCode != http.StatusNoContent {
		t.Fatalf("response=%+v problem=%+v", response, problem)
	}
	if body := <-delivered; !strings.Contains(string(body), localProvisionID) {
		t.Fatalf("external body=%s", body)
	}
}

func TestMLModelMonitorResourcesRouteToTheirOwners(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
	registration := []byte(`{
		"consumerId":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		"modelId":7,
		"futureRegistration":true
	}`)
	response, problem := processor.HandleCreateMLModelMonitorRegistration(context.Background(), registration)
	if problem != nil || response.StatusCode != http.StatusCreated || len(mtlfBackend.registrationBody) == 0 {
		t.Fatalf("registration response=%+v problem=%+v", response, problem)
	}
	localRegistrationID, err := backend.ResourceIDFromLocation(response.Location)
	if err != nil {
		t.Fatal(err)
	}
	if route, found := ctx.GetMLModelMonitorRegistrationRoute(localRegistrationID); !found ||
		route.PeerRoute.BackendResourceID != testRegistrationID {
		t.Fatal("registration route was not recorded")
	}

	subscription := []byte(`{
		"modelIds":[7],
		"notificationUri":"http://consumer.example/accuracy",
		"notifCorrId":"accuracy-7",
		"futureSubscription":{"release":18}
	}`)
	response, problem = processor.HandleCreateMLModelMonitorSubscription(context.Background(), subscription)
	if problem != nil || response.StatusCode != http.StatusCreated || len(anlfBackend.body) == 0 {
		t.Fatalf("subscription response=%+v problem=%+v", response, problem)
	}
	if strings.Contains(string(anlfBackend.body), "consumer.example") ||
		!strings.Contains(string(response.Body), "futureSubscription") {
		t.Fatalf("monitor URI separation failed: backend=%s response=%s", anlfBackend.body, response.Body)
	}
	localMonitorID, err := backend.ResourceIDFromLocation(response.Location)
	if err != nil {
		t.Fatal(err)
	}
	if route, found := ctx.GetMLModelMonitorSubscriptionRoute(localMonitorID); !found ||
		route.PeerRoute.BackendResourceID != testMonitorID {
		t.Fatal("monitor subscription route was not recorded")
	}
	if mtlfAvailability.refreshed != 1 || anlfAvailability.refreshed != 1 {
		t.Fatalf("refresh counts: mtlf=%d anlf=%d", mtlfAvailability.refreshed, anlfAvailability.refreshed)
	}
}

func TestMLModelMonitorBackendSubscriptionRoutesNotificationToMTLFBackend(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, _, _ := newMLModelProcessorTestSubject()
	subscription := []byte(`{
		"modelIds":[7],
		"notificationUri":"http://mtlf.backend/internal/v1/ml-model-monitor/notifications",
		"notifCorrId":"accuracy-7",
		"modelMetric":"ACCURACY",
		"mLEvent":"UE_COMMUNICATION",
		"mLEventFilter":{},
		"tgtUe":{"intGroupIds":["group-a"]}
	}`)
	response, problem := processor.HandleCreateMLModelMonitorSubscriptionFromBackend(
		context.Background(),
		subscription,
		"registration-7",
		nil,
	)
	if problem != nil || response == nil || response.StatusCode != http.StatusCreated {
		t.Fatalf("create response=%+v problem=%+v", response, problem)
	}
	localMonitorID, err := backend.ResourceIDFromLocation(response.Location)
	if err != nil {
		t.Fatal(err)
	}
	route, found := ctx.GetMLModelMonitorSubscriptionRoute(localMonitorID)
	if !found || route.Destination != nwdaf_context.MLModelRoutePartyMTLFBackend {
		t.Fatalf("route=%+v found=%v", route, found)
	}
	if route.OwnerRegistrationID != "registration-7" {
		t.Fatalf("owner registration ID=%q", route.OwnerRegistrationID)
	}
	if strings.Contains(string(anlfBackend.body), "mtlf.backend") ||
		!strings.Contains(string(anlfBackend.body), "192.0.2.20:8090"+mlModelMonitorCallbackPath) {
		t.Fatalf("AnLF backend representation=%s", anlfBackend.body)
	}

	notification := []byte(`{
		"notifCorrId":"accuracy-7",
		"modelAccuInfos":[{
			"modelId":7,
			"deviation":0.2,
			"inferenceNum":3,
			"modelMetric":"ACCURACY",
			"monitorInterval":{
				"startTime":"2026-01-01T00:00:00Z",
				"stopTime":"2026-01-01T00:01:30Z"
			}
		}],
		"mLEvent":"UE_COMMUNICATION"
	}`)
	response, problem = processor.HandleMLModelMonitorNotification(
		context.Background(),
		"",
		notification,
	)
	if problem != nil || response == nil || response.StatusCode != http.StatusNoContent {
		t.Fatalf("notification response=%+v problem=%+v", response, problem)
	}
	if !bytes.Equal(mtlfBackend.registrationBody, notification) {
		t.Fatalf("MTLF backend notification=%s", mtlfBackend.registrationBody)
	}
}

func TestMLModelBackendFailureMapping(t *testing.T) {
	validBody := []byte(`{
		"mLEventSubscs":[{"mLEvent":"UE_COMMUNICATION","mLEventFilter":{}}],
		"notifUri":"http://consumer.example/provision"
	}`)
	tests := []struct {
		name       string
		err        error
		usable     bool
		wantStatus int32
		wantMarked int
	}{
		{name: "unavailable", usable: false, wantStatus: http.StatusServiceUnavailable},
		{
			name: "standard problem", usable: true, wantStatus: http.StatusBadRequest,
			err: &backend.StandardError{StatusCode: http.StatusBadRequest, ProblemDetails: models.ProblemDetails{
				Status: http.StatusBadRequest, Cause: "INVALID_REQUEST",
			}},
		},
		{
			name: "malformed backend success", usable: true, wantStatus: http.StatusBadGateway,
			err: &backend.ContractError{Operation: "create", Detail: "missing Location"},
		},
		{
			name: "transport failure", usable: true, wantStatus: http.StatusServiceUnavailable,
			err:        &backend.TransportError{Operation: "create", Cause: errors.New("connection refused")},
			wantMarked: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			processor, _, mtlfBackend, _, availability, _ := newMLModelProcessorTestSubject()
			availability.usable = test.usable
			mtlfBackend.err = test.err
			response, problem := processor.HandleCreateMLModelProvision(context.Background(), validBody)
			if response != nil || problem == nil || problem.Status != test.wantStatus {
				t.Fatalf("response=%+v problem=%+v", response, problem)
			}
			if availability.marked != test.wantMarked {
				t.Fatalf("MarkUnavailable calls = %d, want %d", availability.marked, test.wantMarked)
			}
		})
	}
}

func TestMLModelPrivateBackendRedirectIsNotExposed(t *testing.T) {
	processor, ctx, mtlfBackend, anlf, mtlfState, anlfState := newMLModelProcessorTestSubject()
	_ = ctx
	_ = anlf
	_ = mtlfState
	_ = anlfState
	body := []byte(`{
		"mLEventSubscs":[{"mLEvent":"UE_COMMUNICATION","mLEventFilter":{}}],
		"notifUri":"http://consumer.example/provision"
	}`)
	created, problem := processor.HandleCreateMLModelProvision(context.Background(), body)
	if problem != nil {
		t.Fatalf("create problem = %+v", problem)
	}
	localProvisionID, err := backend.ResourceIDFromLocation(created.Location)
	if err != nil {
		t.Fatal(err)
	}
	mtlfBackend.response = &backend.StandardResponse{
		StatusCode: http.StatusTemporaryRedirect,
		Location:   "http://mtlf.internal/private/resource",
	}
	response, problem := processor.HandleReplaceMLModelProvision(
		context.Background(),
		localProvisionID,
		body,
	)
	if response != nil || problem == nil || problem.Status != http.StatusBadGateway {
		t.Fatalf("response=%+v problem=%+v, want 502 without private redirect", response, problem)
	}
}

func TestMLModelMonitorRegistrationRawRepresentationIsCopied(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
	_ = mtlfBackend
	_ = anlfBackend
	_ = mtlfAvailability
	_ = anlfAvailability
	body := []byte(`{"consumerId":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","modelId":7}`)
	response, problem := processor.HandleCreateMLModelMonitorRegistration(context.Background(), body)
	if problem != nil {
		t.Fatal(problem)
	}
	localRegistrationID, err := backend.ResourceIDFromLocation(response.Location)
	if err != nil {
		t.Fatal(err)
	}
	route, _ := ctx.GetMLModelMonitorRegistrationRoute(localRegistrationID)
	var value map[string]json.RawMessage
	decodeErr := json.Unmarshal(route.AcceptedRepresentation, &value)
	if decodeErr != nil || value["modelId"] == nil {
		t.Fatalf("stored representation = %s error=%v", route.AcceptedRepresentation, decodeErr)
	}
}

func TestRemoteMLModelProvisionUsesLocalIdentityAndRelaysCallback(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
	_ = mtlfBackend
	_ = mtlfAvailability
	_ = anlfAvailability
	peer := &mlModelPeerConsumerStub{}
	processor.SetMLModelPeerConsumer(peer)
	target := backend.SelectedTarget{
		NFInstanceID:        "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
		NFServiceInstanceID: "model-provision-c",
		ServiceName:         "nnwdaf-mlmodelprovision",
		APIRoot:             "http://nwdaf-c.example",
		SelectionSource:     backend.SelectionSourceNRF,
	}
	body := []byte(`{
		"mLEventSubscs":[{"mLEvent":"UE_COMMUNICATION","mLEventFilter":{}}],
		"notifUri":"http://anlf-a.internal/provision",
		"notifCorreId":"corr-a",
		"suppFeats":"8"
	}`)
	response, problem := processor.HandleCreateMLModelProvisionFromBackend(
		context.Background(),
		body,
		&target,
	)
	if problem != nil || response == nil || response.StatusCode != http.StatusCreated {
		t.Fatalf("create response=%+v problem=%+v", response, problem)
	}
	localRouteID, err := backend.ResourceIDFromLocation(response.Location)
	if err != nil {
		t.Fatal(err)
	}
	if localRouteID == "peer-provision" ||
		!strings.Contains(string(peer.provisionBody), "/nnwdaf-callback/v1/ml-model-provision/"+localRouteID) ||
		strings.Contains(string(response.Body), "X-NWDAF-Target") {
		t.Fatalf("local identity or callback rewrite failed: response=%s peer=%s", response.Body, peer.provisionBody)
	}
	route, found := ctx.GetMLModelProvisionSubscriptionRoute(localRouteID)
	if !found || route.PeerRoute.SelectedTarget == nil ||
		route.PeerRoute.SelectedTarget.NFInstanceID != target.NFInstanceID ||
		!strings.HasSuffix(route.PeerRoute.PeerLocation, "/peer-provision") {
		t.Fatalf("remote route=%+v found=%v", route, found)
	}

	notification := []byte(`[{
		"subscriptionId":"peer-provision",
		"eventNotifs":[{
			"event":"UE_COMMUNICATION",
			"notifCorreId":"corr-a",
			"modelUniqueId":1,
			"mLFileAddr":{"mLModelUrl":"http://nwdaf-c.example/artifacts/m1"}
		}]
	}]`)
	spoofed := bytes.Replace(
		notification,
		[]byte(`"subscriptionId":"peer-provision"`),
		[]byte(`"subscriptionId":"`+localRouteID+`"`),
		1,
	)
	if _, spoofProblem := processor.HandleMLModelProvisionNotification(
		context.Background(),
		localRouteID,
		spoofed,
	); spoofProblem == nil || spoofProblem.Status != http.StatusBadRequest {
		t.Fatalf("local-ID callback spoof problem=%+v", spoofProblem)
	}
	callbackResponse, callbackProblem := processor.HandleMLModelProvisionNotification(
		context.Background(),
		localRouteID,
		notification,
	)
	if callbackProblem != nil || callbackResponse.StatusCode != http.StatusNoContent ||
		!strings.Contains(string(anlfBackend.body), localRouteID) {
		t.Fatalf(
			"callback response=%+v problem=%+v body=%s",
			callbackResponse,
			callbackProblem,
			anlfBackend.body,
		)
	}

	deleteResponse, deleteProblem := processor.HandleDeleteMLModelProvisionFromBackend(
		context.Background(),
		localRouteID,
	)
	if deleteProblem != nil || deleteResponse.StatusCode != http.StatusNoContent ||
		!strings.HasSuffix(peer.deletedProvision, "/peer-provision") {
		t.Fatalf(
			"delete response=%+v problem=%+v peerLocation=%s",
			deleteResponse,
			deleteProblem,
			peer.deletedProvision,
		)
	}
}

func TestSelfDiscoveredModelProvisionUsesLocalMTLFBackend(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
	_ = anlfBackend
	peer := &mlModelPeerConsumerStub{}
	processor.SetMLModelPeerConsumer(peer)
	target := backend.SelectedTarget{
		NFInstanceID:        ctx.NfId,
		NFServiceInstanceID: "model-provision-self",
		ServiceName:         "nnwdaf-mlmodelprovision",
		APIRoot:             "http://self.example",
		SelectionSource:     backend.SelectionSourceConfigured,
	}
	body := []byte(`{
		"mLEventSubscs":[{"mLEvent":"UE_COMMUNICATION","mLEventFilter":{}}],
		"notifUri":"http://anlf.backend/provision",
		"notifCorreId":"corr-self",
		"suppFeats":"8"
	}`)

	response, problem := processor.HandleCreateMLModelProvisionFromBackend(
		context.Background(),
		body,
		&target,
	)

	if problem != nil || response == nil || response.StatusCode != http.StatusCreated {
		t.Fatalf("create response=%+v problem=%+v", response, problem)
	}
	if len(peer.provisionBody) != 0 {
		t.Fatal("self-discovered target was sent through the peer HTTP consumer")
	}
	if len(mtlfBackend.provisionBody) == 0 {
		t.Fatal("self-discovered target did not reach the local MTLF backend")
	}
	localRouteID, err := backend.ResourceIDFromLocation(response.Location)
	if err != nil {
		t.Fatal(err)
	}
	route, found := ctx.GetMLModelProvisionSubscriptionRoute(localRouteID)
	if !found || route.Initiator != nwdaf_context.MLModelRoutePartyAnLFBackend ||
		route.Destination != nwdaf_context.MLModelRoutePartyAnLFBackend ||
		route.PeerRoute.SelectedTarget != nil ||
		route.PeerRoute.ProcessGeneration != mtlfAvailability.generation ||
		route.PeerRoute.RelatedBackend != backend.KindAnLF ||
		route.PeerRoute.RelatedGeneration != anlfAvailability.generation {
		t.Fatalf("local provision route=%+v found=%v", route, found)
	}
}

func TestSelfDiscoveredMonitorRegistrationUsesLocalMTLFBackend(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
	_ = anlfBackend
	peer := &mlModelPeerConsumerStub{}
	processor.SetMLModelPeerConsumer(peer)
	target := backend.SelectedTarget{
		NFInstanceID:        ctx.NfId,
		NFServiceInstanceID: "model-monitor-self",
		ServiceName:         "nnwdaf-mlmodelmonitor",
		APIRoot:             "http://self.example",
		SelectionSource:     backend.SelectionSourceConfigured,
	}
	body := []byte(`{
		"consumerId":"` + ctx.NfId + `",
		"modelId":1,
		"modelAccuInd":true,
		"mLEvent":"UE_COMMUNICATION",
		"mLEventFilter":{}
	}`)

	response, problem := processor.HandleCreateMLModelMonitorRegistrationFromBackend(
		context.Background(),
		body,
		&target,
	)

	if problem != nil || response == nil || response.StatusCode != http.StatusCreated {
		t.Fatalf("create response=%+v problem=%+v", response, problem)
	}
	if len(peer.registrationBody) != 0 {
		t.Fatal("self-discovered target was sent through the peer HTTP consumer")
	}
	if len(mtlfBackend.registrationBody) == 0 {
		t.Fatal("self-discovered target did not reach the local MTLF backend")
	}
	localRouteID, err := backend.ResourceIDFromLocation(response.Location)
	if err != nil {
		t.Fatal(err)
	}
	route, found := ctx.GetMLModelMonitorRegistrationRoute(localRouteID)
	if !found || route.Initiator != nwdaf_context.MLModelRoutePartyAnLFBackend ||
		route.PeerRoute.SelectedTarget != nil ||
		route.PeerRoute.ProcessGeneration != mtlfAvailability.generation ||
		route.PeerRoute.RelatedBackend != backend.KindAnLF ||
		route.PeerRoute.RelatedGeneration != anlfAvailability.generation {
		t.Fatalf("local monitor registration route=%+v found=%v", route, found)
	}
}

func TestRemoteMonitorSubscriptionRejectsUnknownOwner(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
	_ = ctx
	_ = mtlfBackend
	_ = anlfBackend
	_ = mtlfAvailability
	_ = anlfAvailability
	peer := &mlModelPeerConsumerStub{}
	processor.SetMLModelPeerConsumer(peer)
	target := backend.SelectedTarget{
		NFInstanceID:        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		NFServiceInstanceID: "model-monitor-a",
		ServiceName:         "nnwdaf-mlmodelmonitor",
		APIRoot:             "http://nwdaf-a.example",
		SelectionSource:     backend.SelectionSourceNRF,
	}
	body := []byte(`{
		"modelIds":[7],
		"notificationUri":"http://mtlf-c.internal/monitor",
		"notifCorrId":"scope-a-generation-1",
		"mLEvent":"UE_COMMUNICATION"
	}`)
	response, problem := processor.HandleCreateMLModelMonitorSubscriptionFromBackend(
		context.Background(),
		body,
		"unknown-registration",
		&target,
	)
	if response != nil || problem == nil || problem.Status != http.StatusBadRequest {
		t.Fatalf("response=%+v problem=%+v", response, problem)
	}
	if len(peer.monitorBody) != 0 {
		t.Fatal("peer create ran before owner validation")
	}
}

func TestSelfDiscoveredMonitorSubscriptionUsesLocalAnLFBackend(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
	_ = mtlfBackend
	peer := &mlModelPeerConsumerStub{}
	processor.SetMLModelPeerConsumer(peer)
	if !ctx.AddMLModelMonitorRegistrationRoute(
		nwdaf_context.MLModelMonitorRegistrationRoute{
			RegistrationID: "registration-go",
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				Direction:         nwdaf_context.MLModelRouteDirectionInbound,
				BackendResourceID: "registration-mtlf",
				LifecycleState:    nwdaf_context.MLModelRouteActive,
			},
		},
	) {
		t.Fatal("could not seed local monitor registration route")
	}
	target := backend.SelectedTarget{
		NFInstanceID:        ctx.NfId,
		NFServiceInstanceID: "model-monitor-self",
		ServiceName:         "nnwdaf-mlmodelmonitor",
		APIRoot:             "http://self.example",
		SelectionSource:     backend.SelectionSourceNRF,
	}
	body := []byte(`{
		"modelIds":[7],
		"notificationUri":"http://mtlf.backend/monitor",
		"notifCorrId":"scope-a-generation-1",
		"modelMetric":"ACCURACY",
		"mLEvent":"UE_COMMUNICATION"
	}`)
	response, problem := processor.HandleCreateMLModelMonitorSubscriptionFromBackend(
		context.Background(),
		body,
		"registration-mtlf",
		&target,
	)
	if problem != nil || response == nil || response.StatusCode != http.StatusCreated {
		t.Fatalf("create response=%+v problem=%+v", response, problem)
	}
	if len(peer.monitorBody) != 0 {
		t.Fatal("self-discovered target was sent through the peer HTTP consumer")
	}
	if len(anlfBackend.body) == 0 {
		t.Fatal("self-discovered target did not reach the local AnLF backend")
	}
	localRouteID, err := backend.ResourceIDFromLocation(response.Location)
	if err != nil {
		t.Fatal(err)
	}
	route, found := ctx.GetMLModelMonitorSubscriptionRoute(localRouteID)
	if !found || route.OwnerRegistrationID != "registration-go" ||
		route.PeerRoute.SelectedTarget != nil ||
		route.PeerRoute.ProcessGeneration != anlfAvailability.generation ||
		route.PeerRoute.RelatedBackend != backend.KindMTLF ||
		route.PeerRoute.RelatedGeneration != mtlfAvailability.generation {
		t.Fatalf("local monitor route=%+v found=%v", route, found)
	}
}

func TestFailedPeerCreateRetainsPendingCleanupUntilDeleteSucceeds(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
	_ = mtlfBackend
	_ = anlfBackend
	_ = mtlfAvailability
	_ = anlfAvailability
	deleteFailure := &backend.TransportError{
		Operation: "delete peer provision",
		Cause:     errors.New("peer unavailable"),
	}
	peer := &mlModelPeerConsumerStub{
		provisionResponse: &backend.StandardResponse{
			StatusCode:   http.StatusCreated,
			Location:     "peer-provision",
			EffectiveURI: "http://nwdaf-c.example/nnwdaf-mlmodelprovision/v1/subscriptions",
			ContentType:  "application/json",
			Body:         []byte(`[]`),
		},
		deleteProvisionErrors: []error{deleteFailure},
	}
	processor.SetMLModelPeerConsumer(peer)
	target := backend.SelectedTarget{
		NFInstanceID:        "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
		NFServiceInstanceID: "model-provision-c",
		ServiceName:         "nnwdaf-mlmodelprovision",
		APIRoot:             "http://nwdaf-c.example",
		SelectionSource:     backend.SelectionSourceNRF,
	}
	body := []byte(`{
		"mLEventSubscs":[{"mLEvent":"UE_COMMUNICATION","mLEventFilter":{}}],
		"notifUri":"http://anlf-a.internal/provision",
		"notifCorreId":"corr-a"
	}`)
	response, problem := processor.HandleCreateMLModelProvisionFromBackend(
		context.Background(),
		body,
		&target,
	)
	if response != nil || problem == nil || problem.Status != http.StatusBadGateway {
		t.Fatalf("response=%+v problem=%+v", response, problem)
	}
	routes := ctx.GetAllMLModelProvisionSubscriptionRoutes()
	if len(routes) != 1 ||
		routes[0].PeerRoute.LifecycleState != nwdaf_context.MLModelRoutePendingCleanup ||
		routes[0].PeerRoute.OperationRevision != 2 ||
		routes[0].PeerRoute.PeerLocation == "" ||
		len(routes[0].AcceptedRepresentation) != 0 {
		t.Fatalf("pending cleanup route=%+v", routes)
	}
	processor.ReconcilePendingMLModelPeerCleanup(
		context.Background(),
		routes[0].PeerRoute.NextCleanupAt,
	)
	if remaining := ctx.GetAllMLModelProvisionSubscriptionRoutes(); len(remaining) != 0 {
		t.Fatalf("cleanup route remained after successful retry: %+v", remaining)
	}
}

func TestPublicMonitorCallbackRejectsInboundAndPendingRoutes(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
	_ = mtlfBackend
	_ = anlfBackend
	_ = mtlfAvailability
	_ = anlfAvailability
	report := []byte(`{
		"notifCorrId":"corr-a",
		"modelAccuInfos":[{"modelId":7,"deviation":0.2}]
	}`)
	representation := []byte(`{
		"modelIds":[7],
		"notificationUri":"http://consumer.example/monitor",
		"notifCorrId":"corr-a"
	}`)
	for _, test := range []struct {
		id        string
		peerRoute nwdaf_context.MLModelPeerRoute
		status    int32
	}{
		{
			id: "inbound",
			peerRoute: nwdaf_context.MLModelPeerRoute{
				Direction:      nwdaf_context.MLModelRouteDirectionInbound,
				LifecycleState: nwdaf_context.MLModelRouteActive,
			},
			status: http.StatusBadRequest,
		},
		{
			id: "pending",
			peerRoute: nwdaf_context.MLModelPeerRoute{
				Direction:      nwdaf_context.MLModelRouteDirectionOutbound,
				LifecycleState: nwdaf_context.MLModelRoutePendingCleanup,
				SelectedTarget: &backend.SelectedTarget{NFInstanceID: "peer"},
			},
			status: http.StatusServiceUnavailable,
		},
	} {
		if !ctx.AddMLModelMonitorSubscriptionRoute(
			nwdaf_context.MLModelMonitorSubscriptionRoute{
				SubscriptionID:            test.id,
				PeerRoute:                 test.peerRoute,
				AcceptedRepresentation:    representation,
				NotificationCorrelationID: "corr-a",
				Destination:               nwdaf_context.MLModelRoutePartyMTLFBackend,
			},
		) {
			t.Fatalf("could not add %s route", test.id)
		}
		response, problem := processor.HandleMLModelMonitorNotification(
			context.Background(),
			test.id,
			report,
		)
		if response != nil || problem == nil || problem.Status != test.status {
			t.Fatalf("%s response=%+v problem=%+v", test.id, response, problem)
		}
	}
}

func TestMonitorCallbackUsesCommittedRepresentationDuringReplace(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
	_ = anlfBackend
	_ = mtlfAvailability
	_ = anlfAvailability
	representation := []byte(`{
		"modelIds":[7],
		"notificationUri":"http://consumer.example/monitor",
		"notifCorrId":"corr-a",
		"mLEvent":"UE_COMMUNICATION"
	}`)
	if !ctx.AddMLModelMonitorSubscriptionRoute(
		nwdaf_context.MLModelMonitorSubscriptionRoute{
			SubscriptionID: "monitor",
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				LifecycleState:    nwdaf_context.MLModelRouteReplacing,
				OperationRevision: 7,
			},
			AcceptedRepresentation:    representation,
			Destination:               nwdaf_context.MLModelRoutePartyMTLFBackend,
			NotificationCorrelationID: "corr-a",
		},
	) {
		t.Fatal("could not seed replacing monitor route")
	}
	report := []byte(`{
		"notifCorrId":"corr-a",
		"modelAccuInfos":[{"modelId":7,"deviation":0.2}]
	}`)
	response, problem := processor.HandleMLModelMonitorNotification(
		t.Context(),
		"",
		report,
	)
	if problem != nil || response == nil || response.StatusCode != http.StatusNoContent {
		t.Fatalf("callback response=%+v problem=%+v", response, problem)
	}
	if !bytes.Equal(mtlfBackend.registrationBody, report) {
		t.Fatalf("delivered report=%s", mtlfBackend.registrationBody)
	}
}

func TestRemoteProvisionPersistsOnlyPermanentRedirectLocation(t *testing.T) {
	for _, test := range []struct {
		name               string
		permanentURI       string
		expectedDeleteTail string
	}{
		{
			name:               "temporary",
			expectedDeleteTail: "/peer-provision",
		},
		{
			name:               "permanent",
			permanentURI:       "http://nwdaf-new.example/subscriptions/permanent",
			expectedDeleteTail: "/subscriptions/permanent",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
			_ = ctx
			_ = mtlfBackend
			_ = anlfBackend
			_ = mtlfAvailability
			_ = anlfAvailability
			peer := &mlModelPeerConsumerStub{}
			processor.SetMLModelPeerConsumer(peer)
			target := backend.SelectedTarget{
				NFInstanceID:        "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
				NFServiceInstanceID: "model-provision-c",
				ServiceName:         "nnwdaf-mlmodelprovision",
				APIRoot:             "http://nwdaf-c.example",
				SelectionSource:     backend.SelectionSourceNRF,
			}
			body := []byte(`{
				"mLEventSubscs":[{"mLEvent":"UE_COMMUNICATION","mLEventFilter":{}}],
				"notifUri":"http://anlf-a.internal/provision",
				"notifCorreId":"corr-a"
			}`)
			created, problem := processor.HandleCreateMLModelProvisionFromBackend(
				context.Background(),
				body,
				&target,
			)
			if problem != nil {
				t.Fatal(problem)
			}
			routeID, err := backend.ResourceIDFromLocation(created.Location)
			if err != nil {
				t.Fatal(err)
			}
			peer.provisionReplaceResponse = &backend.StandardResponse{
				StatusCode:           http.StatusNoContent,
				EffectiveURI:         "http://nwdaf-temp.example/subscriptions/temporary",
				PermanentRedirectURI: test.permanentURI,
			}
			if response, replaceProblem := processor.HandleReplaceMLModelProvisionFromBackend(
				context.Background(),
				routeID,
				body,
			); replaceProblem != nil || response.StatusCode != http.StatusNoContent {
				t.Fatalf("replace response=%+v problem=%+v", response, replaceProblem)
			}
			if _, deleteProblem := processor.HandleDeleteMLModelProvisionFromBackend(
				context.Background(),
				routeID,
			); deleteProblem != nil {
				t.Fatal(deleteProblem)
			}
			if !strings.HasSuffix(peer.deletedProvision, test.expectedDeleteTail) {
				t.Fatalf("delete location=%q", peer.deletedProvision)
			}
		})
	}
}

func TestRemoteMonitorSubscriptionKeepsOwnerAndIsolatesReportRoute(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
	_ = anlfBackend
	_ = mtlfAvailability
	_ = anlfAvailability
	peer := &mlModelPeerConsumerStub{}
	processor.SetMLModelPeerConsumer(peer)
	if !ctx.AddMLModelMonitorRegistrationRoute(
		nwdaf_context.MLModelMonitorRegistrationRoute{
			RegistrationID: "registration-go",
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				Direction:         nwdaf_context.MLModelRouteDirectionInbound,
				BackendResourceID: "registration-a",
				LifecycleState:    nwdaf_context.MLModelRouteActive,
			},
		},
	) {
		t.Fatal("could not seed local monitor registration route")
	}
	target := backend.SelectedTarget{
		NFInstanceID:        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		NFServiceInstanceID: "model-monitor-a",
		ServiceName:         "nnwdaf-mlmodelmonitor",
		APIRoot:             "http://nwdaf-a.example",
		SelectionSource:     backend.SelectionSourceNRF,
	}
	body := []byte(`{
		"modelIds":[7],
		"notificationUri":"http://mtlf-c.internal/monitor",
		"notifCorrId":"scope-a-generation-1",
		"modelMetric":"ACCURACY",
		"mLEvent":"UE_COMMUNICATION",
		"mLEventFilter":{"aoi":{"tais":[{"plmnId":{"mcc":"001","mnc":"01"},"tac":"000001"}]}},
		"tgtUe":{"intGroupIds":["group-a"]}
	}`)
	response, problem := processor.HandleCreateMLModelMonitorSubscriptionFromBackend(
		context.Background(),
		body,
		"registration-a",
		&target,
	)
	if problem != nil || response == nil || response.StatusCode != http.StatusCreated {
		t.Fatalf("create response=%+v problem=%+v", response, problem)
	}
	localRouteID, err := backend.ResourceIDFromLocation(response.Location)
	if err != nil {
		t.Fatal(err)
	}
	route, found := ctx.GetMLModelMonitorSubscriptionRoute(localRouteID)
	if !found || route.OwnerRegistrationID != "registration-go" ||
		route.NotificationCorrelationID != "scope-a-generation-1" ||
		route.PeerRoute.SelectedTarget == nil ||
		!strings.Contains(string(peer.monitorBody), "/nnwdaf-callback/v1/ml-model-monitor/"+localRouteID) {
		t.Fatalf("remote monitor route=%+v found=%v peerBody=%s", route, found, peer.monitorBody)
	}

	report := []byte(`{
		"notifCorrId":"scope-a-generation-1",
		"modelAccuInfos":[{
			"modelId":7,
			"deviation":0.25,
			"inferenceNum":10,
			"modelMetric":"ACCURACY"
		}],
		"mLEvent":"UE_COMMUNICATION"
	}`)
	callbackResponse, callbackProblem := processor.HandleMLModelMonitorNotification(
		context.Background(),
		localRouteID,
		report,
	)
	if callbackProblem != nil || callbackResponse.StatusCode != http.StatusNoContent ||
		!bytes.Equal(mtlfBackend.registrationBody, report) {
		t.Fatalf(
			"callback response=%+v problem=%+v backendBody=%s",
			callbackResponse,
			callbackProblem,
			mtlfBackend.registrationBody,
		)
	}
}

func TestOppositeCrossNodeMLModelDeletesDoNotHoldRouteLockAcrossPeerCall(t *testing.T) {
	nwdaf_context.Init()
	ctxA := nwdaf_context.GetSelf()
	nwdaf_context.Init()
	ctxC := nwdaf_context.GetSelf()

	newProcessor := func(ctx *nwdaf_context.NWDAFContext) *Processor {
		app := &isolatedMLModelTestApp{
			subscriptionTestApp: &subscriptionTestApp{ctx: context.Background()},
			nwdafContext:        ctx,
		}
		processor := &Processor{nwdaf: app}
		processor.SetMLModelBackends(
			&mtlfMLModelBackendStub{},
			&anlfMLModelBackendStub{},
			&mlModelAvailabilityStub{usable: true, generation: "mtlf-generation"},
			&mlModelAvailabilityStub{usable: true, generation: "anlf-generation"},
		)
		return processor
	}

	processorA := newProcessor(ctxA)
	processorC := newProcessor(ctxC)
	target := &backend.SelectedTarget{NFInstanceID: "peer"}
	if !ctxA.AddMLModelProvisionSubscriptionRoute(
		nwdaf_context.MLModelProvisionSubscriptionRoute{
			SubscriptionID: "a-outbound-provision",
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				Direction:      nwdaf_context.MLModelRouteDirectionOutbound,
				SelectedTarget: target,
				PeerLocation:   "c-inbound-provision",
				LifecycleState: nwdaf_context.MLModelRouteActive,
			},
		},
	) {
		t.Fatal("could not seed A outbound provision route")
	}
	if !ctxA.AddMLModelMonitorSubscriptionRoute(
		nwdaf_context.MLModelMonitorSubscriptionRoute{
			SubscriptionID: "a-inbound-monitor",
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				Direction:         nwdaf_context.MLModelRouteDirectionInbound,
				BackendResourceID: "a-monitor-backend",
				LifecycleState:    nwdaf_context.MLModelRouteActive,
			},
		},
	) {
		t.Fatal("could not seed A inbound monitor route")
	}
	if !ctxC.AddMLModelMonitorSubscriptionRoute(
		nwdaf_context.MLModelMonitorSubscriptionRoute{
			SubscriptionID: "c-outbound-monitor",
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				Direction:      nwdaf_context.MLModelRouteDirectionOutbound,
				SelectedTarget: target,
				PeerLocation:   "a-inbound-monitor",
				LifecycleState: nwdaf_context.MLModelRouteActive,
			},
		},
	) {
		t.Fatal("could not seed C outbound monitor route")
	}
	if !ctxC.AddMLModelProvisionSubscriptionRoute(
		nwdaf_context.MLModelProvisionSubscriptionRoute{
			SubscriptionID: "c-inbound-provision",
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				Direction:         nwdaf_context.MLModelRouteDirectionInbound,
				BackendResourceID: "c-provision-backend",
				LifecycleState:    nwdaf_context.MLModelRouteActive,
			},
		},
	) {
		t.Fatal("could not seed C inbound provision route")
	}

	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	processorA.SetMLModelPeerConsumer(&crossNodeMLModelPeerConsumer{
		mlModelPeerConsumerStub: &mlModelPeerConsumerStub{},
		deleteProvision: func(
			ctx context.Context,
			_ string,
		) (*backend.StandardResponse, error) {
			arrived <- struct{}{}
			<-release
			response, problem := processorC.HandleDeleteMLModelProvision(
				ctx,
				"c-inbound-provision",
			)
			if problem != nil {
				return nil, errors.New(problem.Detail)
			}
			return response, nil
		},
	})
	processorC.SetMLModelPeerConsumer(&crossNodeMLModelPeerConsumer{
		mlModelPeerConsumerStub: &mlModelPeerConsumerStub{},
		deleteMonitor: func(
			ctx context.Context,
			_ string,
		) (*backend.StandardResponse, error) {
			arrived <- struct{}{}
			<-release
			response, problem := processorA.HandleDeleteMLModelMonitorSubscription(
				ctx,
				"a-inbound-monitor",
			)
			if problem != nil {
				return nil, errors.New(problem.Detail)
			}
			return response, nil
		},
	})

	type result struct {
		response *backend.StandardResponse
		problem  *models.ProblemDetails
	}
	results := make(chan result, 2)
	go func() {
		response, problem := processorA.HandleDeleteMLModelProvision(
			t.Context(),
			"a-outbound-provision",
		)
		results <- result{response: response, problem: problem}
	}()
	go func() {
		response, problem := processorC.HandleDeleteMLModelMonitorSubscription(
			t.Context(),
			"c-outbound-monitor",
		)
		results <- result{response: response, problem: problem}
	}()

	for range 2 {
		select {
		case <-arrived:
		case <-time.After(time.Second):
			t.Fatal("opposite peer DELETE did not reach the peer boundary")
		}
	}
	close(release)
	for range 2 {
		select {
		case got := <-results:
			if got.problem != nil || got.response == nil ||
				got.response.StatusCode != http.StatusNoContent {
				t.Fatalf("delete response=%+v problem=%+v", got.response, got.problem)
			}
		case <-time.After(time.Second):
			t.Fatal("opposite peer DELETE requests deadlocked")
		}
	}
	if len(ctxA.GetAllMLModelProvisionSubscriptionRoutes()) != 0 ||
		len(ctxA.GetAllMLModelMonitorSubscriptionRoutes()) != 0 ||
		len(ctxC.GetAllMLModelProvisionSubscriptionRoutes()) != 0 ||
		len(ctxC.GetAllMLModelMonitorSubscriptionRoutes()) != 0 {
		t.Fatal("cross-node delete left a route behind")
	}
}

func TestConcurrentDeleteDoesNotDuplicatePeerRequest(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
	_ = mtlfBackend
	_ = anlfBackend
	_ = mtlfAvailability
	_ = anlfAvailability
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	peer := &crossNodeMLModelPeerConsumer{
		mlModelPeerConsumerStub: &mlModelPeerConsumerStub{},
		deleteProvision: func(
			_ context.Context,
			_ string,
		) (*backend.StandardResponse, error) {
			started <- struct{}{}
			<-release
			return noContentMLModelResponse(), nil
		},
	}
	processor.SetMLModelPeerConsumer(peer)
	if !ctx.AddMLModelProvisionSubscriptionRoute(
		nwdaf_context.MLModelProvisionSubscriptionRoute{
			SubscriptionID: "provision",
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				Direction:      nwdaf_context.MLModelRouteDirectionOutbound,
				SelectedTarget: &backend.SelectedTarget{NFInstanceID: "peer"},
				PeerLocation:   "http://peer.example/provision",
				LifecycleState: nwdaf_context.MLModelRouteActive,
			},
		},
	) {
		t.Fatal("could not seed provision route")
	}

	firstDone := make(chan *models.ProblemDetails, 1)
	go func() {
		_, problem := processor.HandleDeleteMLModelProvision(t.Context(), "provision")
		firstDone <- problem
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first peer DELETE did not start")
	}
	response, problem := processor.HandleDeleteMLModelProvision(t.Context(), "provision")
	if response != nil || problem == nil || problem.Status != http.StatusServiceUnavailable {
		t.Fatalf("concurrent delete response=%+v problem=%+v", response, problem)
	}
	close(release)
	select {
	case problem = <-firstDone:
		if problem != nil {
			t.Fatalf("first delete problem=%+v", problem)
		}
	case <-time.After(time.Second):
		t.Fatal("first peer DELETE did not complete")
	}
}

func TestDeleteTreatsMissingDestinationResourceAsCompleted(t *testing.T) {
	missing := func() error {
		return &backend.StandardError{
			StatusCode: http.StatusNotFound,
			ProblemDetails: models.ProblemDetails{
				Status: http.StatusNotFound,
				Cause:  "RESOURCE_NOT_FOUND",
			},
		}
	}
	assertCompleted := func(
		t *testing.T,
		response *backend.StandardResponse,
		problem *models.ProblemDetails,
	) {
		t.Helper()
		if problem != nil || response == nil || response.StatusCode != http.StatusNoContent {
			t.Fatalf("delete response=%+v problem=%+v", response, problem)
		}
	}

	t.Run("provision", func(t *testing.T) {
		processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
		_ = anlfBackend
		_ = mtlfAvailability
		_ = anlfAvailability
		mtlfBackend.err = missing()
		ctx.AddMLModelProvisionSubscriptionRoute(nwdaf_context.MLModelProvisionSubscriptionRoute{
			SubscriptionID: "provision",
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				BackendResourceID: "backend-provision",
				LifecycleState:    nwdaf_context.MLModelRouteActive,
			},
		})
		response, problem := processor.HandleDeleteMLModelProvision(t.Context(), "provision")
		assertCompleted(t, response, problem)
	})

	t.Run("registration", func(t *testing.T) {
		processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
		_ = anlfBackend
		_ = mtlfAvailability
		_ = anlfAvailability
		mtlfBackend.err = missing()
		ctx.AddMLModelMonitorRegistrationRoute(nwdaf_context.MLModelMonitorRegistrationRoute{
			RegistrationID: "registration",
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				BackendResourceID: "backend-registration",
				LifecycleState:    nwdaf_context.MLModelRouteActive,
			},
		})
		response, problem := processor.HandleDeleteMLModelMonitorRegistration(
			t.Context(),
			"registration",
		)
		assertCompleted(t, response, problem)
	})

	t.Run("monitor", func(t *testing.T) {
		processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
		_ = mtlfBackend
		_ = mtlfAvailability
		_ = anlfAvailability
		anlfBackend.err = missing()
		ctx.AddMLModelMonitorSubscriptionRoute(nwdaf_context.MLModelMonitorSubscriptionRoute{
			SubscriptionID: "monitor",
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				BackendResourceID: "backend-monitor",
				LifecycleState:    nwdaf_context.MLModelRouteActive,
			},
		})
		response, problem := processor.HandleDeleteMLModelMonitorSubscription(t.Context(), "monitor")
		assertCompleted(t, response, problem)
	})

	t.Run("training", func(t *testing.T) {
		processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
		_ = anlfBackend
		_ = mtlfAvailability
		_ = anlfAvailability
		mtlfBackend.trainingDeleteError = missing()
		ctx.AddMLModelTrainingSubscriptionRoute(nwdaf_context.MLModelTrainingSubscriptionRoute{
			SubscriptionID:    "training",
			OwnerNFInstanceID: ctx.NfId,
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				Direction:      nwdaf_context.MLModelRouteDirectionInbound,
				LifecycleState: nwdaf_context.MLModelRouteActive,
			},
			NotificationCorrelationID: "training-correlation",
		})
		response, problem := processor.HandleDeleteMLModelTraining(t.Context(), "training")
		assertCompleted(t, response, problem)
	})
}

func TestDeleteCompletionAfterBackendResetDoesNotRecreateRouteOrConsumeTombstone(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
	_ = mtlfBackend
	_ = anlfBackend
	_ = mtlfAvailability
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	peer := &crossNodeMLModelPeerConsumer{
		mlModelPeerConsumerStub: &mlModelPeerConsumerStub{},
		deleteProvision: func(
			_ context.Context,
			_ string,
		) (*backend.StandardResponse, error) {
			started <- struct{}{}
			<-release
			return noContentMLModelResponse(), nil
		},
	}
	processor.SetMLModelPeerConsumer(peer)
	if !ctx.AddMLModelProvisionSubscriptionRoute(
		nwdaf_context.MLModelProvisionSubscriptionRoute{
			SubscriptionID: "provision",
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				Direction:         nwdaf_context.MLModelRouteDirectionOutbound,
				SelectedTarget:    &backend.SelectedTarget{NFInstanceID: "peer"},
				PeerLocation:      "http://peer.example/provision",
				LifecycleState:    nwdaf_context.MLModelRouteActive,
				ProcessGeneration: anlfAvailability.generation,
			},
		},
	) {
		t.Fatal("could not seed provision route")
	}

	deleteDone := make(chan struct {
		response *backend.StandardResponse
		problem  *models.ProblemDetails
	}, 1)
	go func() {
		response, problem := processor.HandleDeleteMLModelProvision(t.Context(), "provision")
		deleteDone <- struct {
			response *backend.StandardResponse
			problem  *models.ProblemDetails
		}{response: response, problem: problem}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("peer DELETE did not start")
	}

	resetDone := make(chan struct{})
	go func() {
		processor.ResetMLModelBackendGeneration(
			t.Context(),
			backend.KindAnLF,
			anlfAvailability.generation,
		)
		close(resetDone)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("backend reset cleanup did not start")
	}
	close(release)

	select {
	case got := <-deleteDone:
		if got.problem != nil || got.response == nil ||
			got.response.StatusCode != http.StatusNoContent {
			t.Fatalf("stale delete response=%+v problem=%+v", got.response, got.problem)
		}
	case <-time.After(time.Second):
		t.Fatal("stale delete completion did not finish")
	}
	select {
	case <-resetDone:
	case <-time.After(time.Second):
		t.Fatal("backend reset did not finish")
	}
	if _, found := ctx.GetMLModelProvisionSubscriptionRoute("provision"); found {
		t.Fatal("stale completion recreated a reset route")
	}
	if _, found := ctx.GetMLModelDeletionRecord(
		nwdaf_context.MLModelResourceProvisionSubscription,
		"provision",
	); !found {
		t.Fatal("stale completion consumed the late-delete tombstone")
	}
	response, problem := processor.HandleDeleteMLModelProvision(t.Context(), "provision")
	if problem != nil || response == nil || response.StatusCode != http.StatusNoContent {
		t.Fatalf("late delete response=%+v problem=%+v", response, problem)
	}
	response, problem = processor.HandleDeleteMLModelProvision(t.Context(), "provision")
	if response != nil || problem == nil || problem.Status != http.StatusNotFound {
		t.Fatalf("second late delete response=%+v problem=%+v", response, problem)
	}
}

func TestPendingCleanupAllowsOnlyOneInFlightPeerDelete(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
	_ = mtlfBackend
	_ = anlfBackend
	_ = mtlfAvailability
	_ = anlfAvailability
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	processor.SetMLModelPeerConsumer(&crossNodeMLModelPeerConsumer{
		mlModelPeerConsumerStub: &mlModelPeerConsumerStub{},
		deleteProvision: func(
			_ context.Context,
			_ string,
		) (*backend.StandardResponse, error) {
			started <- struct{}{}
			<-release
			return noContentMLModelResponse(), nil
		},
	})
	now := time.Now()
	if !ctx.AddMLModelProvisionSubscriptionRoute(
		nwdaf_context.MLModelProvisionSubscriptionRoute{
			SubscriptionID: "pending",
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				SelectedTarget:  &backend.SelectedTarget{NFInstanceID: "peer"},
				PeerLocation:    "http://peer.example/provision",
				LifecycleState:  nwdaf_context.MLModelRoutePendingCleanup,
				CleanupAttempts: 1,
				NextCleanupAt:   now,
			},
		},
	) {
		t.Fatal("could not seed pending cleanup route")
	}
	firstDone := make(chan struct{})
	go func() {
		processor.ReconcilePendingMLModelPeerCleanup(t.Context(), now)
		close(firstDone)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first cleanup did not start")
	}
	processor.ReconcilePendingMLModelPeerCleanup(t.Context(), now)
	select {
	case <-started:
		t.Fatal("a second cleanup request started while the first was in flight")
	default:
	}
	close(release)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first cleanup did not finish")
	}
	if _, found := ctx.GetMLModelProvisionSubscriptionRoute("pending"); found {
		t.Fatal("completed cleanup route remains")
	}
}

func TestStaleDeleteCompletionCannotRemoveNewerRouteWithSameID(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
	_ = mtlfBackend
	_ = anlfBackend
	_ = mtlfAvailability
	_ = anlfAvailability
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	processor.SetMLModelPeerConsumer(&crossNodeMLModelPeerConsumer{
		mlModelPeerConsumerStub: &mlModelPeerConsumerStub{},
		deleteProvision: func(
			_ context.Context,
			_ string,
		) (*backend.StandardResponse, error) {
			started <- struct{}{}
			<-release
			return noContentMLModelResponse(), nil
		},
	})
	if !ctx.AddMLModelProvisionSubscriptionRoute(
		nwdaf_context.MLModelProvisionSubscriptionRoute{
			SubscriptionID: "provision",
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				SelectedTarget: &backend.SelectedTarget{NFInstanceID: "old-peer"},
				PeerLocation:   "http://old-peer.example/provision",
				LifecycleState: nwdaf_context.MLModelRouteActive,
			},
		},
	) {
		t.Fatal("could not seed old provision route")
	}
	result := make(chan struct {
		response *backend.StandardResponse
		problem  *models.ProblemDetails
	}, 1)
	go func() {
		response, problem := processor.HandleDeleteMLModelProvision(t.Context(), "provision")
		result <- struct {
			response *backend.StandardResponse
			problem  *models.ProblemDetails
		}{response: response, problem: problem}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("old peer DELETE did not start")
	}

	processor.mlModelMu.Lock()
	ctx.DeleteMLModelProvisionSubscriptionRoute("provision")
	ctx.AddMLModelProvisionSubscriptionRoute(nwdaf_context.MLModelProvisionSubscriptionRoute{
		SubscriptionID: "provision",
		PeerRoute: nwdaf_context.MLModelPeerRoute{
			SelectedTarget:    &backend.SelectedTarget{NFInstanceID: "new-peer"},
			PeerLocation:      "http://new-peer.example/provision",
			LifecycleState:    nwdaf_context.MLModelRouteActive,
			OperationRevision: processor.nextMLModelOperationRevisionLocked(),
		},
	})
	processor.mlModelMu.Unlock()
	close(release)

	select {
	case got := <-result:
		if got.response != nil || got.problem == nil ||
			got.problem.Status != http.StatusServiceUnavailable {
			t.Fatalf("stale completion response=%+v problem=%+v", got.response, got.problem)
		}
	case <-time.After(time.Second):
		t.Fatal("old peer DELETE did not complete")
	}
	newRoute, found := ctx.GetMLModelProvisionSubscriptionRoute("provision")
	if !found || newRoute.PeerRoute.SelectedTarget == nil ||
		newRoute.PeerRoute.SelectedTarget.NFInstanceID != "new-peer" ||
		newRoute.PeerRoute.LifecycleState != nwdaf_context.MLModelRouteActive {
		t.Fatalf("newer route was changed by stale completion: %+v found=%v", newRoute, found)
	}
}

func TestTransientDeleteFailureRestoresActiveRoute(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
	_ = mtlfBackend
	_ = anlfBackend
	_ = mtlfAvailability
	_ = anlfAvailability
	processor.SetMLModelPeerConsumer(&crossNodeMLModelPeerConsumer{
		mlModelPeerConsumerStub: &mlModelPeerConsumerStub{},
		deleteProvision: func(
			_ context.Context,
			_ string,
		) (*backend.StandardResponse, error) {
			return nil, &backend.TransportError{
				Operation: "delete peer provision",
				Cause:     errors.New("peer unavailable"),
			}
		},
	})
	if !ctx.AddMLModelProvisionSubscriptionRoute(
		nwdaf_context.MLModelProvisionSubscriptionRoute{
			SubscriptionID: "provision",
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				SelectedTarget: &backend.SelectedTarget{NFInstanceID: "peer"},
				PeerLocation:   "http://peer.example/provision",
				LifecycleState: nwdaf_context.MLModelRouteActive,
			},
		},
	) {
		t.Fatal("could not seed provision route")
	}
	response, problem := processor.HandleDeleteMLModelProvision(t.Context(), "provision")
	if response != nil || problem == nil || problem.Status != http.StatusServiceUnavailable {
		t.Fatalf("delete response=%+v problem=%+v", response, problem)
	}
	route, found := ctx.GetMLModelProvisionSubscriptionRoute("provision")
	if !found || route.PeerRoute.LifecycleState != nwdaf_context.MLModelRouteActive ||
		route.PeerRoute.OperationRevision == 0 {
		t.Fatalf("route was not restored after transient failure: %+v found=%v", route, found)
	}
}

func TestTrainingReplacePeerNotFoundRestoresCommittedRoute(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
	_ = anlfBackend
	_ = mtlfAvailability
	_ = anlfAvailability
	body := []byte(`{
		"mLEventSubscs":[{
			"mLEvent":"UE_COMMUNICATION",
			"mLEventFilter":{},
			"modelInterInfo":"bundle-v1"
		}],
		"notifUri":"http://server.example/training-callback",
		"notifCorreId":"training-correlation",
		"mlCorreId":"fl-process-001",
		"mLPreFlag":true,
		"mLModelTrainInfos":[{
			"dataAvReq":{"inpEvents":[{"upfEvent":"USER_DATA_USAGE_TRENDS"}]},
			"timeAvReq":"PT5M"
		}]
	}`)
	if _, problem := processor.HandleCreateMLModelTraining(t.Context(), body); problem != nil {
		t.Fatalf("create problem=%+v", problem)
	}
	routes := ctx.GetAllMLModelTrainingSubscriptionRoutes()
	if len(routes) != 1 {
		t.Fatalf("training routes=%+v", routes)
	}
	route := routes[0]
	route.PeerRoute.SelectedTarget = &backend.SelectedTarget{NFInstanceID: "peer"}
	route.PeerRoute.PeerLocation = "http://peer.example/training"
	if !ctx.UpdateMLModelTrainingSubscriptionRoute(route) {
		t.Fatal("could not convert training route to peer route")
	}
	peer := &mlModelPeerConsumerStub{}
	processor.SetMLModelPeerConsumer(peer)
	// The training stub is unused after the route is converted to a peer route.
	_ = mtlfBackend
	peerTrainingError := &backend.StandardError{
		StatusCode: http.StatusNotFound,
		ProblemDetails: models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "RESOURCE_NOT_FOUND",
		},
	}
	peer.trainingReplaceError = peerTrainingError
	response, problem := processor.HandleReplaceMLModelTraining(
		t.Context(),
		route.SubscriptionID,
		body,
	)
	if response != nil || problem == nil || problem.Status != http.StatusNotFound {
		t.Fatalf("replace response=%+v problem=%+v", response, problem)
	}
	current, found := ctx.GetMLModelTrainingSubscriptionRoute(route.ResourceKey())
	if !found || current.PeerRoute.LifecycleState != nwdaf_context.MLModelRouteActive ||
		!bytes.Equal(current.AcceptedRepresentation, route.AcceptedRepresentation) {
		t.Fatalf("training route not restored: %+v found=%v", current, found)
	}
}

func TestPendingCleanupRetainsRouteOnMalformedSuccess(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
	_ = mtlfBackend
	_ = anlfBackend
	_ = mtlfAvailability
	_ = anlfAvailability
	processor.SetMLModelPeerConsumer(&crossNodeMLModelPeerConsumer{
		mlModelPeerConsumerStub: &mlModelPeerConsumerStub{},
		deleteProvision: func(
			_ context.Context,
			_ string,
		) (*backend.StandardResponse, error) {
			return nil, nil
		},
	})
	now := time.Now()
	if !ctx.AddMLModelProvisionSubscriptionRoute(
		nwdaf_context.MLModelProvisionSubscriptionRoute{
			SubscriptionID: "pending",
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				SelectedTarget:  &backend.SelectedTarget{NFInstanceID: "peer"},
				PeerLocation:    "http://peer.example/provision",
				LifecycleState:  nwdaf_context.MLModelRoutePendingCleanup,
				CleanupAttempts: 1,
				NextCleanupAt:   now,
			},
		},
	) {
		t.Fatal("could not seed pending route")
	}
	processor.ReconcilePendingMLModelPeerCleanup(t.Context(), now)
	route, found := ctx.GetMLModelProvisionSubscriptionRoute("pending")
	if !found || route.PeerRoute.LifecycleState != nwdaf_context.MLModelRoutePendingCleanup ||
		route.PeerRoute.CleanupAttempts != 2 || !route.PeerRoute.NextCleanupAt.After(now) {
		t.Fatalf("malformed cleanup result was accepted: %+v found=%v", route, found)
	}
}
