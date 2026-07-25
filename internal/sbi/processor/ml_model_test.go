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
	usable    bool
	marked    int
	refreshed int
}

func (s *mlModelAvailabilityStub) Usable() bool { return s.usable }
func (s *mlModelAvailabilityStub) MarkUnavailable(string) {
	s.marked++
}
func (s *mlModelAvailabilityStub) Refresh() { s.refreshed++ }

type mtlfMLModelBackendStub struct {
	provisionBody       []byte
	registrationBody    []byte
	response            *backend.StandardResponse
	err                 error
	deletedProvision    string
	deletedRegistration string
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

type anlfMLModelBackendStub struct {
	body     []byte
	response *backend.StandardResponse
	err      error
	deleted  string
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
	mtlfAvailability := &mlModelAvailabilityStub{usable: true}
	anlfAvailability := &mlModelAvailabilityStub{usable: true}
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
	if response.StatusCode != http.StatusCreated || !strings.HasSuffix(response.Location, "/"+testProvisionID) {
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
	route, found := ctx.GetMLModelProvisionSubscriptionRoute(testProvisionID)
	if !found || route.DestinationNotificationURI != "http://consumer.example/provision" ||
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
		context.Background(), testProvisionID, replacement,
	)
	if problem != nil || response.StatusCode != http.StatusNoContent || len(response.Body) != 0 {
		t.Fatalf("replace response=%+v problem=%+v", response, problem)
	}
	route, _ = ctx.GetMLModelProvisionSubscriptionRoute(testProvisionID)
	if route.DestinationNotificationURI != "http://consumer.example/replaced" {
		t.Fatalf("updated route = %+v", route)
	}

	response, problem = processor.HandleDeleteMLModelProvision(context.Background(), testProvisionID)
	if problem != nil || response.StatusCode != http.StatusNoContent || mtlfBackend.deletedProvision != testProvisionID {
		t.Fatalf("delete response=%+v problem=%+v backend=%+v", response, problem, mtlfBackend)
	}
	if _, found = ctx.GetMLModelProvisionSubscriptionRoute(testProvisionID); found {
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
	)
	if problem != nil || response.StatusCode != http.StatusCreated {
		t.Fatalf("create response=%+v problem=%+v", response, problem)
	}
	route, found := ctx.GetMLModelProvisionSubscriptionRoute(testProvisionID)
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
		testProvisionID,
		notification,
	)
	if problem != nil || response.StatusCode != http.StatusNoContent {
		t.Fatalf("notification response=%+v problem=%+v", response, problem)
	}
	if !bytes.Equal(anlfBackend.body, notification) {
		t.Fatalf("AnLF backend notification=%s", anlfBackend.body)
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
		testProvisionID,
		updatedNotification,
	)
	if problem != nil || response.StatusCode != http.StatusNoContent {
		t.Fatalf("updated notification response=%+v problem=%+v", response, problem)
	}
	if !bytes.Equal(anlfBackend.body, updatedNotification) {
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
	if _, problem := processor.HandleCreateMLModelProvision(
		context.Background(),
		createBody,
	); problem != nil {
		t.Fatal(problem)
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
	if body := <-delivered; !bytes.Equal(body, notification) {
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
	if _, found := ctx.GetMLModelMonitorRegistrationRoute(testRegistrationID); !found {
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
	if _, found := ctx.GetMLModelMonitorSubscriptionRoute(testMonitorID); !found {
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
	)
	if problem != nil || response == nil || response.StatusCode != http.StatusCreated {
		t.Fatalf("create response=%+v problem=%+v", response, problem)
	}
	route, found := ctx.GetMLModelMonitorSubscriptionRoute(testMonitorID)
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
	if _, problem := processor.HandleCreateMLModelProvision(context.Background(), body); problem != nil {
		t.Fatalf("create problem = %+v", problem)
	}
	mtlfBackend.response = &backend.StandardResponse{
		StatusCode: http.StatusTemporaryRedirect,
		Location:   "http://mtlf.internal/private/resource",
	}
	response, problem := processor.HandleReplaceMLModelProvision(
		context.Background(),
		testProvisionID,
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
	_, problem := processor.HandleCreateMLModelMonitorRegistration(context.Background(), body)
	if problem != nil {
		t.Fatal(problem)
	}
	route, _ := ctx.GetMLModelMonitorRegistrationRoute(testRegistrationID)
	var value map[string]json.RawMessage
	if err := json.Unmarshal(route.AcceptedRepresentation, &value); err != nil || value["modelId"] == nil {
		t.Fatalf("stored representation = %s error=%v", route.AcceptedRepresentation, err)
	}
}
