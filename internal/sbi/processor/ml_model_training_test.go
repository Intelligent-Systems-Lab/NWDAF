package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/free5gc/nwdaf/internal/backend"
	wire "github.com/free5gc/nwdaf/internal/compat/mlmodeltraining"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/openapi/models"
)

type mlModelTrainingRoundTripper func(*http.Request) (*http.Response, error)

func (f mlModelTrainingRoundTripper) RoundTrip(
	request *http.Request,
) (*http.Response, error) {
	return f(request)
}

func TestBackendTerminationNotificationWaitsForConsumerDelete(t *testing.T) {
	callback := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter,
		request *http.Request,
	) {
		if request.Method != http.MethodPost {
			t.Fatalf("callback method = %s, want POST", request.Method)
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer callback.Close()

	processor, ctx, mtlfBackend, _, availability, _ := newMLModelProcessorTestSubject()
	processor.SetMLModelHTTPClient(callback.Client())
	route := nwdaf_context.MLModelTrainingSubscriptionRoute{
		SubscriptionID:             "old-public-resource",
		OwnerNFInstanceID:          ctx.NfId,
		NotificationCorrelationID:  "old-notification-correlation",
		MLCorrelationID:            "hierarchy-procedure",
		Destination:                nwdaf_context.MLModelRoutePartyExternal,
		DestinationNotificationURI: callback.URL,
		PeerRoute: nwdaf_context.MLModelPeerRoute{
			Direction:         nwdaf_context.MLModelRouteDirectionInbound,
			LifecycleState:    nwdaf_context.MLModelRouteActive,
			ProcessGeneration: availability.generation,
		},
	}
	if !ctx.AddMLModelTrainingSubscriptionRoute(route) {
		t.Fatal("could not add old inbound route")
	}

	response, problem := processor.HandleMLModelTrainingNotification(
		t.Context(), "", []byte(`{
			"notifCorreId":"old-notification-correlation",
			"mlCorreId":"hierarchy-procedure",
			"termTrainReq":"NOT_AVAILABLE_ML_TRAIN"
		}`),
	)
	if problem != nil || response == nil || response.StatusCode != http.StatusNoContent {
		t.Fatalf("termination response=%+v problem=%+v", response, problem)
	}
	terminating, found := ctx.GetMLModelTrainingSubscriptionRoute(route.ResourceKey())
	if !found || terminating.PeerRoute.LifecycleState != nwdaf_context.MLModelRouteLifecycle("TERMINATING") {
		t.Fatalf("terminating route=%+v found=%t", terminating, found)
	}
	if mtlfBackend.deletedTrainingBackend != "" {
		t.Fatalf("backend was deleted before consumer DELETE: %q", mtlfBackend.deletedTrainingBackend)
	}

	deleteResponse, deleteProblem := processor.HandleDeleteMLModelTraining(
		t.Context(), route.SubscriptionID,
	)
	if deleteProblem != nil || deleteResponse == nil ||
		deleteResponse.StatusCode != http.StatusNoContent {
		t.Fatalf("delete response=%+v problem=%+v", deleteResponse, deleteProblem)
	}
	if mtlfBackend.deletedTrainingBackend != route.SubscriptionID {
		t.Fatalf("deleted backend resource = %q", mtlfBackend.deletedTrainingBackend)
	}
	if _, found = ctx.GetMLModelTrainingSubscriptionRoute(route.ResourceKey()); found {
		t.Fatal("terminal route remains after consumer DELETE")
	}
}

func TestBackendTerminationNotificationPeerFailureReturnsAndTombstonesRoute(t *testing.T) {
	callback := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter,
		_ *http.Request,
	) {
		writer.Header().Set("Content-Type", "application/problem+json")
		writer.WriteHeader(http.StatusServiceUnavailable)
		if _, err := writer.Write([]byte(`{
			"status":503,
			"title":"Service Unavailable",
			"cause":"SERVICE_NOT_AVAILABLE"
		}`)); err != nil {
			return
		}
	}))
	defer callback.Close()

	processor, ctx, mtlfBackend, _, availability, _ := newMLModelProcessorTestSubject()
	processor.SetMLModelHTTPClient(callback.Client())
	route := nwdaf_context.MLModelTrainingSubscriptionRoute{
		SubscriptionID:             "old-public-resource",
		OwnerNFInstanceID:          ctx.NfId,
		NotificationCorrelationID:  "old-notification-correlation",
		MLCorrelationID:            "hierarchy-procedure",
		Destination:                nwdaf_context.MLModelRoutePartyExternal,
		DestinationNotificationURI: callback.URL,
		PeerRoute: nwdaf_context.MLModelPeerRoute{
			Direction:         nwdaf_context.MLModelRouteDirectionInbound,
			LifecycleState:    nwdaf_context.MLModelRouteActive,
			ProcessGeneration: availability.generation,
		},
	}
	if !ctx.AddMLModelTrainingSubscriptionRoute(route) {
		t.Fatal("could not add old inbound route")
	}

	response, problem := processor.HandleMLModelTrainingNotification(
		t.Context(), "", []byte(`{
			"notifCorreId":"old-notification-correlation",
			"mlCorreId":"hierarchy-procedure",
			"termTrainReq":"NOT_AVAILABLE_ML_TRAIN"
		}`),
	)
	if response != nil || problem == nil || problem.Status != http.StatusServiceUnavailable {
		t.Fatalf("termination response=%+v problem=%+v", response, problem)
	}
	if _, found := ctx.GetMLModelTrainingSubscriptionRoute(route.ResourceKey()); found {
		t.Fatal("failed-delivery route remains")
	}
	if _, found := ctx.GetMLModelDeletionRecord(
		nwdaf_context.MLModelResourceTrainingSubscription,
		route.SubscriptionID,
	); !found {
		t.Fatal("failed-delivery route was not tombstoned")
	}
	if mtlfBackend.deletedTrainingBackend != "" {
		t.Fatalf("Go deleted backend after peer failure: %q", mtlfBackend.deletedTrainingBackend)
	}

	lateDeleteResponse, lateDeleteProblem := processor.HandleDeleteMLModelTraining(
		t.Context(), route.SubscriptionID,
	)
	if lateDeleteProblem != nil || lateDeleteResponse == nil ||
		lateDeleteResponse.StatusCode != http.StatusNoContent {
		t.Fatalf("late delete response=%+v problem=%+v", lateDeleteResponse, lateDeleteProblem)
	}
	if mtlfBackend.deletedTrainingBackend != "" {
		t.Fatalf("late delete reached backend: %q", mtlfBackend.deletedTrainingBackend)
	}

	duplicateDeleteResponse, duplicateDeleteProblem := processor.HandleDeleteMLModelTraining(
		t.Context(), route.SubscriptionID,
	)
	if duplicateDeleteResponse != nil || duplicateDeleteProblem == nil ||
		duplicateDeleteProblem.Status != http.StatusNotFound {
		t.Fatalf(
			"duplicate delete response=%+v problem=%+v",
			duplicateDeleteResponse,
			duplicateDeleteProblem,
		)
	}
	if mtlfBackend.deletedTrainingBackend != "" {
		t.Fatalf("duplicate delete reached backend: %q", mtlfBackend.deletedTrainingBackend)
	}
}

func TestBackendTerminationNotificationTransportFailureTombstonesRoute(t *testing.T) {
	processor, ctx, mtlfBackend, _, availability, _ := newMLModelProcessorTestSubject()
	processor.SetMLModelHTTPClient(&http.Client{Transport: mlModelTrainingRoundTripper(
		func(*http.Request) (*http.Response, error) {
			return nil, errors.New("peer is unreachable")
		},
	)})
	route := nwdaf_context.MLModelTrainingSubscriptionRoute{
		SubscriptionID:             "old-public-resource",
		OwnerNFInstanceID:          ctx.NfId,
		NotificationCorrelationID:  "old-notification-correlation",
		MLCorrelationID:            "hierarchy-procedure",
		Destination:                nwdaf_context.MLModelRoutePartyExternal,
		DestinationNotificationURI: "http://old-branch.example/notification",
		PeerRoute: nwdaf_context.MLModelPeerRoute{
			Direction:         nwdaf_context.MLModelRouteDirectionInbound,
			LifecycleState:    nwdaf_context.MLModelRouteActive,
			ProcessGeneration: availability.generation,
		},
	}
	if !ctx.AddMLModelTrainingSubscriptionRoute(route) {
		t.Fatal("could not add old inbound route")
	}

	response, problem := processor.HandleMLModelTrainingNotification(
		t.Context(), "", []byte(`{
			"notifCorreId":"old-notification-correlation",
			"mlCorreId":"hierarchy-procedure",
			"termTrainReq":"NOT_AVAILABLE_ML_TRAIN"
		}`),
	)
	if response != nil || problem == nil || problem.Status != http.StatusBadGateway {
		t.Fatalf("termination response=%+v problem=%+v", response, problem)
	}
	if _, found := ctx.GetMLModelTrainingSubscriptionRoute(route.ResourceKey()); found {
		t.Fatal("transport-failed route remains")
	}
	if _, found := ctx.GetMLModelDeletionRecord(
		nwdaf_context.MLModelResourceTrainingSubscription,
		route.SubscriptionID,
	); !found {
		t.Fatal("transport-failed route was not tombstoned")
	}
	if mtlfBackend.deletedTrainingBackend != "" {
		t.Fatalf("Go deleted backend after peer transport failure: %q", mtlfBackend.deletedTrainingBackend)
	}
}

func TestTerminationGraceCleanupDeletesBackendResource(t *testing.T) {
	processor, ctx, mtlfBackend, _, availability, _ := newMLModelProcessorTestSubject()
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	route := nwdaf_context.MLModelTrainingSubscriptionRoute{
		SubscriptionID:            "old-public-resource",
		OwnerNFInstanceID:         ctx.NfId,
		NotificationCorrelationID: "old-notification-correlation",
		MLCorrelationID:           "hierarchy-procedure",
		Destination:               nwdaf_context.MLModelRoutePartyExternal,
		PeerRoute: nwdaf_context.MLModelPeerRoute{
			Direction:         nwdaf_context.MLModelRouteDirectionInbound,
			LifecycleState:    nwdaf_context.MLModelRouteLifecycle("TERMINATING"),
			ProcessGeneration: availability.generation,
			NextCleanupAt:     now.Add(-time.Second),
		},
	}
	if !ctx.AddMLModelTrainingSubscriptionRoute(route) {
		t.Fatal("could not add terminating inbound route")
	}

	processor.ReconcilePendingMLModelPeerCleanup(t.Context(), now)

	if mtlfBackend.deletedTrainingBackend != route.SubscriptionID {
		t.Fatalf("deleted backend resource = %q", mtlfBackend.deletedTrainingBackend)
	}
	if _, found := ctx.GetMLModelTrainingSubscriptionRoute(route.ResourceKey()); found {
		t.Fatal("expired terminating route remains")
	}
	if _, found := ctx.GetMLModelDeletionRecord(
		nwdaf_context.MLModelResourceTrainingSubscription,
		route.SubscriptionID,
	); !found {
		t.Fatal("expired terminating route was not tombstoned")
	}
}

func candidateTrainingBody(receiverID string) []byte {
	return []byte(fmt.Sprintf(`{
		"mLEventSubscs":[{
			"mLEvent":"UE_COMMUNICATION",
			"mLEventFilter":{},
			"modelInterInfo":"bundle-v1"
		}],
		"notifUri":"http://server.example/training-callback",
		"notifCorreId":"candidate-client-a",
		"suppFeats":"4",
		"mlCorreId":"hierarchical-fl-001",
		"mLPreFlag":true,
		"mLModelTrainInfos":[{
			"dataAvReq":{"inpEvents":[{"upfEvent":"USER_DATA_USAGE_TRENDS"}]},
			"timeAvReq":"PT5M"
		}],
		"x-flTopology":{
			"nfInstanceId":%q,
			"children":[{
				"nfInstanceId":"10000000-0000-4000-8000-000000000101"
			}]
		}
	}`, receiverID))
}

func TestMLModelTrainingCreateReturnsPublicRouteAndInternalizesCallback(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, peerConsumer, availability := newMLModelProcessorTestSubject()
	_ = anlfBackend
	_ = peerConsumer
	_ = availability
	body := []byte(`{
		"mLEventSubscs":[{
			"mLEvent":"UE_COMMUNICATION",
			"mLEventFilter":{"networkArea":{"tais":[{
				"plmnId":{"mcc":"466","mnc":"92"},"tac":"000001"
			}]}},
			"modelInterInfo":"bundle-v1"
		}],
		"notifUri":"http://server.example/training-callback",
		"notifCorreId":"prep-client-a",
		"mlCorreId":"fl-process-001",
		"mLPreFlag":true,
		"eventReq":{"notifMethod":"ON_EVENT_DETECTION"},
		"mLModelTrainInfos":[{
			"dataAvReq":{
				"inpEvents":[{"upfEvent":"USER_DATA_USAGE_TRENDS"}],
				"minNumSamples":1
			},
			"timeAvReq":"PT5M"
		}],
		"mLTrainRepInfo":{"maxResTime":300}
	}`)

	response, problem := processor.HandleCreateMLModelTraining(context.Background(), body)
	if problem != nil {
		t.Fatalf("HandleCreateMLModelTraining() problem = %+v", problem)
	}
	if response.StatusCode != http.StatusCreated ||
		!strings.Contains(response.Location, "/nnwdaf-mlmodeltraining/v1/subscriptions/") {
		t.Fatalf("response = %+v", response)
	}
	if strings.Contains(string(mtlfBackend.trainingBody), "server.example") ||
		!strings.Contains(string(mtlfBackend.trainingBody), "192.0.2.21:8091") {
		t.Fatalf("backend callback was not internalized: %s", mtlfBackend.trainingBody)
	}
	routes := ctx.GetAllMLModelTrainingSubscriptionRoutes()
	if len(routes) != 1 ||
		routes[0].DestinationNotificationURI != "http://server.example/training-callback" ||
		routes[0].NotificationCorrelationID != "prep-client-a" {
		t.Fatalf("training routes = %+v", routes)
	}
}

func TestMLModelTrainingNotificationContractIsAppliedBeforeRelay(t *testing.T) {
	var delivered string
	callback := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter,
		request *http.Request,
	) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("ReadAll() error = %v", err)
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		delivered = string(body)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer callback.Close()

	processor, ctx, mtlfBackend, anlfBackend, peerConsumer, availability := newMLModelProcessorTestSubject()
	_ = ctx
	_ = mtlfBackend
	_ = anlfBackend
	_ = peerConsumer
	_ = availability
	processor.SetMLModelHTTPClient(callback.Client())
	body := []byte(`{
		"mLEventSubscs":[{
			"mLEvent":"UE_COMMUNICATION",
			"mLEventFilter":{},
			"modelInterInfo":"bundle-v1"
		}],
		"notifUri":"` + callback.URL + `",
		"notifCorreId":"prep-client-a",
		"mlCorreId":"fl-process-001",
		"mLPreFlag":true,
		"mLModelTrainInfos":[{
			"dataAvReq":{"inpEvents":[{"upfEvent":"USER_DATA_USAGE_TRENDS"}]},
			"timeAvReq":"PT5M"
		}]
	}`)
	if _, problem := processor.HandleCreateMLModelTraining(context.Background(), body); problem != nil {
		t.Fatalf("create problem = %+v", problem)
	}
	notification := []byte(`{
		"notifCorreId":"prep-client-a",
		"mlCorreId":"fl-process-001",
		"statusReport":{"trainInDataInfo":{"samplRatio":100}}
	}`)
	response, problem := processor.HandleMLModelTrainingNotification(
		context.Background(), "", notification,
	)
	if problem == nil || response != nil {
		t.Fatalf("callback response=%+v problem=%+v, want validation problem", response, problem)
	}
	if delivered != "" {
		t.Fatalf("invalid callback was relayed: %s", delivered)
	}

	validNotification := []byte(`{
		"notifCorreId":"prep-client-a",
		"mlCorreId":"fl-process-001",
		"mLModelInfos":[{
			"event":"UE_COMMUNICATION",
			"mLFileAddr":{"mLModelUrl":"http://client.example/preparation-result"}
		}],
		"termTrainReq":"NOT_AVAILABLE_ML_TRAIN"
	}`)
	response, problem = processor.HandleMLModelTrainingNotification(
		context.Background(), "", validNotification,
	)
	if problem != nil || response == nil || response.StatusCode != http.StatusNoContent {
		t.Fatalf("callback response=%+v problem=%+v", response, problem)
	}
	if delivered != string(validNotification) {
		t.Fatalf("delivered body = %s", delivered)
	}
}

func TestMLModelTrainingPatchForwardsDestinationRepresentation(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, peerConsumer, availability := newMLModelProcessorTestSubject()
	_ = anlfBackend
	_ = peerConsumer
	_ = availability
	createBody := []byte(`{
		"mLEventSubscs":[{
			"mLEvent":"UE_COMMUNICATION",
			"mLEventFilter":{},
			"modelInterInfo":"bundle-v1"
		}],
		"notifUri":"http://server.example/training-callback",
		"notifCorreId":"prep-client-a",
		"mlCorreId":"fl-process-001",
		"mLPreFlag":true,
		"eventReq":{"notifMethod":"ON_EVENT_DETECTION"},
		"mLModelTrainInfos":[{
			"dataAvReq":{"inpEvents":[{"upfEvent":"USER_DATA_USAGE_TRENDS"}]},
			"timeAvReq":"PT5M"
		}],
		"mLTrainRepInfo":{"maxResTime":300}
	}`)
	if _, problem := processor.HandleCreateMLModelTraining(
		context.Background(),
		createBody,
	); problem != nil {
		t.Fatalf("create problem = %+v", problem)
	}
	routes := ctx.GetAllMLModelTrainingSubscriptionRoutes()
	if len(routes) != 1 {
		t.Fatalf("training routes = %+v", routes)
	}
	mtlfBackend.trainingResponse = &backend.StandardResponse{
		StatusCode:  http.StatusOK,
		ContentType: "application/json",
		Body: []byte(`{
			"mLEventSubscs":[{
				"mLEvent":"UE_COMMUNICATION",
				"mLEventFilter":{},
				"modelInterInfo":"bundle-v1"
			}],
			"notifUri":"http://192.0.2.21:8091/internal/v1/ml-model-training/notifications",
			"notifCorreId":"prep-client-a",
			"mlCorreId":"fl-process-001",
			"mLPreFlag":true,
			"eventReq":{"notifMethod":"ON_EVENT_DETECTION"},
			"mLModelTrainInfos":[{
				"dataAvReq":{"inpEvents":[{"upfEvent":"USER_DATA_USAGE_TRENDS"}]},
				"timeAvReq":"PT5M"
			}],
			"mLTrainRepInfo":{"maxResTime":601}
		}`),
	}

	response, problem := processor.HandlePatchMLModelTraining(
		context.Background(),
		routes[0].SubscriptionID,
		[]byte(`{"mLTrainRepInfo":{"maxResTime":600}}`),
	)
	if problem != nil {
		t.Fatalf("patch problem = %+v", problem)
	}
	if response.StatusCode != http.StatusOK ||
		!strings.Contains(string(response.Body), `"maxResTime":601`) ||
		!strings.Contains(string(response.Body), "server.example") ||
		strings.Contains(string(response.Body), "192.0.2.21") {
		t.Fatalf("patch response = %+v", response)
	}
	updated, found := ctx.GetMLModelTrainingSubscriptionRoute(routes[0].ResourceKey())
	if !found ||
		!strings.Contains(string(updated.AcceptedRepresentation), `"maxResTime":601`) ||
		!strings.Contains(string(updated.BackendRepresentation), "192.0.2.21") {
		t.Fatalf("updated training route = %+v", updated)
	}
}

func TestMLModelTrainingRequirementsProblemIdentifiesInvalidParameters(t *testing.T) {
	problem := mlModelTrainingValidationProblem(
		&wire.RequirementsError{Violations: []wire.InvalidParameter{
			{Parameter: "mLModelTrainInfos[0].dataAvReq", Reason: "is required"},
		}},
	)

	if problem.Status != http.StatusForbidden ||
		problem.Cause != wire.CauseMLModelTrainingRequirementsNotMet ||
		len(problem.InvalidParams) != 1 ||
		problem.InvalidParams[0].Param != "mLModelTrainInfos[0].dataAvReq" ||
		problem.InvalidParams[0].Reason != "is required" {
		t.Fatalf("problem = %+v", problem)
	}
}

func TestMLModelTrainingCandidateCreatePreservesContractAndFeatureState(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, peerConsumer, availability := newMLModelProcessorTestSubject()
	_ = anlfBackend
	_ = peerConsumer
	_ = availability
	body := candidateTrainingBody(ctx.NfId)

	response, problem := processor.HandleCreateMLModelTraining(context.Background(), body)
	if problem != nil || response == nil || response.StatusCode != http.StatusCreated {
		t.Fatalf("response=%+v problem=%+v", response, problem)
	}
	if !strings.Contains(string(response.Body), `"x-flTopology"`) ||
		!strings.Contains(string(mtlfBackend.trainingBody), `"x-flTopology"`) {
		t.Fatalf("candidate contract was dropped: response=%s backend=%s", response.Body, mtlfBackend.trainingBody)
	}
	routes := ctx.GetAllMLModelTrainingSubscriptionRoutes()
	if len(routes) != 1 {
		t.Fatalf("routes=%+v", routes)
	}
	route := routes[0]
	if route.OfferedSupportedFeatures != "4" || route.NegotiatedSupportedFeatures != "4" ||
		!route.HierarchicalFLFeatureNegotiated || route.BoundParticipantNFInstanceID != ctx.NfId {
		t.Fatalf("route feature state=%+v", route)
	}
}

func TestMLModelTrainingCandidateCreateForwardsButDoesNotPersistOperation(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, peerConsumer, availability := newMLModelProcessorTestSubject()
	_ = anlfBackend
	_ = peerConsumer
	_ = availability
	requestBody := bytes.Replace(
		candidateTrainingBody(ctx.NfId),
		[]byte(`"mLPreFlag":true,`),
		[]byte(`"mLPreFlag":true,"x-retainedResultReq":true,`),
		1,
	)
	acceptedBody, err := replaceTrainingNotificationURI(
		candidateTrainingBody(ctx.NfId), processor.mtlfCallbackURI(mlModelTrainingCallbackPath),
	)
	if err != nil {
		t.Fatal(err)
	}
	mtlfBackend.trainingCreateResponse = &backend.StandardResponse{
		StatusCode: http.StatusCreated,
		Location: "http://mtlf.internal/internal/v1/ml-model-training/subscriptions/" +
			testProvisionID,
		ContentType: "application/json",
		Body:        acceptedBody,
	}

	response, problem := processor.HandleCreateMLModelTraining(context.Background(), requestBody)
	if problem != nil || response == nil || response.StatusCode != http.StatusCreated {
		t.Fatalf("response=%+v problem=%+v", response, problem)
	}
	if !bytes.Contains(mtlfBackend.trainingBody, []byte(`"x-retainedResultReq":true`)) {
		t.Fatalf("operation was not forwarded to the destination: %s", mtlfBackend.trainingBody)
	}
	routes := ctx.GetAllMLModelTrainingSubscriptionRoutes()
	if len(routes) != 1 ||
		bytes.Contains(response.Body, []byte("x-retainedResultReq")) ||
		bytes.Contains(routes[0].AcceptedRepresentation, []byte("x-retainedResultReq")) ||
		bytes.Contains(routes[0].BackendRepresentation, []byte("x-retainedResultReq")) {
		t.Fatalf("operation was persisted: response=%s routes=%+v", response.Body, routes)
	}
}

func TestMLModelTrainingCreateRejectsMissingOrInvalidLocation(t *testing.T) {
	tests := []struct {
		name     string
		location string
		remote   bool
	}{
		{name: "local missing", location: ""},
		{name: "local invalid resource id", location: "http://mtlf.internal/training/not-a-uuid"},
		{name: "remote missing", location: "", remote: true},
		{name: "remote invalid URI", location: "http://[::1", remote: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			processor, ctx, mtlfBackend, anlfBackend, peerConsumer, availability := newMLModelProcessorTestSubject()
			_ = anlfBackend
			_ = peerConsumer
			_ = availability
			receiverID := ctx.NfId
			var target *backend.SelectedTarget
			if test.remote {
				target = &backend.SelectedTarget{
					NFInstanceID: "10000000-0000-4000-8000-000000000099",
					APIRoot:      "http://peer.example",
				}
				receiverID = target.NFInstanceID
			}
			body := candidateTrainingBody(receiverID)
			createResponse := &backend.StandardResponse{
				StatusCode:   http.StatusCreated,
				Location:     test.location,
				EffectiveURI: "http://peer.example/nnwdaf-mlmodeltraining/v1/subscriptions",
				ContentType:  "application/json",
				Body:         body,
			}

			var response *backend.StandardResponse
			var problem *models.ProblemDetails
			if test.remote {
				peer := &mlModelPeerConsumerStub{trainingCreateResponse: createResponse}
				processor.SetMLModelPeerConsumer(peer)
				response, problem = processor.HandleCreateMLModelTrainingFromBackend(
					context.Background(), body, target,
				)
			} else {
				mtlfBackend.trainingCreateResponse = createResponse
				response, problem = processor.HandleCreateMLModelTraining(context.Background(), body)
			}

			if response != nil || problem == nil || problem.Status != http.StatusBadGateway {
				t.Fatalf("response=%+v problem=%+v", response, problem)
			}
			if routes := ctx.GetAllMLModelTrainingSubscriptionRoutes(); len(routes) != 0 {
				t.Fatalf("invalid create left routes=%+v", routes)
			}
		})
	}
}

func TestMLModelTrainingCandidateReceiverMismatchIsBadRequest(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, peerConsumer, availability := newMLModelProcessorTestSubject()
	_ = ctx
	_ = anlfBackend
	_ = peerConsumer
	_ = availability
	body := candidateTrainingBody("10000000-0000-4000-8000-000000000099")

	response, problem := processor.HandleCreateMLModelTraining(context.Background(), body)
	if response != nil || problem == nil || problem.Status != http.StatusBadRequest ||
		problem.Cause != "INVALID_MSG_FORMAT" || len(problem.InvalidParams) != 1 ||
		problem.InvalidParams[0].Param != "x-flTopology.nfInstanceId" {
		t.Fatalf("response=%+v problem=%+v", response, problem)
	}
	if len(mtlfBackend.trainingBody) != 0 {
		t.Fatalf("invalid request reached backend: %s", mtlfBackend.trainingBody)
	}
}

func TestMLModelTrainingCandidateParseErrorKeepsStructuredPath(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, peerConsumer, availability := newMLModelProcessorTestSubject()
	_ = mtlfBackend
	_ = anlfBackend
	_ = peerConsumer
	_ = availability
	body := bytes.Replace(
		candidateTrainingBody(ctx.NfId),
		[]byte(`"children":[{`),
		[]byte(`"unknown":true,"children":[{`),
		1,
	)

	response, problem := processor.HandleCreateMLModelTraining(context.Background(), body)
	if response != nil || problem == nil || problem.Status != http.StatusBadRequest ||
		problem.Cause != "INVALID_MSG_FORMAT" || len(problem.InvalidParams) != 1 ||
		problem.InvalidParams[0].Param != "x-flTopology.unknown" {
		t.Fatalf("response=%+v problem=%+v", response, problem)
	}
}

func TestMLModelTrainingCandidateRejectsMalformedSupportedFeaturesBeforeDestination(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, peerConsumer, availability := newMLModelProcessorTestSubject()
	_ = anlfBackend
	_ = peerConsumer
	_ = availability
	body := bytes.Replace(
		candidateTrainingBody(ctx.NfId), []byte(`"suppFeats":"4"`), []byte(`"suppFeats":"G"`), 1,
	)

	response, problem := processor.HandleCreateMLModelTraining(context.Background(), body)
	if response != nil || problem == nil || problem.Status != http.StatusBadRequest {
		t.Fatalf("response=%+v problem=%+v", response, problem)
	}
	if len(mtlfBackend.trainingBody) != 0 {
		t.Fatalf("malformed feature mask reached backend: %s", mtlfBackend.trainingBody)
	}
}

func TestMLModelTrainingUnnegotiatedCandidatePatchIsRejected(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, peerConsumer, availability := newMLModelProcessorTestSubject()
	_ = anlfBackend
	_ = peerConsumer
	_ = availability
	body := candidateTrainingBody(ctx.NfId)
	var accepted map[string]any
	if err := json.Unmarshal(body, &accepted); err != nil {
		t.Fatal(err)
	}
	delete(accepted, "suppFeats")
	acceptedBody, err := json.Marshal(accepted)
	if err != nil {
		t.Fatal(err)
	}
	mtlfBackend.trainingCreateResponse = &backend.StandardResponse{
		StatusCode: http.StatusCreated,
		Location: "http://mtlf.internal/internal/v1/ml-model-training/subscriptions/" +
			testProvisionID,
		ContentType: "application/json",
		Body:        acceptedBody,
	}

	response, problem := processor.HandleCreateMLModelTraining(context.Background(), body)
	if problem != nil || response == nil {
		t.Fatalf("create response=%+v problem=%+v", response, problem)
	}
	routes := ctx.GetAllMLModelTrainingSubscriptionRoutes()
	if len(routes) != 1 || routes[0].OfferedSupportedFeatures != "4" ||
		routes[0].NegotiatedSupportedFeatures != "" ||
		routes[0].HierarchicalFLFeatureNegotiated {
		t.Fatalf("route=%+v", routes)
	}
	mtlfBackend.trainingBody = nil
	patch := []byte(fmt.Sprintf(`{"x-flTopology":{"nfInstanceId":%q}}`, ctx.NfId))
	patchResponse, patchProblem := processor.HandlePatchMLModelTraining(
		context.Background(), routes[0].SubscriptionID, patch,
	)
	if patchResponse != nil || patchProblem == nil || patchProblem.Status != http.StatusForbidden ||
		patchProblem.Cause != wire.CauseMLModelTrainingRequirementsNotMet {
		t.Fatalf("patch response=%+v problem=%+v", patchResponse, patchProblem)
	}
	if len(mtlfBackend.trainingBody) != 0 {
		t.Fatalf("unnegotiated patch reached backend: %s", mtlfBackend.trainingBody)
	}

	replaceResponse, replaceProblem := processor.HandleReplaceMLModelTraining(
		context.Background(), routes[0].SubscriptionID, body,
	)
	if replaceResponse != nil || replaceProblem == nil ||
		replaceProblem.Status != http.StatusForbidden {
		t.Fatalf("replace response=%+v problem=%+v", replaceResponse, replaceProblem)
	}
	if len(mtlfBackend.trainingBody) != 0 {
		t.Fatalf("unnegotiated replace reached backend: %s", mtlfBackend.trainingBody)
	}

	notify := []byte(fmt.Sprintf(`{
		"notifCorreId":"candidate-client-a",
		"mlCorreId":"hierarchical-fl-001",
		"x-flTopologyReport":{"nfInstanceId":%q}
	}`, ctx.NfId))
	notifyResponse, notifyProblem := processor.HandleMLModelTrainingNotification(
		context.Background(), "", notify,
	)
	if notifyResponse != nil || notifyProblem == nil || notifyProblem.Status != http.StatusForbidden {
		t.Fatalf("notify response=%+v problem=%+v", notifyResponse, notifyProblem)
	}
}

func TestMLModelTrainingCandidatePatchMergesAndDoesNotPersistOperations(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, peerConsumer, availability := newMLModelProcessorTestSubject()
	_ = anlfBackend
	_ = peerConsumer
	_ = availability
	if _, problem := processor.HandleCreateMLModelTraining(
		context.Background(), candidateTrainingBody(ctx.NfId),
	); problem != nil {
		t.Fatalf("create problem = %+v", problem)
	}
	routes := ctx.GetAllMLModelTrainingSubscriptionRoutes()
	if len(routes) != 1 {
		t.Fatalf("routes = %+v", routes)
	}

	patch := []byte(`{
		"x-retainedResultReq":true,
		"x-flTopology":{"policy":{"minTrainNodes":1}}
	}`)
	response, problem := processor.HandlePatchMLModelTraining(
		context.Background(), routes[0].SubscriptionID, patch,
	)
	if problem != nil || response == nil || response.StatusCode != http.StatusNoContent {
		t.Fatalf("patch response=%+v problem=%+v", response, problem)
	}
	if !bytes.Contains(mtlfBackend.trainingBody, []byte(`"x-retainedResultReq":true`)) {
		t.Fatalf("operation was not visible to destination: %s", mtlfBackend.trainingBody)
	}
	updated, found := ctx.GetMLModelTrainingSubscriptionRoute(routes[0].ResourceKey())
	if !found || bytes.Contains(updated.AcceptedRepresentation, []byte("retainedResultReq")) ||
		bytes.Contains(updated.BackendRepresentation, []byte("retainedResultReq")) ||
		!bytes.Contains(updated.AcceptedRepresentation, []byte(`"children"`)) ||
		!bytes.Contains(updated.AcceptedRepresentation, []byte(`"minTrainNodes":1`)) {
		t.Fatalf("persistent route = %+v found=%v", updated, found)
	}
}

func TestMLModelTrainingCandidatePatchAcceptsAuthoritative200Representation(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, peerConsumer, availability := newMLModelProcessorTestSubject()
	_ = anlfBackend
	_ = peerConsumer
	_ = availability
	if _, problem := processor.HandleCreateMLModelTraining(
		context.Background(), candidateTrainingBody(ctx.NfId),
	); problem != nil {
		t.Fatalf("create problem = %+v", problem)
	}
	routes := ctx.GetAllMLModelTrainingSubscriptionRoutes()
	if len(routes) != 1 {
		t.Fatalf("routes = %+v", routes)
	}
	var accepted map[string]any
	if err := json.Unmarshal(routes[0].BackendRepresentation, &accepted); err != nil {
		t.Fatal(err)
	}
	topology := accepted["x-flTopology"].(map[string]any)
	topology["policy"] = map[string]any{"minTrainNodes": float64(2)}
	responseBody, err := json.Marshal(accepted)
	if err != nil {
		t.Fatal(err)
	}
	mtlfBackend.trainingResponse = &backend.StandardResponse{
		StatusCode:  http.StatusOK,
		ContentType: "application/json",
		Body:        responseBody,
	}

	response, problem := processor.HandlePatchMLModelTraining(
		context.Background(), routes[0].SubscriptionID,
		[]byte(`{"x-flTopology":{"policy":{"minTrainNodes":1}}}`),
	)
	if problem != nil || response == nil || response.StatusCode != http.StatusOK ||
		!bytes.Contains(response.Body, []byte(`"minTrainNodes":2`)) {
		t.Fatalf("response=%+v problem=%+v", response, problem)
	}
	updated, found := ctx.GetMLModelTrainingSubscriptionRoute(routes[0].ResourceKey())
	if !found || !bytes.Contains(updated.AcceptedRepresentation, []byte(`"minTrainNodes":2`)) ||
		!bytes.Contains(updated.BackendRepresentation, []byte(`"minTrainNodes":2`)) ||
		updated.NegotiatedSupportedFeatures != "4" {
		t.Fatalf("updated route=%+v found=%v", updated, found)
	}
}

func TestMLModelTrainingCandidateMutationResponseUsesEffectiveRound(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, peerConsumer, availability := newMLModelProcessorTestSubject()
	_ = anlfBackend
	_ = peerConsumer
	_ = availability
	var create map[string]any
	if err := json.Unmarshal(candidateTrainingBody(ctx.NfId), &create); err != nil {
		t.Fatal(err)
	}
	delete(create, "mLPreFlag")
	delete(create, "mLModelTrainInfos")
	create["roundInd"] = float64(1)
	create["mLModelInfos"] = []any{map[string]any{
		"event":      "UE_COMMUNICATION",
		"mLFileAddr": map[string]any{"mLModelUrl": "http://root.example/round-1"},
	}}
	createBody, err := json.Marshal(create)
	if err != nil {
		t.Fatal(err)
	}
	if _, problem := processor.HandleCreateMLModelTraining(
		context.Background(), createBody,
	); problem != nil {
		t.Fatalf("create problem = %+v", problem)
	}
	routes := ctx.GetAllMLModelTrainingSubscriptionRoutes()
	if len(routes) != 1 {
		t.Fatalf("routes = %+v", routes)
	}

	responseValue := create
	responseValue["notifUri"] = "http://192.0.2.21:8091/internal/v1/ml-model-training/notifications"
	responseValue["roundInd"] = float64(2)
	responseValue["immReport"] = map[string]any{
		"notifCorreId": "candidate-client-a",
		"mlCorreId":    "hierarchical-fl-001",
		"roundInd":     float64(2),
		"x-flTopologyReport": map[string]any{
			"nfInstanceId": ctx.NfId,
		},
	}
	responseBody, err := json.Marshal(responseValue)
	if err != nil {
		t.Fatal(err)
	}
	mtlfBackend.trainingResponse = &backend.StandardResponse{
		StatusCode: http.StatusOK, ContentType: "application/json", Body: responseBody,
	}

	response, problem := processor.HandlePatchMLModelTraining(
		context.Background(), routes[0].SubscriptionID,
		[]byte(`{"roundInd":2,"x-flTopology":{"policy":{"minTrainNodes":1}}}`),
	)
	if problem != nil || response == nil || response.StatusCode != http.StatusOK {
		t.Fatalf("response=%+v problem=%+v", response, problem)
	}
}

func TestMLModelTrainingCandidatePutIsFullReplacement(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, peerConsumer, availability := newMLModelProcessorTestSubject()
	_ = mtlfBackend
	_ = anlfBackend
	_ = peerConsumer
	_ = availability
	body := candidateTrainingBody(ctx.NfId)
	if _, problem := processor.HandleCreateMLModelTraining(context.Background(), body); problem != nil {
		t.Fatalf("create problem = %+v", problem)
	}
	routes := ctx.GetAllMLModelTrainingSubscriptionRoutes()
	if len(routes) != 1 {
		t.Fatalf("routes = %+v", routes)
	}
	var replacement map[string]any
	if err := json.Unmarshal(body, &replacement); err != nil {
		t.Fatal(err)
	}
	delete(replacement, "x-flTopology")
	replacementBody, err := json.Marshal(replacement)
	if err != nil {
		t.Fatal(err)
	}

	response, problem := processor.HandleReplaceMLModelTraining(
		context.Background(), routes[0].SubscriptionID, replacementBody,
	)
	if problem != nil || response == nil || response.StatusCode != http.StatusNoContent {
		t.Fatalf("replace response=%+v problem=%+v", response, problem)
	}
	updated, found := ctx.GetMLModelTrainingSubscriptionRoute(routes[0].ResourceKey())
	if !found || bytes.Contains(updated.AcceptedRepresentation, []byte("x-flTopology")) ||
		bytes.Contains(updated.BackendRepresentation, []byte("x-flTopology")) ||
		updated.NegotiatedSupportedFeatures != "4" {
		t.Fatalf("updated route=%+v found=%v", updated, found)
	}
}

func TestMLModelTrainingCandidatePutForwardsButDoesNotPersistOperation(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, peerConsumer, availability := newMLModelProcessorTestSubject()
	_ = anlfBackend
	_ = peerConsumer
	_ = availability
	body := candidateTrainingBody(ctx.NfId)
	if _, problem := processor.HandleCreateMLModelTraining(context.Background(), body); problem != nil {
		t.Fatalf("create problem = %+v", problem)
	}
	routes := ctx.GetAllMLModelTrainingSubscriptionRoutes()
	if len(routes) != 1 {
		t.Fatalf("routes = %+v", routes)
	}
	replacement := bytes.Replace(
		body,
		[]byte(`"mLPreFlag":true,`),
		[]byte(`"mLPreFlag":true,"x-retainedResultReq":true,`),
		1,
	)

	response, problem := processor.HandleReplaceMLModelTraining(
		context.Background(), routes[0].SubscriptionID, replacement,
	)
	if problem != nil || response == nil || response.StatusCode != http.StatusNoContent {
		t.Fatalf("replace response=%+v problem=%+v", response, problem)
	}
	if !bytes.Contains(mtlfBackend.trainingBody, []byte(`"x-retainedResultReq":true`)) {
		t.Fatalf("operation was not forwarded to the destination: %s", mtlfBackend.trainingBody)
	}
	updated, found := ctx.GetMLModelTrainingSubscriptionRoute(routes[0].ResourceKey())
	if !found ||
		bytes.Contains(updated.AcceptedRepresentation, []byte("x-retainedResultReq")) ||
		bytes.Contains(updated.BackendRepresentation, []byte("x-retainedResultReq")) {
		t.Fatalf("operation was persisted: route=%+v found=%v", updated, found)
	}
}

func TestMLModelTrainingCandidatePutAcceptsAuthoritative200Representation(t *testing.T) {
	for _, remote := range []bool{false, true} {
		name := "local"
		if remote {
			name = "remote"
		}
		t.Run(name, func(t *testing.T) {
			processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
			_ = anlfBackend
			_ = mtlfAvailability
			_ = anlfAvailability
			receiverID := ctx.NfId
			var target *backend.SelectedTarget
			var peer *mlModelPeerConsumerStub
			if remote {
				target = &backend.SelectedTarget{
					NFInstanceID: "10000000-0000-4000-8000-000000000099",
					APIRoot:      "http://peer.example",
				}
				receiverID = target.NFInstanceID
				peer = &mlModelPeerConsumerStub{}
				processor.SetMLModelPeerConsumer(peer)
			}
			body := candidateTrainingBody(receiverID)
			var createProblem *models.ProblemDetails
			if remote {
				_, createProblem = processor.HandleCreateMLModelTrainingFromBackend(
					context.Background(), body, target,
				)
			} else {
				_, createProblem = processor.HandleCreateMLModelTraining(context.Background(), body)
			}
			if createProblem != nil {
				t.Fatalf("create problem = %+v", createProblem)
			}
			routes := ctx.GetAllMLModelTrainingSubscriptionRoutes()
			if len(routes) != 1 {
				t.Fatalf("routes = %+v", routes)
			}
			replacement := bytes.Replace(
				body,
				[]byte(`"x-flTopology":{`),
				[]byte(`"x-flTopology":{"policy":{"minTrainNodes":1},`),
				1,
			)
			destinationBody, err := replaceTrainingNotificationURI(
				replacement, trainingRouteCallbackURI(processor, routes[0]),
			)
			if err != nil {
				t.Fatal(err)
			}
			destinationResponse := &backend.StandardResponse{
				StatusCode:  http.StatusOK,
				ContentType: "application/json",
				Body:        destinationBody,
			}
			if remote {
				peer.trainingReplaceResponse = destinationResponse
			} else {
				mtlfBackend.trainingReplaceResponse = destinationResponse
			}

			var response *backend.StandardResponse
			var problem *models.ProblemDetails
			if remote {
				response, problem = processor.HandleReplaceMLModelTrainingFromBackend(
					context.Background(), target.NFInstanceID, routes[0].SubscriptionID, replacement,
				)
			} else {
				response, problem = processor.HandleReplaceMLModelTraining(
					context.Background(), routes[0].SubscriptionID, replacement,
				)
			}
			if problem != nil || response == nil || response.StatusCode != http.StatusOK ||
				!bytes.Contains(response.Body, []byte(`"minTrainNodes":1`)) {
				t.Fatalf("response=%+v problem=%+v", response, problem)
			}
			updated, found := ctx.GetMLModelTrainingSubscriptionRoute(routes[0].ResourceKey())
			if !found ||
				!bytes.Contains(updated.AcceptedRepresentation, []byte(`"minTrainNodes":1`)) ||
				!bytes.Contains(updated.BackendRepresentation, []byte(`"minTrainNodes":1`)) ||
				updated.NegotiatedSupportedFeatures != "4" {
				t.Fatalf("updated route=%+v found=%v", updated, found)
			}
		})
	}
}

func TestMLModelTrainingInvalidCandidateMutationResponseRollsBack(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, peerConsumer, availability := newMLModelProcessorTestSubject()
	_ = anlfBackend
	_ = peerConsumer
	_ = availability
	if _, problem := processor.HandleCreateMLModelTraining(
		context.Background(), candidateTrainingBody(ctx.NfId),
	); problem != nil {
		t.Fatalf("create problem = %+v", problem)
	}
	routes := ctx.GetAllMLModelTrainingSubscriptionRoutes()
	before := routes[0]
	invalidResponse := candidateTrainingBody(ctx.NfId)
	invalidResponse = bytes.Replace(
		invalidResponse,
		[]byte(`"mLPreFlag":true,`),
		[]byte(`"mLPreFlag":true,"x-retainedResultReq":false,`),
		1,
	)
	mtlfBackend.trainingResponse = &backend.StandardResponse{
		StatusCode:  http.StatusOK,
		ContentType: "application/json",
		Body:        invalidResponse,
	}

	response, problem := processor.HandlePatchMLModelTraining(
		context.Background(), before.SubscriptionID,
		[]byte(`{"x-flTopology":{"policy":{"minTrainNodes":1}}}`),
	)
	if response != nil || problem == nil || problem.Status != http.StatusBadGateway {
		t.Fatalf("response=%+v problem=%+v", response, problem)
	}
	after, found := ctx.GetMLModelTrainingSubscriptionRoute(before.ResourceKey())
	if !found || after.PeerRoute.LifecycleState != before.PeerRoute.LifecycleState ||
		!bytes.Equal(after.AcceptedRepresentation, before.AcceptedRepresentation) ||
		!bytes.Equal(after.BackendRepresentation, before.BackendRepresentation) {
		t.Fatalf("route changed after invalid success: before=%+v after=%+v", before, after)
	}
}

func TestMLModelTrainingCandidateDestinationFailureRollsBack(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, peerConsumer, availability := newMLModelProcessorTestSubject()
	_ = anlfBackend
	_ = peerConsumer
	_ = availability
	if _, problem := processor.HandleCreateMLModelTraining(
		context.Background(), candidateTrainingBody(ctx.NfId),
	); problem != nil {
		t.Fatalf("create problem = %+v", problem)
	}
	routes := ctx.GetAllMLModelTrainingSubscriptionRoutes()
	before := routes[0]
	mtlfBackend.trainingError = &backend.TransportError{
		Operation: "patch training subscription",
		Cause:     fmt.Errorf("destination unavailable"),
	}

	response, problem := processor.HandlePatchMLModelTraining(
		context.Background(), before.SubscriptionID,
		[]byte(`{"x-flTopology":{"policy":{"minTrainNodes":1}}}`),
	)
	if response != nil || problem == nil || problem.Status != http.StatusServiceUnavailable {
		t.Fatalf("response=%+v problem=%+v", response, problem)
	}
	after, found := ctx.GetMLModelTrainingSubscriptionRoute(before.ResourceKey())
	if !found || after.PeerRoute.LifecycleState != before.PeerRoute.LifecycleState ||
		after.PeerRoute.OperationRevision == before.PeerRoute.OperationRevision ||
		!bytes.Equal(after.AcceptedRepresentation, before.AcceptedRepresentation) ||
		!bytes.Equal(after.BackendRepresentation, before.BackendRepresentation) ||
		after.NegotiatedSupportedFeatures != before.NegotiatedSupportedFeatures ||
		after.HierarchicalFLFeatureNegotiated != before.HierarchicalFLFeatureNegotiated {
		t.Fatalf("route changed after destination failure: before=%+v after=%+v", before, after)
	}
}

func TestMLModelTrainingStaleCandidatePatchCannotOverwriteNewerRoute(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, peerConsumer, availability := newMLModelProcessorTestSubject()
	_ = anlfBackend
	_ = peerConsumer
	_ = availability
	if _, problem := processor.HandleCreateMLModelTraining(
		context.Background(), candidateTrainingBody(ctx.NfId),
	); problem != nil {
		t.Fatalf("create problem = %+v", problem)
	}
	routes := ctx.GetAllMLModelTrainingSubscriptionRoutes()
	before := routes[0]
	started := make(chan struct{})
	release := make(chan struct{})
	mtlfBackend.trainingPatchFunc = func(
		_ context.Context,
		_ string,
		_ []byte,
	) (*backend.StandardResponse, error) {
		close(started)
		<-release
		return &backend.StandardResponse{StatusCode: http.StatusNoContent}, nil
	}
	result := make(chan struct {
		response *backend.StandardResponse
		problem  *models.ProblemDetails
	}, 1)
	go func() {
		response, problem := processor.HandlePatchMLModelTraining(
			t.Context(), before.SubscriptionID,
			[]byte(`{"x-flTopology":{"policy":{"minTrainNodes":1}}}`),
		)
		result <- struct {
			response *backend.StandardResponse
			problem  *models.ProblemDetails
		}{response: response, problem: problem}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("candidate patch did not reach the destination")
	}

	processor.mlModelMu.Lock()
	newer, found := ctx.GetMLModelTrainingSubscriptionRoute(before.ResourceKey())
	if !found {
		processor.mlModelMu.Unlock()
		t.Fatal("candidate route disappeared while patch was in flight")
	}
	restoreActiveMLModelRoute(&newer.PeerRoute)
	newer.PeerRoute.OperationRevision = processor.nextMLModelOperationRevisionLocked()
	if !ctx.UpdateMLModelTrainingSubscriptionRoute(newer) {
		processor.mlModelMu.Unlock()
		t.Fatal("could not install newer candidate route revision")
	}
	processor.mlModelMu.Unlock()
	close(release)

	select {
	case got := <-result:
		if got.response != nil || got.problem == nil ||
			got.problem.Status != http.StatusServiceUnavailable {
			t.Fatalf("response=%+v problem=%+v", got.response, got.problem)
		}
	case <-time.After(time.Second):
		t.Fatal("stale candidate patch did not complete")
	}
	after, found := ctx.GetMLModelTrainingSubscriptionRoute(before.ResourceKey())
	if !found || after.PeerRoute.OperationRevision != newer.PeerRoute.OperationRevision ||
		!bytes.Equal(after.AcceptedRepresentation, newer.AcceptedRepresentation) ||
		!bytes.Equal(after.BackendRepresentation, newer.BackendRepresentation) ||
		bytes.Contains(after.AcceptedRepresentation, []byte(`"minTrainNodes":1`)) {
		t.Fatalf("stale patch overwrote the newer route: route=%+v found=%v", after, found)
	}
}

func TestMLModelTrainingGenerationResetClearsCandidateState(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
	_ = mtlfBackend
	_ = anlfBackend
	_ = anlfAvailability
	if _, problem := processor.HandleCreateMLModelTraining(
		context.Background(), candidateTrainingBody(ctx.NfId),
	); problem != nil {
		t.Fatalf("create problem = %+v", problem)
	}
	routes := ctx.GetAllMLModelTrainingSubscriptionRoutes()
	if len(routes) != 1 || !routes[0].HierarchicalFLFeatureNegotiated {
		t.Fatalf("candidate route=%+v", routes)
	}

	processor.ResetMLModelBackendGeneration(
		context.Background(), backend.KindMTLF, mtlfAvailability.generation,
	)
	if _, found := ctx.GetMLModelTrainingSubscriptionRoute(routes[0].ResourceKey()); found {
		t.Fatal("candidate route survived backend generation reset")
	}
	response, problem := processor.HandlePatchMLModelTraining(
		context.Background(), routes[0].SubscriptionID,
		[]byte(`{"x-flTopology":{"policy":{"minTrainNodes":1}}}`),
	)
	if response != nil || problem == nil || problem.Status != http.StatusNotFound {
		t.Fatalf("late patch response=%+v problem=%+v", response, problem)
	}
}

func TestMLModelTrainingCreateRejectsUnsupportedNegotiatedFeaturesAndCompensates(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, peerConsumer, availability := newMLModelProcessorTestSubject()
	_ = anlfBackend
	_ = peerConsumer
	_ = availability
	responseBody := bytes.Replace(
		candidateTrainingBody(ctx.NfId), []byte(`"suppFeats":"4"`), []byte(`"suppFeats":"8"`), 1,
	)
	mtlfBackend.trainingCreateResponse = &backend.StandardResponse{
		StatusCode:  http.StatusCreated,
		Location:    "http://mtlf.internal/internal/v1/ml-model-training/subscriptions/" + testProvisionID,
		ContentType: "application/json",
		Body:        responseBody,
	}

	response, problem := processor.HandleCreateMLModelTraining(
		context.Background(), candidateTrainingBody(ctx.NfId),
	)
	if response != nil || problem == nil || problem.Status != http.StatusBadGateway {
		t.Fatalf("response=%+v problem=%+v", response, problem)
	}
	if mtlfBackend.deletedTrainingBackend != mtlfBackend.trainingCreateID ||
		len(ctx.GetAllMLModelTrainingSubscriptionRoutes()) != 0 {
		t.Fatalf(
			"compensation id=%q routes=%+v",
			mtlfBackend.deletedTrainingBackend,
			ctx.GetAllMLModelTrainingSubscriptionRoutes(),
		)
	}
}

func TestMLModelTrainingCreateRejectsWriteOnlyInstructionInSuccess(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, peerConsumer, availability := newMLModelProcessorTestSubject()
	_ = anlfBackend
	_ = peerConsumer
	_ = availability
	responseBody := bytes.Replace(
		candidateTrainingBody(ctx.NfId),
		[]byte(`"mLPreFlag":true,`),
		[]byte(`"mLPreFlag":true,"x-retainedResultReq":false,`),
		1,
	)
	mtlfBackend.trainingCreateResponse = &backend.StandardResponse{
		StatusCode: http.StatusCreated,
		Location: "http://mtlf.internal/internal/v1/ml-model-training/subscriptions/" +
			testProvisionID,
		ContentType: "application/json",
		Body:        responseBody,
	}

	response, problem := processor.HandleCreateMLModelTraining(
		context.Background(), candidateTrainingBody(ctx.NfId),
	)
	if response != nil || problem == nil || problem.Status != http.StatusBadGateway {
		t.Fatalf("response=%+v problem=%+v", response, problem)
	}
	if mtlfBackend.deletedTrainingBackend != mtlfBackend.trainingCreateID ||
		len(ctx.GetAllMLModelTrainingSubscriptionRoutes()) != 0 {
		t.Fatalf(
			"compensation id=%q routes=%+v",
			mtlfBackend.deletedTrainingBackend,
			ctx.GetAllMLModelTrainingSubscriptionRoutes(),
		)
	}
}

func TestMLModelTrainingCreateRejectsImmediateReportFromWrongParticipant(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, peerConsumer, availability := newMLModelProcessorTestSubject()
	_ = anlfBackend
	_ = peerConsumer
	_ = availability
	var response map[string]any
	if err := json.Unmarshal(candidateTrainingBody(ctx.NfId), &response); err != nil {
		t.Fatal(err)
	}
	response["immReport"] = map[string]any{
		"notifCorreId": "candidate-client-a",
		"mlCorreId":    "hierarchical-fl-001",
		"x-flTopologyReport": map[string]any{
			"nfInstanceId": "10000000-0000-4000-8000-000000000099",
		},
	}
	responseBody, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	mtlfBackend.trainingCreateResponse = &backend.StandardResponse{
		StatusCode: http.StatusCreated,
		Location: "http://mtlf.internal/internal/v1/ml-model-training/subscriptions/" +
			testProvisionID,
		ContentType: "application/json",
		Body:        responseBody,
	}

	result, problem := processor.HandleCreateMLModelTraining(
		context.Background(), candidateTrainingBody(ctx.NfId),
	)
	if result != nil || problem == nil || problem.Status != http.StatusBadGateway {
		t.Fatalf("result=%+v problem=%+v", result, problem)
	}
	if mtlfBackend.deletedTrainingBackend != mtlfBackend.trainingCreateID ||
		len(ctx.GetAllMLModelTrainingSubscriptionRoutes()) != 0 {
		t.Fatalf(
			"compensation id=%q routes=%+v",
			mtlfBackend.deletedTrainingBackend,
			ctx.GetAllMLModelTrainingSubscriptionRoutes(),
		)
	}
}

func TestRemoteCandidateTrainingCreateAndNotifyUseBoundParticipant(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, peerConsumer, availability := newMLModelProcessorTestSubject()
	_ = anlfBackend
	_ = peerConsumer
	_ = availability
	peer := &mlModelPeerConsumerStub{}
	processor.SetMLModelPeerConsumer(peer)
	target := backend.SelectedTarget{
		NFInstanceID: "10000000-0000-4000-8000-000000000099",
		APIRoot:      "http://peer.example",
	}
	response, problem := processor.HandleCreateMLModelTrainingFromBackend(
		context.Background(), candidateTrainingBody(target.NFInstanceID), &target,
	)
	if problem != nil || response == nil || response.StatusCode != http.StatusCreated {
		t.Fatalf("create response=%+v problem=%+v", response, problem)
	}
	if !bytes.Contains(peer.trainingBody, []byte(`"x-flTopology"`)) ||
		!bytes.Contains(peer.trainingBody, []byte("/nnwdaf-callback/v1/ml-model-training/")) {
		t.Fatalf("peer body = %s", peer.trainingBody)
	}
	routes := ctx.GetAllMLModelTrainingSubscriptionRoutes()
	if len(routes) != 1 || routes[0].BoundParticipantNFInstanceID != target.NFInstanceID ||
		!routes[0].HierarchicalFLFeatureNegotiated {
		t.Fatalf("route = %+v", routes)
	}
	patch := []byte(`{"x-flTopology":{"policy":{"minTrainNodes":1}}}`)
	patchResponse, patchProblem := processor.HandlePatchMLModelTrainingFromBackend(
		context.Background(), target.NFInstanceID, routes[0].SubscriptionID, patch,
	)
	if patchProblem != nil || patchResponse == nil || patchResponse.StatusCode != http.StatusNoContent ||
		!bytes.Equal(peer.trainingPatchBody, patch) {
		t.Fatalf(
			"patch response=%+v problem=%+v peerBody=%s",
			patchResponse, patchProblem, peer.trainingPatchBody,
		)
	}

	validNotify := []byte(fmt.Sprintf(`{
		"notifCorreId":"candidate-client-a",
		"mlCorreId":"hierarchical-fl-001",
		"x-flTopologyReport":{"nfInstanceId":%q}
	}`, target.NFInstanceID))
	notifyResponse, notifyProblem := processor.HandleMLModelTrainingNotification(
		context.Background(), routes[0].CallbackRouteID, validNotify,
	)
	if notifyProblem != nil || notifyResponse == nil || notifyResponse.StatusCode != http.StatusNoContent ||
		!bytes.Equal(mtlfBackend.trainingNotification, validNotify) {
		t.Fatalf("notify response=%+v problem=%+v body=%s", notifyResponse, notifyProblem, mtlfBackend.trainingNotification)
	}

	mtlfBackend.trainingNotification = nil
	invalidNotify := bytes.Replace(
		validNotify, []byte(target.NFInstanceID),
		[]byte("10000000-0000-4000-8000-000000000098"), 1,
	)
	notifyResponse, notifyProblem = processor.HandleMLModelTrainingNotification(
		context.Background(), routes[0].CallbackRouteID, invalidNotify,
	)
	if notifyResponse != nil || notifyProblem == nil || notifyProblem.Status != http.StatusBadRequest ||
		len(mtlfBackend.trainingNotification) != 0 {
		t.Fatalf(
			"invalid notify response=%+v problem=%+v body=%s",
			notifyResponse, notifyProblem, mtlfBackend.trainingNotification,
		)
	}
}

func TestRemoteTrainingCallbackWaitsForResourceBinding(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, peerConsumer, availability := newMLModelProcessorTestSubject()
	_ = anlfBackend
	_ = peerConsumer
	_ = availability
	target := backend.SelectedTarget{
		NFInstanceID: "10000000-0000-4000-8000-000000000099",
		APIRoot:      "http://peer.example",
	}
	notification := []byte(fmt.Sprintf(`{
		"notifCorreId":"candidate-client-a",
		"mlCorreId":"hierarchical-fl-001",
		"x-flTopologyReport":{"nfInstanceId":%q}
	}`, target.NFInstanceID))
	callbackID := ""
	peer := &mlModelPeerConsumerStub{}
	peer.trainingCreateHook = func() {
		value, err := wire.ParseNwdafMLModelTrainSubsc(peer.trainingBody)
		if err != nil {
			t.Fatal(err)
		}
		callbackID = value.NotificationURI[strings.LastIndex(value.NotificationURI, "/")+1:]
		_, problem := processor.HandleMLModelTrainingNotification(t.Context(), callbackID, notification)
		if problem == nil || problem.Status != http.StatusServiceUnavailable {
			t.Fatalf("early callback problem = %+v, want 503", problem)
		}
	}
	processor.SetMLModelPeerConsumer(peer)
	response, problem := processor.HandleCreateMLModelTrainingFromBackend(
		t.Context(), candidateTrainingBody(target.NFInstanceID), &target,
	)
	if problem != nil || response == nil || callbackID == "" {
		t.Fatalf("create response=%+v problem=%+v callbackID=%q", response, problem, callbackID)
	}
	routes := ctx.GetAllMLModelTrainingSubscriptionRoutes()
	if len(routes) != 1 || routes[0].CallbackRouteID != callbackID {
		t.Fatalf("active routes = %+v", routes)
	}
	delivered, problem := processor.HandleMLModelTrainingNotification(t.Context(), callbackID, notification)
	if problem != nil || delivered == nil || delivered.StatusCode != http.StatusNoContent ||
		!bytes.Equal(mtlfBackend.trainingNotification, notification) {
		t.Fatalf("callback response=%+v problem=%+v", delivered, problem)
	}
}

func TestRemoteTrainingScopesSamePeerResourceIDByNF(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, peerConsumer, availability := newMLModelProcessorTestSubject()
	_ = anlfBackend
	_ = peerConsumer
	_ = availability
	const resourceID = "shared resource"
	const escapedResourceID = "shared%20resource"
	peer := &mlModelPeerConsumerStub{
		trainingCreateFunc: func(target backend.SelectedTarget, body []byte) (*backend.StandardResponse, error) {
			return &backend.StandardResponse{
				StatusCode:   http.StatusCreated,
				Location:     target.APIRoot + "/nnwdaf-mlmodeltraining/v1/subscriptions/" + escapedResourceID,
				EffectiveURI: target.APIRoot + "/nnwdaf-mlmodeltraining/v1/subscriptions",
				ContentType:  "application/json", Body: append([]byte(nil), body...),
			}, nil
		},
	}
	processor.SetMLModelPeerConsumer(peer)
	targets := []backend.SelectedTarget{
		{NFInstanceID: "10000000-0000-4000-8000-000000000091", APIRoot: "http://peer-a.example"},
		{NFInstanceID: "10000000-0000-4000-8000-000000000092", APIRoot: "http://peer-b.example"},
	}
	for i, target := range targets {
		body := candidateTrainingBody(target.NFInstanceID)
		if i == 1 {
			body = bytes.Replace(body, []byte("candidate-client-a"), []byte("candidate-client-b"), 1)
		}
		response, problem := processor.HandleCreateMLModelTrainingFromBackend(t.Context(), body, &target)
		if problem != nil || response == nil || response.StatusCode != http.StatusCreated ||
			!strings.Contains(response.Location, "/targets/"+target.NFInstanceID+"/subscriptions/"+escapedResourceID) {
			t.Fatalf("create target=%s response=%+v problem=%+v", target.NFInstanceID, response, problem)
		}
	}
	for _, target := range targets {
		key := nwdaf_context.MLModelTrainingResourceKey{
			Direction:         nwdaf_context.MLModelRouteDirectionOutbound,
			OwnerNFInstanceID: target.NFInstanceID, SubscriptionID: resourceID,
		}
		route, found := ctx.GetMLModelTrainingSubscriptionRoute(key)
		if !found || route.CallbackRouteID == "" {
			t.Fatalf("route target=%s: %+v found=%t", target.NFInstanceID, route, found)
		}
		patch := []byte(`{"x-flTopology":{"policy":{"minTrainNodes":1}}}`)
		response, problem := processor.HandlePatchMLModelTrainingFromBackend(
			t.Context(), target.NFInstanceID, resourceID, patch,
		)
		if problem != nil || response == nil || response.StatusCode != http.StatusNoContent ||
			peer.trainingPatchLocation != target.APIRoot+"/nnwdaf-mlmodeltraining/v1/subscriptions/"+escapedResourceID {
			t.Fatalf("patch target=%s response=%+v problem=%+v location=%q",
				target.NFInstanceID, response, problem, peer.trainingPatchLocation)
		}
		correlation := "candidate-client-a"
		if target == targets[1] {
			correlation = "candidate-client-b"
		}
		notification := []byte(fmt.Sprintf(`{
			"notifCorreId":%q,"mlCorreId":"hierarchical-fl-001",
			"x-flTopologyReport":{"nfInstanceId":%q}
		}`, correlation, target.NFInstanceID))
		response, problem = processor.HandleMLModelTrainingNotification(
			t.Context(), route.CallbackRouteID, notification,
		)
		if problem != nil || response == nil || response.StatusCode != http.StatusNoContent ||
			!bytes.Equal(mtlfBackend.trainingNotification, notification) {
			t.Fatalf("notify target=%s response=%+v problem=%+v", target.NFInstanceID, response, problem)
		}
	}
	first := targets[0]
	response, problem := processor.HandleDeleteMLModelTrainingFromBackend(t.Context(), first.NFInstanceID, resourceID)
	if problem != nil || response == nil || response.StatusCode != http.StatusNoContent {
		t.Fatalf("delete response=%+v problem=%+v", response, problem)
	}
	secondKey := nwdaf_context.MLModelTrainingResourceKey{
		Direction:         nwdaf_context.MLModelRouteDirectionOutbound,
		OwnerNFInstanceID: targets[1].NFInstanceID, SubscriptionID: resourceID,
	}
	if _, found := ctx.GetMLModelTrainingSubscriptionRoute(secondKey); !found {
		t.Fatal("deleting first peer removed second peer route")
	}
	if len(peer.deletedTraining) != 1 || peer.deletedTraining[0] !=
		first.APIRoot+"/nnwdaf-mlmodeltraining/v1/subscriptions/"+escapedResourceID {
		t.Fatalf("deleted locations = %+v", peer.deletedTraining)
	}
}

func TestRetainedResultFoundDoesNotUseNormalRoundEquality(t *testing.T) {
	value, err := wire.ParseNwdafMLModelTrainNotif([]byte(`{
		"notifCorreId":"candidate-client-a",
		"mlCorreId":"hierarchical-fl-001",
		"x-retainedResultStatus":"FOUND",
		"roundInd":5,
		"mLModelInfos":[{
			"event":"UE_COMMUNICATION",
			"mLFileAddr":{"mLModelUrl":"http://leaf.example/round-5"}
		}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	expected := int64(1)
	if validationErr := wire.ValidateFLNotification(value, &wire.TrainingResourceIdentity{
		MLCorrelationID:           "hierarchical-fl-001",
		NotificationCorrelationID: "candidate-client-a",
		ExpectedRoundIndicator:    &expected,
	}); validationErr != nil {
		t.Fatalf("retained result used normal round equality: %v", validationErr)
	}

	notFound, err := wire.ParseNwdafMLModelTrainNotif([]byte(`{
		"notifCorreId":"candidate-client-a",
		"mlCorreId":"hierarchical-fl-001",
		"x-retainedResultStatus":"NOT_FOUND"
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if validationErr := wire.ValidateFLNotification(notFound, &wire.TrainingResourceIdentity{
		MLCorrelationID:           "hierarchical-fl-001",
		NotificationCorrelationID: "candidate-client-a",
		ExpectedRoundIndicator:    &expected,
	}); validationErr != nil {
		t.Fatalf("retained NOT_FOUND used normal round equality: %v", validationErr)
	}
}
