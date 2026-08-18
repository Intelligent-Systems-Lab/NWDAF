package processor

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/free5gc/nwdaf/internal/backend"
	wire "github.com/free5gc/nwdaf/internal/compat/mlmodeltraining"
)

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
	updated, found := ctx.GetMLModelTrainingSubscriptionRoute(routes[0].SubscriptionID)
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
