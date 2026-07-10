package client

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/free5gc/nwdaf/internal/anlf"
	anlfprocessor "github.com/free5gc/nwdaf/internal/anlf/processor"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/notifier"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

type liveModelWorkflow struct{}

func (*liveModelWorkflow) PlanModelProvisionActions(*models.NwdafMlModelProvNotif) []anlf.ModelProvisionAction {
	return nil
}

func (*liveModelWorkflow) StartModelProvisionActions([]anlf.ModelProvisionAction) {}

func TestLivePyAnLFContract(t *testing.T) {
	endpoint := os.Getenv("PYANLF_LIVE_ENDPOINT")
	if endpoint == "" {
		t.Skip("PYANLF_LIVE_ENDPOINT is not set")
	}

	externalReport := make(chan string, 1)
	consumer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		body, readErr := io.ReadAll(request.Body)
		if readErr != nil {
			t.Errorf("ReadAll() error = %v", readErr)
		}
		select {
		case externalReport <- string(body):
		default:
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer consumer.Close()

	probe, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve callback port: %v", err)
	}
	callbackPort := probe.Addr().(*net.TCPAddr).Port
	if closeErr := probe.Close(); closeErr != nil {
		t.Fatalf("release callback port: %v", closeErr)
	}
	serverConfig := &factory.Config{Configuration: &factory.Configuration{
		Anlf: &factory.AnlfConfig{Server: &factory.AuxiliaryServerConfig{
			BindingIPv4:  "127.0.0.1",
			RegisterIPv4: "127.0.0.1",
			Port:         callbackPort,
		}},
	}}

	nwdaf_context.Init()
	const subscriptionID = "live-contract-subscription"
	subscription := &nwdaf_context.Subscription{
		ID:              subscriptionID,
		NotificationURI: consumer.URL,
		NotifCorrId:     "live-correlation",
		EventSubs: []models.NwdafEventsSubscriptionEventSubscription{{
			Event: models.NwdafEvent_UE_COMMUNICATION,
		}},
		IsActive: true,
	}
	nwdaf_context.GetSelf().AddSubscription(subscription)
	dispatcher := notifier.NewReportDispatcher(context.Background(), nil)
	processor := anlfprocessor.NewProcessor(&liveModelWorkflow{}, dispatcher)
	callbackServer, err := anlf.NewServer(serverConfig, processor)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	var serverWG sync.WaitGroup
	if err = callbackServer.Run(&serverWG); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	defer func() {
		callbackServer.Shutdown()
		serverWG.Wait()
	}()

	client := NewClient(endpoint)
	request := anlf.ApplySubscriptionRuntimeRequest{
		Subscription: anlf.SubscriptionRuntimeContext{
			SubscriptionID: subscriptionID,
			EvtReq: &models.ReportingInformation{
				NotifMethod:  models.SmfEventExposureNotificationMethod_PERIODIC,
				RepPeriod:    1,
				MaxReportNbr: 1,
			},
			EventSubscriptions: []models.NwdafEventsSubscriptionEventSubscription{{
				Event: models.NwdafEvent_UE_COMMUNICATION,
			}},
		},
		ReportCallbackURI: serverConfig.GetAnlfServerURI() +
			"/subscriptions/" + subscriptionID + "/analytics-reports",
	}

	response, err := client.ApplySubscriptionRuntime(context.Background(), request)
	if err != nil {
		t.Fatalf("ApplySubscriptionRuntime() error = %v", err)
	}
	defer func() {
		if releaseErr := client.ReleaseSubscriptionRuntime(context.Background(), subscriptionID); releaseErr != nil {
			t.Errorf("ReleaseSubscriptionRuntime() error = %v", releaseErr)
		}
	}()
	if response.RuntimeRevision <= 0 || response.CollectionRequirements.SamplingIntervalSeconds <= 0 {
		t.Fatalf("incomplete apply response: %+v", response)
	}
	subscription.SetRuntime(
		response.RuntimeRevision,
		nwdaf_context.CollectionRequirements{
			SamplingIntervalSeconds: response.CollectionRequirements.SamplingIntervalSeconds,
			RequiredMeasurements:    response.CollectionRequirements.RequiredMeasurements,
		},
		nil,
	)

	binding := anlf.ObservationBinding{
		ObservationSourceID: "live-source",
		Source:              anlf.ObservationSource{SourceType: "SMF_UPF", Supi: "imsi-live"},
		CollectionProfile:   response.CollectionRequirements,
	}
	if err = client.SyncObservationBindings(
		context.Background(),
		subscriptionID,
		anlf.SyncObservationBindingsRequest{
			RuntimeRevision: response.RuntimeRevision,
			Bindings:        []anlf.ObservationBinding{binding},
		},
	); err != nil {
		t.Fatalf("SyncObservationBindings() error = %v", err)
	}

	if err = client.SendObservations(context.Background(), "live-source", anlf.ObservationBatch{
		BatchID: "live-batch",
		Observations: []anlf.SourceObservation{{
			ObservedAt: time.Now().UTC(),
			Supi:       "imsi-live",
			Dnn:        "internet",
		}},
	}); err != nil {
		t.Fatalf("SendObservations() error = %v", err)
	}

	select {
	case body := <-externalReport:
		if !strings.Contains(body, `"subscriptionId":"`+subscriptionID+`"`) ||
			!strings.Contains(body, `"notifCorrId":"live-correlation"`) {
			t.Fatalf("unexpected external notification: %s", body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for PyAnLF analytics report callback")
	}
}
