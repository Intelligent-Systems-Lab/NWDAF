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

	anlfserver "github.com/free5gc/nwdaf/internal/anlf"
	"github.com/free5gc/nwdaf/internal/anlf/contract"
	"github.com/free5gc/nwdaf/internal/anlf/coordinator"
	anlfprocessor "github.com/free5gc/nwdaf/internal/anlf/processor"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/sbi/notifier"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

type liveModelWorkflow struct{}

func (*liveModelWorkflow) PlanModelProvisionActions(
	*contract.ModelProvisionNotification,
) []coordinator.ModelProvisionAction {
	return nil
}

func (*liveModelWorkflow) StartModelProvisionActions([]coordinator.ModelProvisionAction) {}

func (*liveModelWorkflow) CompleteSubscriptionRuntime(event *contract.RuntimeCompletionEvent) error {
	subscription := nwdaf_context.GetSelf().GetSubscription(event.SubscriptionID)
	if subscription != nil {
		subscription.CompleteRuntime(event.RuntimeRevision)
	}
	return nil
}

type liveReportDispatcher struct {
	delegate *notifier.ReportDispatcher
	reports  chan contract.AnalyticsReport
}

func (d *liveReportDispatcher) DispatchAnalyticsReport(
	subscriptionID string,
	report *contract.AnalyticsReport,
) error {
	if report != nil {
		select {
		case d.reports <- *report:
		default:
		}
	}
	return d.delegate.DispatchAnalyticsReport(subscriptionID, report)
}

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
	dispatcher := &liveReportDispatcher{
		delegate: notifier.NewReportDispatcher(context.Background()),
		reports:  make(chan contract.AnalyticsReport, 1),
	}
	processor := anlfprocessor.NewProcessor(&liveModelWorkflow{}, dispatcher)
	callbackServer, err := anlfserver.NewServer(serverConfig, processor)
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
	request := contract.ApplySubscriptionRuntimeRequest{
		Subscription: contract.SubscriptionRuntimeContext{
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
		RuntimeCompletionCallbackURI: serverConfig.GetAnlfServerURI() +
			"/subscriptions/" + subscriptionID + "/runtime-completions",
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

	binding := contract.ObservationBinding{
		ObservationSourceID: "live-source",
		Source:              contract.ObservationSource{SourceType: "SMF_UPF", Supi: "imsi-live"},
		CollectionProfile:   response.CollectionRequirements,
	}
	if err = client.SyncObservationBindings(
		context.Background(),
		subscriptionID,
		contract.SyncObservationBindingsRequest{
			RuntimeRevision: response.RuntimeRevision,
			Bindings:        []contract.ObservationBinding{binding},
		},
	); err != nil {
		t.Fatalf("SyncObservationBindings() error = %v", err)
	}

	if err = client.SendObservations(context.Background(), "live-source", contract.ObservationBatch{
		BatchID: "live-batch",
		Observations: []contract.SourceObservation{{
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
	select {
	case report := <-dispatcher.reports:
		if report.ReportID == "" || report.ReportSequence != 1 ||
			report.RuntimeRevision != response.RuntimeRevision {
			t.Fatalf("unexpected backend analytics report: %+v", report)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for captured backend analytics report")
	}
	deadline := time.Now().Add(5 * time.Second)
	completed := false
	for time.Now().Before(deadline) {
		_, _, _, active := subscription.RuntimeSnapshot()
		if !active {
			completed = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !completed {
		t.Fatal("timed out waiting for PyAnLF runtime completion callback")
	}
}

func TestLivePyAnLFExplicitReplacementAndReleaseDoNotComplete(t *testing.T) {
	endpoint := os.Getenv("PYANLF_LIVE_ENDPOINT")
	if endpoint == "" {
		t.Skip("PYANLF_LIVE_ENDPOINT is not set")
	}

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
	const subscriptionID = "live-explicit-release-subscription"
	subscription := &nwdaf_context.Subscription{
		ID:       subscriptionID,
		IsActive: true,
		EventSubs: []models.NwdafEventsSubscriptionEventSubscription{{
			Event: models.NwdafEvent_UE_COMMUNICATION,
		}},
	}
	nwdaf_context.GetSelf().AddSubscription(subscription)
	processor := anlfprocessor.NewProcessor(&liveModelWorkflow{})
	callbackServer, err := anlfserver.NewServer(serverConfig, processor)
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
	request := contract.ApplySubscriptionRuntimeRequest{
		Subscription: contract.SubscriptionRuntimeContext{
			SubscriptionID: subscriptionID,
			EvtReq: &models.ReportingInformation{
				NotifMethod:  models.SmfEventExposureNotificationMethod_PERIODIC,
				RepPeriod:    1,
				ImmRep:       false,
				MaxReportNbr: 1,
			},
			EventSubscriptions: subscription.EventSubs,
		},
		ReportCallbackURI: serverConfig.GetAnlfServerURI() +
			"/subscriptions/" + subscriptionID + "/analytics-reports",
		RuntimeCompletionCallbackURI: serverConfig.GetAnlfServerURI() +
			"/subscriptions/" + subscriptionID + "/runtime-completions",
	}
	first, err := client.ApplySubscriptionRuntime(context.Background(), request)
	if err != nil {
		t.Fatalf("first ApplySubscriptionRuntime() error = %v", err)
	}
	subscription.SetRuntime(first.RuntimeRevision, nwdaf_context.CollectionRequirements{}, nil)
	if err = client.SyncObservationBindings(
		context.Background(),
		subscriptionID,
		contract.SyncObservationBindingsRequest{
			RuntimeRevision: first.RuntimeRevision,
			Bindings:        []contract.ObservationBinding{},
		},
	); err != nil {
		t.Fatalf("first SyncObservationBindings() error = %v", err)
	}

	second, err := client.ApplySubscriptionRuntime(context.Background(), request)
	if err != nil {
		t.Fatalf("replacement ApplySubscriptionRuntime() error = %v", err)
	}
	if second.RuntimeRevision <= first.RuntimeRevision {
		t.Fatalf("replacement revision = %d, first = %d", second.RuntimeRevision, first.RuntimeRevision)
	}
	subscription.SetRuntime(second.RuntimeRevision, nwdaf_context.CollectionRequirements{}, nil)
	if err = client.SyncObservationBindings(
		context.Background(),
		subscriptionID,
		contract.SyncObservationBindingsRequest{
			RuntimeRevision: second.RuntimeRevision,
			Bindings:        []contract.ObservationBinding{},
		},
	); err != nil {
		t.Fatalf("replacement SyncObservationBindings() error = %v", err)
	}
	if err = client.ReleaseSubscriptionRuntime(context.Background(), subscriptionID); err != nil {
		t.Fatalf("ReleaseSubscriptionRuntime() error = %v", err)
	}

	time.Sleep(1500 * time.Millisecond)
	revision, _, _, active := subscription.RuntimeSnapshot()
	if revision != second.RuntimeRevision || !active {
		t.Fatalf("explicit lifecycle emitted completion: revision=%d active=%v", revision, active)
	}
}

func TestLivePyAnLFProvisionEventDedupNoMatch(t *testing.T) {
	endpoint := os.Getenv("PYANLF_LIVE_ENDPOINT")
	if endpoint == "" {
		t.Skip("PYANLF_LIVE_ENDPOINT is not set")
	}

	eventID := "live-no-match:" + time.Now().UTC().Format("20060102T150405.000000000")
	event := contract.ModelProvisionEvent{
		EventID: eventID,
		Source:  "LIVE_CONTRACT_TEST",
		ModelIdentity: contract.ModelIdentity{
			ProviderID:    "live-contract-test",
			ModelUniqueID: time.Now().UnixNano(),
		},
		Artifact:       contract.ModelArtifact{MLModelURL: "http://invalid.example/model"},
		AnalyticsEvent: string(models.NwdafEvent_UE_COMMUNICATION),
	}
	client := NewClient(endpoint)

	first, err := client.ApplyModelProvisionEvent(context.Background(), event)
	if err != nil {
		t.Fatalf("first ApplyModelProvisionEvent() error = %v", err)
	}
	second, err := client.ApplyModelProvisionEvent(context.Background(), event)
	if err != nil {
		t.Fatalf("duplicate ApplyModelProvisionEvent() error = %v", err)
	}
	if first.Status != "NO_MATCH" || *first != *second {
		t.Fatalf("duplicate responses differ: first=%+v second=%+v", first, second)
	}
}
