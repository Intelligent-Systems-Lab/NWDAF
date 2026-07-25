package processor

import (
	"context"
	"net/http"
	"testing"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

type subscriptionTestApp struct {
	ctx context.Context
	cfg *factory.Config
}

func (*subscriptionTestApp) SetLogEnable(bool)         {}
func (*subscriptionTestApp) SetLogLevel(string)        {}
func (*subscriptionTestApp) SetReportCaller(bool)      {}
func (*subscriptionTestApp) Start()                    {}
func (*subscriptionTestApp) Terminate()                {}
func (a *subscriptionTestApp) Config() *factory.Config { return a.cfg }
func (*subscriptionTestApp) Context() *nwdaf_context.NWDAFContext {
	return nwdaf_context.GetSelf()
}
func (a *subscriptionTestApp) CancelContext() context.Context { return a.ctx }

func setupTestContext() *nwdaf_context.NWDAFContext {
	nwdaf_context.Init()
	return nwdaf_context.GetSelf()
}

type eventsSubscriptionBackendStub struct {
	created  *models.NnwdafEventsSubscription
	replaced *models.NnwdafEventsSubscription
	deleted  string
	refresh  int
}

func (s *eventsSubscriptionBackendStub) CreateEventsSubscription(
	_ context.Context,
	subscription *models.NnwdafEventsSubscription,
) (*models.NnwdafEventsSubscription, string, error) {
	s.created = subscription
	response := *subscription
	return &response, "dc441d60-b2ee-4dbd-84d5-8aa61a25c955", nil
}

func (s *eventsSubscriptionBackendStub) ReplaceEventsSubscription(
	_ context.Context,
	_ string,
	subscription *models.NnwdafEventsSubscription,
) (*models.NnwdafEventsSubscription, error) {
	s.replaced = subscription
	response := *subscription
	return &response, nil
}

func (s *eventsSubscriptionBackendStub) DeleteEventsSubscription(
	_ context.Context,
	subscriptionID string,
) error {
	s.deleted = subscriptionID
	return nil
}

func (*eventsSubscriptionBackendStub) Usable() bool {
	return true
}

func (*eventsSubscriptionBackendStub) MarkUnavailable(string) {}

func (s *eventsSubscriptionBackendStub) Refresh() {
	s.refresh++
}

func TestBackendEventsSubscriptionRoutingPreservesExternalURI(t *testing.T) {
	ctx := setupTestContext()
	backend := &eventsSubscriptionBackendStub{}
	app := &subscriptionTestApp{
		ctx: context.Background(),
		cfg: &factory.Config{Configuration: &factory.Configuration{
			Anlf: &factory.AnlfConfig{Server: &factory.AuxiliaryServerConfig{
				BindingIPv4:  "127.0.0.1",
				RegisterIPv4: "10.1.2.3",
				Port:         8090,
			}},
		}},
	}
	processor := &Processor{
		nwdaf:              app,
		eventsBackend:      backend,
		eventsAvailability: backend,
	}
	request := &models.NnwdafEventsSubscription{
		NotificationURI: "http://consumer.example/notify",
		NotifCorrId:     "corr-a",
		EventSubscriptions: []models.NwdafEventsSubscriptionEventSubscription{{
			Event: models.NwdafEvent_UE_COMMUNICATION,
		}},
	}

	created, subscriptionID, problem := processor.HandleCreateSubscription(
		context.Background(),
		request,
	)

	if problem != nil {
		t.Fatalf("HandleCreateSubscription() problem = %+v", problem)
	}
	if created.NotificationURI != request.NotificationURI {
		t.Fatalf("response notificationURI = %q", created.NotificationURI)
	}
	wantInternalURI := "http://10.1.2.3:8090/internal/v1/events-subscription-notifications"
	if backend.created.NotificationURI != wantInternalURI {
		t.Fatalf("backend notificationURI = %q, want %q", backend.created.NotificationURI, wantInternalURI)
	}
	route, found := ctx.GetAnalyticsSubscriptionRoute(subscriptionID)
	if !found || route.ExternalNotificationURI != request.NotificationURI ||
		route.AcceptedSubscription.NotificationURI != wantInternalURI {
		t.Fatalf("route = %+v, found=%v", route, found)
	}

	replacement := *request
	replacement.NotificationURI = "http://consumer.example/replaced"
	replaced, problem := processor.HandleUpdateSubscription(
		context.Background(),
		subscriptionID,
		&replacement,
	)
	if problem != nil || replaced.NotificationURI != replacement.NotificationURI {
		t.Fatalf("replace response=%+v problem=%+v", replaced, problem)
	}
	if backend.replaced.NotificationURI != wantInternalURI {
		t.Fatalf("replace backend notificationURI = %q", backend.replaced.NotificationURI)
	}

	if problem = processor.HandleDeleteSubscription(subscriptionID); problem != nil {
		t.Fatalf("HandleDeleteSubscription() problem = %+v", problem)
	}
	if backend.deleted != subscriptionID {
		t.Fatalf("deleted subscription = %q", backend.deleted)
	}
	if _, found = ctx.GetAnalyticsSubscriptionRoute(subscriptionID); found {
		t.Fatal("route remains after successful delete")
	}
	if backend.refresh != 3 {
		t.Fatalf("sync refresh count = %d, want 3", backend.refresh)
	}
}

func TestBackendEventsSubscriptionRejectsUnknownRouteWithoutCallingBackend(t *testing.T) {
	setupTestContext()
	backend := &eventsSubscriptionBackendStub{}
	processor := &Processor{
		nwdaf:              &subscriptionTestApp{ctx: context.Background()},
		eventsBackend:      backend,
		eventsAvailability: backend,
	}

	response, problem := processor.HandleUpdateSubscription(
		context.Background(),
		"missing",
		&models.NnwdafEventsSubscription{},
	)

	if response != nil || problem == nil || problem.Status != http.StatusNotFound {
		t.Fatalf("response=%+v problem=%+v", response, problem)
	}
	if backend.replaced != nil {
		t.Fatal("backend replace was called for an unknown external route")
	}
}

func TestEventsSubscriptionRejectsWhenAnlfBackendIsNotConfigured(t *testing.T) {
	processor := &Processor{}

	created, subscriptionID, problem := processor.HandleCreateSubscription(
		context.Background(),
		&models.NnwdafEventsSubscription{},
	)

	if created != nil || subscriptionID != "" || problem == nil ||
		problem.Status != http.StatusServiceUnavailable {
		t.Fatalf("created=%+v id=%q problem=%+v", created, subscriptionID, problem)
	}
	if problem.Cause != "" {
		t.Fatalf("problem cause = %q, want empty standard ProblemDetails cause", problem.Cause)
	}
}
