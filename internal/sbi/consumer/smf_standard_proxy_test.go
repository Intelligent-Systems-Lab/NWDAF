package consumer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/openapi/models"
)

func TestStandardSmfProxyPreservesBodyLocationAndCleanupOrdering(t *testing.T) {
	var deleteCalls atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodPost:
			response.Header().Set("Content-Type", "application/json; charset=utf-8")
			if request.URL.Path != SmfEventExposurePath {
				t.Errorf("create path = %q", request.URL.Path)
			}
			var body map[string]any
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Errorf("decode create body: %v", err)
			}
			if body["notifId"] != "corr-a" {
				t.Errorf("create body = %v", body)
			}
			response.Header().Set(
				"Location",
				server.URL+SmfEventExposurePath+"/smf-sub-a",
			)
			response.WriteHeader(http.StatusCreated)
			if err := json.NewEncoder(response).Encode(models.NsmfEventExposure{
				SubId:     "smf-sub-a",
				NotifId:   "corr-a",
				NotifUri:  "http://py.example/callbacks/upf-event-exposure",
				EventSubs: []models.SmfEventExposureEventSubscription{{Event: SmfEvent_UPF_EVENT}},
			}); err != nil {
				t.Errorf("encode create response: %v", err)
			}
		case http.MethodDelete:
			if request.URL.Path != SmfEventExposurePath+"/smf-sub-a" {
				t.Errorf("delete path = %q", request.URL.Path)
			}
			switch deleteCalls.Add(1) {
			case 1:
				response.WriteHeader(http.StatusInternalServerError)
				if err := json.NewEncoder(response).Encode(models.ProblemDetails{
					Status: http.StatusInternalServerError,
					Cause:  "SYSTEM_FAILURE",
				}); err != nil {
					t.Errorf("encode delete error response: %v", err)
				}
				return
			case 2:
				response.WriteHeader(http.StatusNotFound)
				return
			}
			response.WriteHeader(http.StatusNoContent)
		default:
			response.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	app := newTestConsumerApp(nil)
	client := newConsumerWithServices(app, NewNsmfService(), &testMtlfService{}, nil)
	body := []byte(
		`{"nfId":"nwdaf-a","notifId":"corr-a",` +
			`"notifUri":"http://py.example/callbacks/upf-event-exposure",` +
			`"eventSubs":[{"event":"UPF_EVENT",` +
			`"upfEvents":[{"type":"USER_DATA_USAGE_MEASURES"}]}]}`,
	)
	created, err := client.CreateSmfEventExposure(context.Background(), server.URL, body)
	if err != nil || created.StatusCode != http.StatusCreated || created.Location == "" {
		t.Fatalf("create response=%+v err=%v", created, err)
	}
	route, found := app.ctx.GetSmfPeerResourceRoute(server.URL, "smf-sub-a")
	if !found || route.ResourceLocation != server.URL+SmfEventExposurePath+"/smf-sub-a" ||
		route.CorrelationID != "corr-a" {
		t.Fatalf("peer route=%+v found=%v", route, found)
	}
	var accepted map[string]any
	if err = json.Unmarshal(route.AcceptedSubscriptionJSON, &accepted); err != nil {
		t.Fatalf("decode accepted subscription: %v", err)
	}
	if accepted["nfId"] != "nwdaf-a" || accepted["subId"] != "smf-sub-a" ||
		accepted["eventSubs"] == nil {
		t.Fatalf("accepted subscription = %v", accepted)
	}

	if _, err = client.DeleteSmfEventExposure(
		context.Background(), server.URL, "smf-sub-a",
	); err == nil {
		t.Fatal("first delete should preserve the peer failure")
	}
	route, found = app.ctx.GetSmfPeerResourceRoute(server.URL, "smf-sub-a")
	if !found || !route.PendingCleanup {
		t.Fatal("peer route was removed before SMF cleanup succeeded")
	}
	deleted, err := client.DeleteSmfEventExposure(
		context.Background(), server.URL, "smf-sub-a",
	)
	if err == nil || deleted.StatusCode != http.StatusNotFound {
		t.Fatalf("second delete response=%+v err=%v, want terminal 404", deleted, err)
	}
	if _, found = app.ctx.GetSmfPeerResourceRoute(server.URL, "smf-sub-a"); found {
		t.Fatal("peer route remains after SMF returned terminal 404")
	}

	if _, err = client.CreateSmfEventExposure(context.Background(), server.URL, body); err != nil {
		t.Fatalf("recreate response error = %v", err)
	}
	deleted, err = client.DeleteSmfEventExposure(
		context.Background(), server.URL, "smf-sub-a",
	)
	if err != nil || deleted.StatusCode != http.StatusNoContent {
		t.Fatalf("third delete response=%+v err=%v", deleted, err)
	}
	if _, found = app.ctx.GetSmfPeerResourceRoute(server.URL, "smf-sub-a"); found {
		t.Fatal("peer route remains after SMF returned 204")
	}
}

func TestStandardSmfProxyKeepsDuplicatePeerIDsIndependentByTarget(t *testing.T) {
	t.Parallel()

	newSMF := func() *httptest.Server {
		var server *httptest.Server
		server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			switch request.Method {
			case http.MethodPost:
				response.Header().Set("Content-Type", "application/json")
				response.Header().Set(
					"Location",
					server.URL+SmfEventExposurePath+"/shared-sub-id",
				)
				response.WriteHeader(http.StatusCreated)
				if err := json.NewEncoder(response).Encode(models.NsmfEventExposure{
					SubId:     "shared-sub-id",
					NotifId:   "corr-shared",
					NotifUri:  "http://py.example/callbacks/upf-event-exposure",
					EventSubs: []models.SmfEventExposureEventSubscription{{Event: SmfEvent_UPF_EVENT}},
				}); err != nil {
					t.Errorf("encode create response: %v", err)
				}
			case http.MethodDelete:
				response.WriteHeader(http.StatusNoContent)
			default:
				response.WriteHeader(http.StatusMethodNotAllowed)
			}
		}))
		return server
	}

	first := newSMF()
	defer first.Close()
	second := newSMF()
	defer second.Close()
	app := newTestConsumerApp(nil)
	client := newConsumerWithServices(app, NewNsmfService(), &testMtlfService{}, nil)
	body := []byte(
		`{"nfId":"nwdaf-a","notifId":"corr-shared",` +
			`"notifUri":"http://py.example/callbacks/upf-event-exposure",` +
			`"eventSubs":[{"event":"UPF_EVENT"}]}`,
	)

	if _, err := client.CreateSmfEventExposure(context.Background(), first.URL, body); err != nil {
		t.Fatalf("create first SMF resource: %v", err)
	}
	if _, err := client.CreateSmfEventExposure(context.Background(), second.URL, body); err != nil {
		t.Fatalf("create second SMF resource: %v", err)
	}
	if len(app.ctx.GetAllSmfPeerResourceRoutes()) != 2 {
		t.Fatalf("peer routes = %+v, want two target-aware resources", app.ctx.GetAllSmfPeerResourceRoutes())
	}

	if _, err := client.DeleteSmfEventExposure(
		context.Background(), first.URL, "shared-sub-id",
	); err != nil {
		t.Fatalf("delete first SMF resource: %v", err)
	}
	if _, found := app.ctx.GetSmfPeerResourceRoute(first.URL, "shared-sub-id"); found {
		t.Fatal("first target route remains after delete")
	}
	if _, found := app.ctx.GetSmfPeerResourceRoute(second.URL, "shared-sub-id"); !found {
		t.Fatal("second target route was removed with the first target")
	}
}

func TestStandardSmfProxyDoesNotFollowRedirects(t *testing.T) {
	for _, statusCode := range []int{http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		statusCode := statusCode
		t.Run(http.StatusText(statusCode), func(t *testing.T) {
			var calls atomic.Int32
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				calls.Add(1)
				switch request.Method {
				case http.MethodPost:
					response.Header().Set("Content-Type", "application/json")
					response.Header().Set("Location", server.URL+SmfEventExposurePath+"/peer-a")
					response.WriteHeader(http.StatusCreated)
					if err := json.NewEncoder(response).Encode(models.NsmfEventExposure{
						SubId: "peer-a", NotifId: "corr-a", NotifUri: "http://py/callback",
						EventSubs: []models.SmfEventExposureEventSubscription{{Event: SmfEvent_UPF_EVENT}},
					}); err != nil {
						t.Errorf("encode create response: %v", err)
					}
				case http.MethodGet, http.MethodPut, http.MethodDelete:
					response.Header().Set("Location", server.URL+SmfEventExposurePath+"/redirected")
					response.WriteHeader(statusCode)
					if err := json.NewEncoder(response).Encode(models.ProblemDetails{
						Status: int32(statusCode), Cause: "SYSTEM_FAILURE",
					}); err != nil {
						t.Errorf("encode redirect response: %v", err)
					}
				default:
					response.WriteHeader(http.StatusMethodNotAllowed)
				}
			}))
			defer server.Close()

			app := newTestConsumerApp(nil)
			client := newConsumerWithServices(app, NewNsmfService(), &testMtlfService{}, nil)
			body := []byte(`{"nfId":"nwdaf-a","notifId":"corr-a","notifUri":"http://py/callback",` +
				`"eventSubs":[{"event":"UPF_EVENT"}]}`)
			if _, err := client.CreateSmfEventExposure(context.Background(), server.URL, body); err != nil {
				t.Fatalf("create SMF resource: %v", err)
			}
			requests := []struct {
				name string
				call func() (*StandardSmfResponse, error)
			}{
				{
					name: "read",
					call: func() (*StandardSmfResponse, error) {
						return client.ReadSmfEventExposure(context.Background(), server.URL, "peer-a")
					},
				},
				{
					name: "replace",
					call: func() (*StandardSmfResponse, error) {
						return client.ReplaceSmfEventExposure(
							context.Background(), server.URL, "peer-a", body,
						)
					},
				},
				{
					name: "delete",
					call: func() (*StandardSmfResponse, error) {
						return client.DeleteSmfEventExposure(context.Background(), server.URL, "peer-a")
					},
				},
			}
			for _, request := range requests {
				response, err := request.call()
				if err == nil {
					t.Fatalf("%s redirect should be returned as a standard error", request.name)
				}
				if response == nil || response.StatusCode != statusCode ||
					response.Location != server.URL+SmfEventExposurePath+"/redirected" {
					t.Fatalf("%s response=%+v err=%v", request.name, response, err)
				}
			}
			if calls.Load() != 4 {
				t.Fatalf("SMF request count = %d, want 4 (create plus three redirected requests)", calls.Load())
			}
		})
	}
}

func TestStandardSmfProxyCompensatesMalformedCreateRepresentation(t *testing.T) {
	var deleteCalls atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodPost:
			response.Header().Set("Content-Type", "application/json")
			response.Header().Set("Location", server.URL+SmfEventExposurePath+"/peer-malformed")
			response.WriteHeader(http.StatusCreated)
			if _, err := response.Write([]byte("{")); err != nil {
				t.Errorf("write malformed response: %v", err)
			}
		case http.MethodDelete:
			deleteCalls.Add(1)
			response.WriteHeader(http.StatusNoContent)
		default:
			response.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	app := newTestConsumerApp(nil)
	client := newConsumerWithServices(app, NewNsmfService(), &testMtlfService{}, nil)
	body := []byte(`{"nfId":"nwdaf-a","notifId":"corr-a","notifUri":"http://py/callback",` +
		`"eventSubs":[{"event":"UPF_EVENT"}]}`)

	if _, err := client.CreateSmfEventExposure(context.Background(), server.URL, body); err == nil {
		t.Fatal("malformed SMF representation should fail")
	}
	if deleteCalls.Load() != 1 {
		t.Fatalf("compensating DELETE calls = %d, want 1", deleteCalls.Load())
	}
}

func TestStandardSmfProxyRetainsPendingRouteWhenCreateCompensationFails(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodPost:
			response.Header().Set("Content-Type", "application/json")
			response.Header().Set("Location", server.URL+SmfEventExposurePath+"/peer-pending")
			response.WriteHeader(http.StatusCreated)
			if _, err := response.Write([]byte("{")); err != nil {
				t.Errorf("write malformed response: %v", err)
			}
		case http.MethodDelete:
			response.WriteHeader(http.StatusServiceUnavailable)
		default:
			response.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	app := newTestConsumerApp(nil)
	client := newConsumerWithServices(app, NewNsmfService(), &testMtlfService{}, nil)
	body := []byte(`{"nfId":"nwdaf-a","notifId":"corr-pending","notifUri":"http://py/callback",` +
		`"eventSubs":[{"event":"UPF_EVENT"}]}`)

	if _, err := client.CreateSmfEventExposure(context.Background(), server.URL, body); err == nil {
		t.Fatal("malformed SMF representation should fail")
	}
	route, found := app.ctx.GetSmfPeerResourceRoute(server.URL, "peer-pending")
	if !found || !route.PendingCleanup || route.CorrelationID != "corr-pending" {
		t.Fatalf("pending route = %+v found=%v", route, found)
	}
}

func TestStandardSmfProxyRejectsMissingReadAndReplaceRepresentations(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodGet:
			response.WriteHeader(http.StatusOK)
		case http.MethodPut:
			response.Header().Set("Content-Type", "text/plain")
			response.WriteHeader(http.StatusOK)
			if _, err := response.Write([]byte(`{}`)); err != nil {
				t.Errorf("write replace response: %v", err)
			}
		default:
			response.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	app := newTestConsumerApp(nil)
	app.ctx.AddSmfPeerResourceRoute(&nwdaf_context.SmfPeerResourceRoute{
		SubscriptionID:   "peer-a",
		ResourceLocation: server.URL + SmfEventExposurePath + "/peer-a",
		TargetAPIBaseURI: server.URL,
	})
	client := newConsumerWithServices(app, NewNsmfService(), &testMtlfService{}, nil)
	if _, err := client.ReadSmfEventExposure(context.Background(), server.URL, "peer-a"); err == nil {
		t.Fatal("read without a JSON representation should fail")
	}
	body := []byte(`{"nfId":"nwdaf-a","notifId":"corr-a","notifUri":"http://py/callback",` +
		`"eventSubs":[{"event":"UPF_EVENT"}]}`)
	if _, err := client.ReplaceSmfEventExposure(
		context.Background(), server.URL, "peer-a", body,
	); err == nil {
		t.Fatal("replace with a non-JSON representation should fail")
	}
}
