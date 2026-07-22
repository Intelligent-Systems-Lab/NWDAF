package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/free5gc/openapi/models"
)

func TestEventsSubscriptionClientPreservesStandardMethodsAndResponses(t *testing.T) {
	const subscriptionID = "78e3d5f4-80b2-4c12-9cce-0477466f2e70"
	requests := make(chan string, 3)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requests <- request.Method + " " + request.URL.Path
		response.Header().Set("Content-Type", "application/json; charset=utf-8")
		switch request.Method {
		case http.MethodPost:
			response.Header().Set("Location", "/internal/v1/events-subscriptions/"+subscriptionID)
			response.WriteHeader(http.StatusCreated)
		case http.MethodPut:
			response.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			response.WriteHeader(http.StatusNoContent)
			return
		}
		if err := json.NewEncoder(response).Encode(models.NnwdafEventsSubscription{
			NotificationURI: "http://go/internal/v1/events-subscription-notifications",
		}); err != nil {
			t.Errorf("Encode() error = %v", err)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL)
	request := &models.NnwdafEventsSubscription{
		NotificationURI: "http://go/internal/v1/events-subscription-notifications",
	}
	created, id, err := client.CreateEventsSubscription(context.Background(), request)
	if err != nil || id != subscriptionID || created.NotificationURI != request.NotificationURI {
		t.Fatalf("create response=%+v id=%q err=%v", created, id, err)
	}
	replaced, err := client.ReplaceEventsSubscription(context.Background(), subscriptionID, request)
	if err != nil || replaced.NotificationURI != request.NotificationURI {
		t.Fatalf("replace response=%+v err=%v", replaced, err)
	}
	if err = client.DeleteEventsSubscription(context.Background(), subscriptionID); err != nil {
		t.Fatalf("delete error = %v", err)
	}

	want := []string{
		"POST /internal/v1/events-subscriptions",
		"PUT /internal/v1/events-subscriptions/" + subscriptionID,
		"DELETE /internal/v1/events-subscriptions/" + subscriptionID,
	}
	for _, expected := range want {
		if got := <-requests; got != expected {
			t.Fatalf("request = %q, want %q", got, expected)
		}
	}
}

func TestEventsSubscriptionClientRejectsMalformedSuccessContract(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contentType string
		body        string
	}{
		{name: "missing media type", body: `{}`},
		{name: "unsupported media type", contentType: "text/plain", body: `{}`},
		{name: "missing representation", contentType: "application/json"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				response.Header().Set("Location", "/internal/v1/events-subscriptions/78e3d5f4-80b2-4c12-9cce-0477466f2e70")
				if test.contentType != "" {
					response.Header().Set("Content-Type", test.contentType)
				}
				response.WriteHeader(http.StatusCreated)
				if _, err := response.Write([]byte(test.body)); err != nil {
					t.Errorf("write response: %v", err)
				}
			}))
			defer server.Close()

			if _, _, err := NewClient(server.URL).CreateEventsSubscription(
				context.Background(),
				&models.NnwdafEventsSubscription{},
			); err == nil {
				t.Fatal("malformed success response should fail")
			}
		})
	}
}

func TestEventsSubscriptionClientPreservesProblemDetails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/problem+json")
		response.WriteHeader(http.StatusServiceUnavailable)
		if err := json.NewEncoder(response).Encode(models.ProblemDetails{
			Status: http.StatusServiceUnavailable,
			Cause:  "ANALYTICS_RUNTIME_UNAVAILABLE",
			Detail: "runtime could not be prepared",
		}); err != nil {
			t.Errorf("encode ProblemDetails: %v", err)
		}
	}))
	defer server.Close()

	_, _, err := NewClient(server.URL).CreateEventsSubscription(
		context.Background(),
		&models.NnwdafEventsSubscription{},
	)
	var requestError *EventsSubscriptionError
	if !errors.As(err, &requestError) {
		t.Fatalf("error = %T %v", err, err)
	}
	problem := requestError.StandardProblemDetails()
	if problem.Status != http.StatusServiceUnavailable ||
		problem.Cause != "ANALYTICS_RUNTIME_UNAVAILABLE" ||
		problem.Detail != "runtime could not be prepared" {
		t.Fatalf("ProblemDetails = %+v", problem)
	}
}
