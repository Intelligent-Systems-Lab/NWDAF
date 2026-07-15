package consumer

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/h2non/gock"
	"golang.org/x/oauth2"

	"github.com/free5gc/openapi"
)

const (
	testSmfEndpoint       = "http://127.0.0.10:8000"
	testSmfSubscriptionID = "sub-123"
)

func TestNsmfService_SubscribeToSmf(t *testing.T) {
	service := NewNsmfService()
	gock.InterceptClient(service.HTTPClient())
	defer gock.Off()
	defer gock.RestoreClient(service.HTTPClient())

	opts := SmfSubscriptionOptions{
		Supi:        "imsi-208930000000003",
		NotifUri:    "http://127.0.0.1:8080/collector/notify",
		NotifId:     "corr-123",
		EventSubs:   BuildUpfEventSubs("http://127.0.0.1:8080/collector/upf-notify", true, true),
		NotifMethod: "PERIODIC",
		RepPeriod:   10,
	}

	gock.New(testSmfEndpoint).
		Post(SmfEventExposurePath).
		MatchHeader("Content-Type", "application/json").
		JSON(ExtendedNsmfEventExposure{
			Supi:        opts.Supi,
			NotifUri:    opts.NotifUri,
			NotifId:     opts.NotifId,
			EventSubs:   opts.EventSubs,
			NotifMethod: opts.NotifMethod,
			RepPeriod:   opts.RepPeriod,
		}).
		Reply(http.StatusCreated).
		SetHeader("Location", SmfEventExposurePath+"/"+testSmfSubscriptionID)

	subscriptionID, err := service.SubscribeToSmf(context.Background(), testSmfEndpoint, opts)
	if err != nil {
		t.Fatalf("SubscribeToSmf returned error: %v", err)
	}
	if subscriptionID != testSmfSubscriptionID {
		t.Fatalf("SubscribeToSmf returned %q, want %q", subscriptionID, testSmfSubscriptionID)
	}
	if !gock.IsDone() {
		t.Fatal("expected SMF subscription request to match gock expectation")
	}
}

func TestNsmfService_SubscribeToSmfReturnsErrorOnFailureStatus(t *testing.T) {
	service := NewNsmfService()
	gock.InterceptClient(service.HTTPClient())
	defer gock.Off()
	defer gock.RestoreClient(service.HTTPClient())

	gock.New(testSmfEndpoint).
		Post(SmfEventExposurePath).
		Reply(http.StatusInternalServerError).
		BodyString("internal error")

	_, err := service.SubscribeToSmf(context.Background(), testSmfEndpoint, SmfSubscriptionOptions{
		Supi:      "imsi-208930000000003",
		NotifUri:  "http://127.0.0.1:8080/collector/notify",
		NotifId:   "corr-123",
		EventSubs: BuildUpfEventSubs("http://127.0.0.1:8080/collector/upf-notify", true, true),
	})
	if err == nil {
		t.Fatal("expected SubscribeToSmf to fail on non-success status")
	}
}

func TestNsmfService_UnsubscribeFromSmf(t *testing.T) {
	service := NewNsmfService()
	gock.InterceptClient(service.HTTPClient())
	defer gock.Off()
	defer gock.RestoreClient(service.HTTPClient())

	gock.New(testSmfEndpoint).
		Delete(SmfEventExposurePath + "/" + testSmfSubscriptionID).
		Reply(http.StatusNoContent)

	if err := service.UnsubscribeFromSmf(context.Background(), testSmfEndpoint, testSmfSubscriptionID); err != nil {
		t.Fatalf("UnsubscribeFromSmf returned error: %v", err)
	}
	if !gock.IsDone() {
		t.Fatal("expected SMF unsubscribe request to match gock expectation")
	}
}

func TestNsmfService_AttachesOAuthBearerToPostAndDelete(t *testing.T) {
	service := NewNsmfService()
	gock.InterceptClient(service.HTTPClient())
	defer gock.Off()
	defer gock.RestoreClient(service.HTTPClient())

	tokenCtx := context.WithValue(
		context.Background(),
		openapi.ContextOAuth2,
		oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "smf-token", TokenType: "Bearer"}),
	)
	gock.New(testSmfEndpoint).
		Post(SmfEventExposurePath).
		MatchHeader("Authorization", "Bearer smf-token").
		Reply(http.StatusCreated).
		SetHeader("Location", SmfEventExposurePath+"/"+testSmfSubscriptionID)
	gock.New(testSmfEndpoint).
		Delete(SmfEventExposurePath+"/"+testSmfSubscriptionID).
		MatchHeader("Authorization", "Bearer smf-token").
		Reply(http.StatusNoContent)

	if _, err := service.SubscribeToSmf(tokenCtx, testSmfEndpoint, SmfSubscriptionOptions{NotifId: "corr-1"}); err != nil {
		t.Fatalf("SubscribeToSmf() error = %v", err)
	}
	if err := service.UnsubscribeFromSmf(tokenCtx, testSmfEndpoint, testSmfSubscriptionID); err != nil {
		t.Fatalf("UnsubscribeFromSmf() error = %v", err)
	}
	if !gock.IsDone() {
		t.Fatalf("pending SMF OAuth requests: %v", gock.Pending())
	}
}

func TestNsmfService_OAuthDisabledDoesNotAttachBearer(t *testing.T) {
	service := NewNsmfService()
	var methods []string
	service.httpClient.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if got := req.Header.Get("Authorization"); got != "" {
			t.Fatalf("unexpected Authorization header = %q", got)
		}
		methods = append(methods, req.Method)
		header := make(http.Header)
		status := http.StatusNoContent
		if req.Method == http.MethodPost {
			status = http.StatusCreated
			header.Set("Location", SmfEventExposurePath+"/"+testSmfSubscriptionID)
		}
		return &http.Response{
			StatusCode: status,
			Header:     header,
			Body:       http.NoBody,
			Request:    req,
		}, nil
	})

	if _, err := service.SubscribeToSmf(context.Background(), testSmfEndpoint, SmfSubscriptionOptions{}); err != nil {
		t.Fatalf("SubscribeToSmf() error = %v", err)
	}
	if err := service.UnsubscribeFromSmf(context.Background(), testSmfEndpoint, testSmfSubscriptionID); err != nil {
		t.Fatalf("UnsubscribeFromSmf() error = %v", err)
	}
	if len(methods) != 2 || methods[0] != http.MethodPost || methods[1] != http.MethodDelete {
		t.Fatalf("SMF request methods = %v, want [POST DELETE]", methods)
	}
}

func TestNsmfService_TokenFailurePreventsDispatch(t *testing.T) {
	service := NewNsmfService()
	gock.InterceptClient(service.HTTPClient())
	defer gock.Off()
	defer gock.RestoreClient(service.HTTPClient())

	tokenErr := errors.New("token source unavailable")
	tokenCtx := context.WithValue(
		context.Background(),
		openapi.ContextOAuth2,
		tokenSourceFunc(func() (*oauth2.Token, error) { return nil, tokenErr }),
	)
	gock.New(testSmfEndpoint).
		Post(SmfEventExposurePath).
		Reply(http.StatusCreated)

	_, err := service.SubscribeToSmf(tokenCtx, testSmfEndpoint, SmfSubscriptionOptions{})
	if !errors.Is(err, tokenErr) {
		t.Fatalf("SubscribeToSmf() error = %v, want token source error", err)
	}
	if gock.IsDone() {
		t.Fatal("SMF request was dispatched after token failure")
	}
}

func TestNsmfService_DeleteTokenFailurePreventsDispatch(t *testing.T) {
	service := NewNsmfService()
	gock.InterceptClient(service.HTTPClient())
	defer gock.Off()
	defer gock.RestoreClient(service.HTTPClient())

	tokenErr := errors.New("delete token source unavailable")
	tokenCtx := context.WithValue(
		context.Background(),
		openapi.ContextOAuth2,
		tokenSourceFunc(func() (*oauth2.Token, error) { return nil, tokenErr }),
	)
	gock.New(testSmfEndpoint).
		Delete(SmfEventExposurePath + "/" + testSmfSubscriptionID).
		Reply(http.StatusNoContent)

	err := service.UnsubscribeFromSmf(tokenCtx, testSmfEndpoint, testSmfSubscriptionID)
	if !errors.Is(err, tokenErr) {
		t.Fatalf("UnsubscribeFromSmf() error = %v, want token source error", err)
	}
	if gock.IsDone() {
		t.Fatal("SMF DELETE was dispatched after token failure")
	}
}

func TestNsmfService_PreservesCallerCancellation(t *testing.T) {
	service := NewNsmfService()
	dispatched := false
	service.httpClient.Transport = roundTripperFunc(func(*http.Request) (*http.Response, error) {
		dispatched = true
		return nil, errors.New("request should not be dispatched")
	})
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	tokenCtx := context.WithValue(
		parent,
		openapi.ContextOAuth2,
		oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "unused-token", TokenType: "Bearer"}),
	)

	if _, err := service.SubscribeToSmf(
		tokenCtx,
		testSmfEndpoint,
		SmfSubscriptionOptions{},
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("SubscribeToSmf() error = %v, want context cancellation", err)
	}
	if err := service.UnsubscribeFromSmf(
		tokenCtx,
		testSmfEndpoint,
		testSmfSubscriptionID,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("UnsubscribeFromSmf() error = %v, want context cancellation", err)
	}
	if dispatched {
		t.Fatal("SMF request was dispatched after caller cancellation")
	}
}

type tokenSourceFunc func() (*oauth2.Token, error)

func (f tokenSourceFunc) Token() (*oauth2.Token, error) {
	return f()
}
