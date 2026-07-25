package consumer

import (
	"bytes"
	"context"
	"net/http"
	"testing"

	"github.com/h2non/gock"
)

const testAdrfEndpoint = "http://127.0.0.40:8000"

func newInterceptedAdrfClient(t *testing.T) *AdrfClient {
	t.Helper()
	client := NewAdrfClient(testAdrfEndpoint)
	gock.InterceptClient(client.HTTPClient())
	t.Cleanup(gock.Off)
	return client
}

func TestAdrfClientExecuteStandardStorageRequestPreservesRepresentationAndLocation(t *testing.T) {
	client := newInterceptedAdrfClient(t)
	body := []byte(
		`{"dataSub":[{"smfDataSub":{"notifId":"corr-a"}}],` +
			`"dataNotif":{"upfEventNotifs":[{"correlationId":"corr-a"}]}}`,
	)
	gock.New(testAdrfEndpoint).
		Post(AdrfDataStoreRecordsPath).
		BodyString(string(body)).
		Reply(http.StatusCreated).
		SetHeader("Location", testAdrfEndpoint+AdrfDataStoreRecordsPath+"/store-a").
		SetHeader("Content-Type", "application/json").
		BodyString(string(body))

	response, err := client.ExecuteStandardStorageRequest(context.Background(), body)
	if err != nil {
		t.Fatalf("ExecuteStandardStorageRequest() error = %v", err)
	}
	if response.StatusCode != http.StatusCreated ||
		response.Location != testAdrfEndpoint+AdrfDataStoreRecordsPath+"/store-a" ||
		!bytes.Equal(response.Body, body) {
		t.Fatalf("response = %+v", response)
	}
}

func TestAdrfClientExecuteStandardStorageRequestRejectsMalformedCreatedRepresentation(t *testing.T) {
	body := []byte(
		`{"dataSub":[{"smfDataSub":{"notifId":"corr-a"}}],` +
			`"dataNotif":{"upfEventNotifs":[{"correlationId":"corr-a"}]}}`,
	)
	for _, test := range []struct {
		name        string
		location    string
		contentType string
		response    string
	}{
		{name: "missing location", contentType: "application/json", response: string(body)},
		{
			name:     "missing content type",
			location: testAdrfEndpoint + AdrfDataStoreRecordsPath + "/store-a",
			response: string(body),
		},
		{
			name:        "malformed body",
			location:    testAdrfEndpoint + AdrfDataStoreRecordsPath + "/store-a",
			contentType: "application/json",
			response:    "{",
		},
		{
			name:        "missing notification",
			location:    testAdrfEndpoint + AdrfDataStoreRecordsPath + "/store-a",
			contentType: "application/json",
			response:    `{"dataSub":[{}]}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newInterceptedAdrfClient(t)
			request := gock.New(testAdrfEndpoint).
				Post(AdrfDataStoreRecordsPath).
				Reply(http.StatusCreated)
			if test.location != "" {
				request.SetHeader("Location", test.location)
			}
			if test.contentType != "" {
				request.SetHeader("Content-Type", test.contentType)
			}
			request.BodyString(test.response)

			response, err := client.ExecuteStandardStorageRequest(context.Background(), body)
			if err == nil || response != nil {
				t.Fatalf("response=%+v err=%v, want contract failure", response, err)
			}
		})
	}
}

func TestAdrfClientExecuteStandardRetrievalLifecycle(t *testing.T) {
	client := newInterceptedAdrfClient(t)
	body := []byte(
		`{"notifCorrId":"job-a","notificationURI":"http://nwdaf.example/collector/retrieval-notify",` +
			`"timePeriod":{"startTime":"2026-07-25T00:00:00Z","stopTime":"2026-07-25T00:10:00Z"},` +
			`"dataSub":{"smfDataSub":{"notifId":"corr-a"}},"consTrigNotif":true}`,
	)
	location := testAdrfEndpoint + AdrfDataRetrievalSubscriptionsPath + "/subscription-a"
	gock.New(testAdrfEndpoint).
		Post(AdrfDataRetrievalSubscriptionsPath).
		BodyString(string(body)).
		Reply(http.StatusCreated).
		SetHeader("Location", location).
		SetHeader("Content-Type", "application/json").
		BodyString(string(body))

	response, err := client.ExecuteStandardRetrievalSubscribe(context.Background(), body)
	if err != nil {
		t.Fatalf("ExecuteStandardRetrievalSubscribe() error = %v", err)
	}
	if response.Location != location || response.StatusCode != http.StatusCreated {
		t.Fatalf("create response = %+v", response)
	}

	gock.New(testAdrfEndpoint).
		Delete(AdrfDataRetrievalSubscriptionsPath + "/subscription-a").
		Reply(http.StatusNoContent)
	response, err = client.ExecuteStandardRetrievalUnsubscribe(context.Background(), location)
	if err != nil {
		t.Fatalf("ExecuteStandardRetrievalUnsubscribe() error = %v", err)
	}
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("delete response = %+v", response)
	}
}
