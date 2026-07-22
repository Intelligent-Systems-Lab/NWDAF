package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/h2non/gock"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
)

const testAdrfEndpoint = "http://127.0.0.50:8000"

func newInterceptedAdrfClient(t *testing.T) *AdrfClient {
	t.Helper()

	client := NewAdrfClient(testAdrfEndpoint)
	gock.InterceptClient(client.httpClient)
	t.Cleanup(func() {
		gock.Off()
		gock.RestoreClient(client.httpClient)
	})

	return client
}

func testAdrfSmfInfo() *nwdaf_context.AdrfSmfInfo {
	return &nwdaf_context.AdrfSmfInfo{
		Supi:        "imsi-208930000000003",
		NotifId:     "corr-123",
		NotifUri:    "http://127.0.0.1:8080/collector/notify",
		UpfNotifUri: "http://127.0.0.1:8080/collector/upf-notify",
		NotifMethod: "PERIODIC",
		RepPeriod:   10,
	}
}

func TestAdrfClient_StorageRequest(t *testing.T) {
	client := newInterceptedAdrfClient(t)
	info := testAdrfSmfInfo()
	upfEventNotifs := []json.RawMessage{
		json.RawMessage(`{"event":"usage-1"}`),
	}

	expected := NadrfDataStoreRecord{
		DataSub: []AdrfDataSubscription{
			{
				SmfDataSub: &ExtendedNsmfEventExposure{
					Supi:        info.Supi,
					NotifId:     info.NotifId,
					NotifUri:    info.NotifUri,
					NotifMethod: info.NotifMethod,
					RepPeriod:   info.RepPeriod,
					EventSubs:   BuildUpfEventSubs(info.UpfNotifUri, true, true),
				},
			},
		},
		DataNotif: &AdrfDataNotification{
			UpfEventNotifs: upfEventNotifs,
		},
	}

	gock.New(testAdrfEndpoint).
		Post(AdrfDataStoreRecordsPath).
		MatchHeader("Content-Type", "application/json").
		JSON(expected).
		Reply(http.StatusCreated).
		SetHeader("Location", AdrfDataStoreRecordsPath+"/store-123")

	storeTransID, err := client.StorageRequest(context.Background(), info, upfEventNotifs)
	if err != nil {
		t.Fatalf("StorageRequest returned error: %v", err)
	}
	if storeTransID != "store-123" {
		t.Fatalf("StorageRequest returned %q, want %q", storeTransID, "store-123")
	}
	if !gock.IsDone() {
		t.Fatal("expected ADRF storage request to match gock expectation")
	}
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
	if !gock.IsDone() {
		t.Fatal("expected standard ADRF storage request to match")
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

func TestAdrfClient_RetrievalSubscribe(t *testing.T) {
	client := newInterceptedAdrfClient(t)
	info := testAdrfSmfInfo()
	timePeriod := AdrfTimePeriod{
		StartTime: "2026-06-23T00:00:00Z",
		StopTime:  "2026-06-23T01:00:00Z",
	}

	expected := NadrfDataRetrievalSubscription{
		NotifCorrId:     "task-123",
		NotificationURI: "http://127.0.0.1:8080/adrf/callback",
		TimePeriod:      timePeriod,
		DataSub: AdrfDataSubscription{
			SmfDataSub: &ExtendedNsmfEventExposure{
				Supi:        info.Supi,
				NotifId:     info.NotifId,
				NotifUri:    info.NotifUri,
				NotifMethod: info.NotifMethod,
				RepPeriod:   info.RepPeriod,
				EventSubs:   BuildUpfEventSubs(info.UpfNotifUri, true, true),
			},
		},
		ConsTrigNotif: true,
	}

	gock.New(testAdrfEndpoint).
		Post(AdrfDataRetrievalSubscriptionsPath).
		MatchHeader("Content-Type", "application/json").
		JSON(expected).
		Reply(http.StatusCreated).
		SetHeader("Location", AdrfDataRetrievalSubscriptionsPath+"/sub-123")

	subscriptionID, err := client.RetrievalSubscribe(
		context.Background(),
		info,
		"task-123",
		"http://127.0.0.1:8080/adrf/callback",
		timePeriod,
	)
	if err != nil {
		t.Fatalf("RetrievalSubscribe returned error: %v", err)
	}
	if subscriptionID != "sub-123" {
		t.Fatalf("RetrievalSubscribe returned %q, want %q", subscriptionID, "sub-123")
	}
	if !gock.IsDone() {
		t.Fatal("expected ADRF retrieval subscription request to match gock expectation")
	}
}

func TestAdrfClient_RetrievalRequest(t *testing.T) {
	client := newInterceptedAdrfClient(t)

	expected := &NadrfDataStoreRecord{
		DataSub: []AdrfDataSubscription{
			{
				SmfDataSub: &ExtendedNsmfEventExposure{
					Supi: "imsi-208930000000003",
				},
			},
		},
	}

	gock.New(testAdrfEndpoint).
		Get(AdrfDataStoreRecordsPath).
		MatchParam("fetch-correlation-ids", "fetch-1,fetch-2").
		Reply(http.StatusOK).
		JSON(expected)

	record, err := client.RetrievalRequest(context.Background(), []string{"fetch-1", "fetch-2"})
	if err != nil {
		t.Fatalf("RetrievalRequest returned error: %v", err)
	}
	if record == nil {
		t.Fatal("RetrievalRequest returned nil record")
	}
	if record.DataSub[0].SmfDataSub.Supi != "imsi-208930000000003" {
		t.Fatalf("RetrievalRequest returned SUPI %q, want %q",
			record.DataSub[0].SmfDataSub.Supi, "imsi-208930000000003")
	}
	if !gock.IsDone() {
		t.Fatal("expected ADRF retrieval request to match gock expectation")
	}
}

func TestAdrfClient_RetrievalRequestReturnsNilOnNoContent(t *testing.T) {
	client := newInterceptedAdrfClient(t)

	gock.New(testAdrfEndpoint).
		Get(AdrfDataStoreRecordsPath).
		MatchParam("fetch-correlation-ids", "fetch-1").
		Reply(http.StatusNoContent)

	record, err := client.RetrievalRequest(context.Background(), []string{"fetch-1"})
	if err != nil {
		t.Fatalf("RetrievalRequest returned error: %v", err)
	}
	if record != nil {
		t.Fatal("RetrievalRequest should return nil record on 204 response")
	}
}

func TestAdrfClient_RetrievalUnsubscribe(t *testing.T) {
	client := newInterceptedAdrfClient(t)

	gock.New(testAdrfEndpoint).
		Delete(AdrfDataRetrievalSubscriptionsPath + "/sub-123").
		Reply(http.StatusNotFound)

	if err := client.RetrievalUnsubscribe(context.Background(), "sub-123"); err != nil {
		t.Fatalf("RetrievalUnsubscribe returned error: %v", err)
	}
	if !gock.IsDone() {
		t.Fatal("expected ADRF retrieval unsubscribe request to match gock expectation")
	}
}

func TestAdrfClient_RetrievalUnsubscribeRetriesServerError(t *testing.T) {
	client := newInterceptedAdrfClient(t)

	gock.New(testAdrfEndpoint).
		Delete(AdrfDataRetrievalSubscriptionsPath + "/sub-123").
		Times(1).
		Reply(http.StatusInternalServerError)
	gock.New(testAdrfEndpoint).
		Delete(AdrfDataRetrievalSubscriptionsPath + "/sub-123").
		Times(1).
		Reply(http.StatusNoContent)

	if err := client.RetrievalUnsubscribe(context.Background(), "sub-123"); err != nil {
		t.Fatalf("RetrievalUnsubscribe returned error after retry: %v", err)
	}
	if !gock.IsDone() {
		t.Fatal("expected ADRF retrieval unsubscribe retry requests to match gock expectations")
	}
}
