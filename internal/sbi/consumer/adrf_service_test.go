package consumer

import (
	"bytes"
	"context"
	"encoding/json"
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

func TestAdrfClientExecuteStandardMLModelRecordMutationLifecycle(t *testing.T) {
	client := newInterceptedAdrfClient(t)
	storeTransID := "round-record-a"
	recordPath := AdrfMLModelStoreRecordsPath + "/" + storeTransID
	body := []byte(`{
		"nfInstanceId":"11111111-1111-4111-8111-111111111111",
		"mlModelInfo":[{
			"modelUniqueId":42,
			"mlFileAddr":{"mLModelUrl":"http://root.example/models/round-a"},
			"mlStorageSize":128,
			"allowConsumerList":[{"nfInstanceId":"22222222-2222-4222-8222-222222222222"}]
		}]
	}`)

	gock.New(testAdrfEndpoint).
		Put(recordPath).
		MatchHeader("Content-Type", "application/json").
		BodyString(string(body)).
		Reply(http.StatusOK).
		SetHeader("Content-Type", "application/json").
		BodyString(string(body))

	response, err := client.ExecuteStandardMLModelUpdateRequest(
		context.Background(),
		storeTransID,
		body,
	)
	if err != nil {
		t.Fatalf("ExecuteStandardMLModelUpdateRequest() error = %v", err)
	}
	if response.StatusCode != http.StatusOK || !bytes.Equal(response.Body, body) {
		t.Fatalf("update response = %+v", response)
	}

	gock.New(testAdrfEndpoint).
		Delete(recordPath).
		Reply(http.StatusNoContent)
	response, err = client.ExecuteStandardMLModelDeleteRequest(
		context.Background(),
		storeTransID,
	)
	if err != nil {
		t.Fatalf("ExecuteStandardMLModelDeleteRequest() error = %v", err)
	}
	if response.StatusCode != http.StatusNoContent || len(response.Body) != 0 {
		t.Fatalf("delete response = %+v", response)
	}
}

func TestAdrfClientExecuteStandardMLModelRecordMutationAcceptsDeclaredRepresentations(t *testing.T) {
	client := newInterceptedAdrfClient(t)
	storeTransID := "round-record-a"
	recordPath := AdrfMLModelStoreRecordsPath + "/" + storeTransID
	body := []byte(`{
		"nfInstanceId":"11111111-1111-4111-8111-111111111111",
		"mlModelInfo":[{
			"modelUniqueId":42,
			"mlFileAddr":{"mLModelUrl":"http://root.example/models/round-a"},
			"mlStorageSize":128
		}]
	}`)

	gock.New(testAdrfEndpoint).
		Put(recordPath).
		BodyString(string(body)).
		Reply(http.StatusNoContent)
	response, err := client.ExecuteStandardMLModelUpdateRequest(
		context.Background(),
		storeTransID,
		body,
	)
	if err != nil || response.StatusCode != http.StatusNoContent || len(response.Body) != 0 {
		t.Fatalf("update response=%+v error=%v", response, err)
	}

	deleteBody := []byte(`[{"modelUniqueId":42,"deleteResult":"ML_MODEL_DELETED"}]`)
	gock.New(testAdrfEndpoint).
		Delete(recordPath).
		Reply(http.StatusOK).
		SetHeader("Content-Type", "application/json").
		BodyString(string(deleteBody))
	response, err = client.ExecuteStandardMLModelDeleteRequest(
		context.Background(),
		storeTransID,
	)
	if err != nil || response.StatusCode != http.StatusOK || !bytes.Equal(response.Body, deleteBody) {
		t.Fatalf("delete response=%+v error=%v", response, err)
	}
}

func TestAdrfClientExecuteStandardMLModelRecordMutationRejectsMalformedSuccess(t *testing.T) {
	record := map[string]any{
		"nfInstanceId": "11111111-1111-4111-8111-111111111111",
		"mlModelInfo": []any{map[string]any{
			"modelUniqueId": int64(42),
			"mlFileAddr":    map[string]any{"mLModelUrl": "http://root.example/models/round-a"},
			"mlStorageSize": int64(128),
		}},
	}
	validBody, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name        string
		method      string
		status      int
		contentType string
		body        string
	}{
		{name: "update 200 missing content type", method: http.MethodPut, status: http.StatusOK, body: string(validBody)},
		{
			name: "update 200 malformed record", method: http.MethodPut,
			status: http.StatusOK, contentType: "application/json", body: `{}`,
		},
		{name: "update 204 with body", method: http.MethodPut, status: http.StatusNoContent, body: `{}`},
		{name: "delete 200 missing content type", method: http.MethodDelete, status: http.StatusOK, body: `[]`},
		{
			name: "delete 200 empty result", method: http.MethodDelete,
			status: http.StatusOK, contentType: "application/json", body: `[]`,
		},
		{
			name: "delete 200 malformed result", method: http.MethodDelete,
			status: http.StatusOK, contentType: "application/json", body: `[{}]`,
		},
		{name: "delete 204 with body", method: http.MethodDelete, status: http.StatusNoContent, body: `{}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := newInterceptedAdrfClient(t)
			request := gock.New(testAdrfEndpoint)
			if test.method == http.MethodPut {
				request.Put(AdrfMLModelStoreRecordsPath + "/round-record-a").BodyString(string(validBody))
			} else {
				request.Delete(AdrfMLModelStoreRecordsPath + "/round-record-a")
			}
			reply := request.Reply(test.status)
			if test.contentType != "" {
				reply.SetHeader("Content-Type", test.contentType)
			}
			reply.BodyString(test.body)

			var response *StandardAdrfResponse
			var callErr error
			if test.method == http.MethodPut {
				response, callErr = client.ExecuteStandardMLModelUpdateRequest(
					context.Background(),
					"round-record-a",
					validBody,
				)
			} else {
				response, callErr = client.ExecuteStandardMLModelDeleteRequest(
					context.Background(),
					"round-record-a",
				)
			}
			if callErr == nil || response != nil {
				t.Fatalf("response=%+v err=%v, want malformed success", response, callErr)
			}
		})
	}
}

func TestAdrfClientExecuteStandardMLModelRecordMutationPreservesProblemDetails(t *testing.T) {
	client := newInterceptedAdrfClient(t)
	problem := `{"status":404,"cause":"RESOURCE_NOT_FOUND"}`
	gock.New(testAdrfEndpoint).
		Delete(AdrfMLModelStoreRecordsPath+"/missing-record").
		Reply(http.StatusNotFound).
		SetHeader("Content-Type", "application/problem+json").
		BodyString(problem)

	response, err := client.ExecuteStandardMLModelDeleteRequest(
		context.Background(),
		"missing-record",
	)
	standardErr, ok := err.(*StandardAdrfError)
	if response == nil || !ok || response.StatusCode != http.StatusNotFound ||
		standardErr.ProblemDetails.Cause != "RESOURCE_NOT_FOUND" {
		t.Fatalf("response=%+v err=%#v", response, err)
	}
}

func TestAdrfClientExecuteStandardMLModelRecordMutationRejectsMissingTransactionID(t *testing.T) {
	client := newInterceptedAdrfClient(t)
	if response, err := client.ExecuteStandardMLModelUpdateRequest(
		context.Background(),
		" ",
		[]byte(`{}`),
	); err == nil || response != nil {
		t.Fatalf("update response=%+v error=%v", response, err)
	}
	if response, err := client.ExecuteStandardMLModelDeleteRequest(
		context.Background(),
		" ",
	); err == nil || response != nil {
		t.Fatalf("delete response=%+v error=%v", response, err)
	}
}
