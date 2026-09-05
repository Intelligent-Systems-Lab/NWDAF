package processor

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/free5gc/nwdaf/internal/sbi/consumer"
)

const testAdrfMLModelTarget = "http://adrf.example"

type adrfRetrievalStub struct {
	createResponse *consumer.StandardAdrfResponse
	createErr      error
	deleteResponse *consumer.StandardAdrfResponse
	deleteErr      error
	deleteTarget   string
	deleteLocation string
	mlModelTarget  string
	storeTransID   string
	mlModelBody    []byte
}

func (s *adrfRetrievalStub) CreateAdrfRetrievalSubscription(
	context.Context,
	string,
	[]byte,
) (*consumer.StandardAdrfResponse, error) {
	return s.createResponse, s.createErr
}

func (s *adrfRetrievalStub) DeleteAdrfRetrievalSubscription(
	_ context.Context,
	target string,
	location string,
) (*consumer.StandardAdrfResponse, error) {
	s.deleteTarget = target
	s.deleteLocation = location
	return s.deleteResponse, s.deleteErr
}

func (s *adrfRetrievalStub) StoreAdrfMLModelRecord(
	context.Context,
	string,
	[]byte,
) (*consumer.StandardAdrfResponse, error) {
	return nil, nil
}

func (s *adrfRetrievalStub) RetrieveAdrfMLModelRecord(
	context.Context,
	string,
	string,
	[]int64,
) (*consumer.StandardAdrfResponse, error) {
	return nil, nil
}

func (s *adrfRetrievalStub) UpdateAdrfMLModelRecord(
	_ context.Context,
	target string,
	storeTransID string,
	body []byte,
) (*consumer.StandardAdrfResponse, error) {
	s.mlModelTarget = target
	s.storeTransID = storeTransID
	s.mlModelBody = append([]byte(nil), body...)
	return s.createResponse, s.createErr
}

func (s *adrfRetrievalStub) DeleteAdrfMLModelRecord(
	_ context.Context,
	target string,
	storeTransID string,
) (*consumer.StandardAdrfResponse, error) {
	s.mlModelTarget = target
	s.storeTransID = storeTransID
	return s.deleteResponse, s.deleteErr
}

func TestAdrfMLModelMutationDelegatesToConsumer(t *testing.T) {
	body := []byte(`{"nfInstanceId":"root-a","mlModelInfo":[]}`)
	stub := &adrfRetrievalStub{
		createResponse: &consumer.StandardAdrfResponse{StatusCode: http.StatusNoContent},
		deleteResponse: &consumer.StandardAdrfResponse{StatusCode: http.StatusNoContent},
	}
	processor := New(nil, nil, stub)

	response, err := processor.UpdateAdrfMLModelRecord(
		context.Background(),
		testAdrfMLModelTarget,
		"round-record-a",
		body,
	)
	if err != nil || response == nil ||
		stub.mlModelTarget != testAdrfMLModelTarget ||
		stub.storeTransID != "round-record-a" ||
		!bytes.Equal(stub.mlModelBody, body) {
		t.Fatalf(
			"response=%+v error=%v target=%q storeTransId=%q body=%s",
			response,
			err,
			stub.mlModelTarget,
			stub.storeTransID,
			string(stub.mlModelBody),
		)
	}

	response, err = processor.DeleteAdrfMLModelRecord(
		context.Background(),
		testAdrfMLModelTarget,
		"round-record-a",
	)
	if err != nil || response == nil ||
		stub.mlModelTarget != testAdrfMLModelTarget ||
		stub.storeTransID != "round-record-a" {
		t.Fatalf(
			"response=%+v error=%v target=%q storeTransId=%q",
			response,
			err,
			stub.mlModelTarget,
			stub.storeTransID,
		)
	}
}

func TestAdrfRetrievalLifecycleUsesCapturedLocation(t *testing.T) {
	stub := &adrfRetrievalStub{
		createResponse: &consumer.StandardAdrfResponse{
			StatusCode: http.StatusCreated,
			Location: "http://adrf.example/nadrf-datamanagement/v1/" +
				"data-retrieval-subscriptions/retrieval-a",
		},
		deleteResponse: &consumer.StandardAdrfResponse{StatusCode: http.StatusNoContent},
	}
	processor := New(nil, nil, stub)
	body := []byte(`{
		"notifCorrId":"corr-a",
		"notificationURI":"http://nwdaf.example/collector/retrieval-notify",
		"timePeriod":{"startTime":"2026-01-01T00:00:00Z","stopTime":"2026-01-01T00:05:00Z"},
		"dataSub":{"smfDataSub":{"notifId":"corr-a","notifUri":"http://anlf.example/callback","eventSubs":[]}},
		"consTrigNotif":true
	}`)

	response, err := processor.CreateAdrfRetrievalSubscription(
		context.Background(),
		"http://adrf.example",
		body,
	)
	if err != nil || response == nil {
		t.Fatalf("create response=%v error=%v", response, err)
	}
	response, err = processor.DeleteAdrfRetrievalSubscription(
		context.Background(),
		"retrieval-a",
	)
	if err != nil || response == nil ||
		stub.deleteTarget != "http://adrf.example" ||
		stub.deleteLocation != stub.createResponse.Location {
		t.Fatalf(
			"delete response=%v error=%v target=%q location=%q",
			response,
			err,
			stub.deleteTarget,
			stub.deleteLocation,
		)
	}
	if _, err = processor.DeleteAdrfRetrievalSubscription(
		context.Background(),
		"retrieval-a",
	); !errors.Is(err, ErrAdrfRetrievalRouteNotFound) {
		t.Fatalf("second delete error=%v", err)
	}
}

func TestAdrfRetrievalRejectsCrossOriginLocation(t *testing.T) {
	stub := &adrfRetrievalStub{createResponse: &consumer.StandardAdrfResponse{
		StatusCode: http.StatusCreated,
		Location: "http://other.example/nadrf-datamanagement/v1/" +
			"data-retrieval-subscriptions/retrieval-a",
	}}
	processor := New(nil, nil, stub)

	_, err := processor.CreateAdrfRetrievalSubscription(
		context.Background(),
		"http://adrf.example",
		[]byte(`{"notifCorrId":"corr-a"}`),
	)
	if err == nil {
		t.Fatal("cross-origin ADRF Location was accepted")
	}
}
