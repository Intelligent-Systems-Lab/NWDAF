package processor

import (
	"context"
	"net/http"
	"testing"

	"github.com/free5gc/nwdaf/internal/sbi/consumer"
)

type adrfRetrievalProxyStub struct {
	createTarget string
	createRoute  string
	deleteTarget string
	deleteRoute  string
}

func (s *adrfRetrievalProxyStub) CreateAdrfRetrievalSubscription(
	_ context.Context,
	target string,
	body []byte,
) (*consumer.StandardAdrfResponse, error) {
	s.createTarget = target
	location := s.createRoute
	if location == "" {
		location = target + consumer.AdrfDataRetrievalSubscriptionsPath + "/peer-sub-1"
	}
	return &consumer.StandardAdrfResponse{
		StatusCode:  http.StatusCreated,
		Location:    location,
		ContentType: "application/json",
		Body:        body,
	}, nil
}

func (s *adrfRetrievalProxyStub) DeleteAdrfRetrievalSubscription(
	_ context.Context,
	target string,
	location string,
) (*consumer.StandardAdrfResponse, error) {
	s.deleteTarget = target
	s.deleteRoute = location
	return &consumer.StandardAdrfResponse{StatusCode: http.StatusNoContent}, nil
}

func TestAdrfRetrievalRoutePreservesSelectedTargetAndPeerLocation(t *testing.T) {
	proxy := &adrfRetrievalProxyStub{}
	processor := NewProcessor(nil)
	processor.adrfRetrieval = proxy
	body := []byte(`{
		"notifCorrId":"11111111-1111-4111-8111-111111111111",
		"notificationURI":"http://nwdaf.example/collector/retrieval-notify",
		"timePeriod":{"startTime":"2026-07-24T00:00:00Z","stopTime":"2026-07-24T01:00:00Z"},
		"dataSub":{"smfDataSub":{"supi":"imsi-001"}},
		"consTrigNotif":true
	}`)

	response, err := processor.CreateAdrfRetrievalSubscription(
		context.Background(),
		"http://adrf.example",
		body,
	)
	if err != nil {
		t.Fatalf("CreateAdrfRetrievalSubscription() error = %v", err)
	}
	if response.StatusCode != http.StatusCreated || proxy.createTarget != "http://adrf.example" {
		t.Fatalf("create response=%+v target=%q", response, proxy.createTarget)
	}

	response, err = processor.DeleteAdrfRetrievalSubscription(
		context.Background(),
		"peer-sub-1",
	)
	if err != nil {
		t.Fatalf("DeleteAdrfRetrievalSubscription() error = %v", err)
	}
	if response.StatusCode != http.StatusNoContent ||
		proxy.deleteTarget != "http://adrf.example" ||
		proxy.deleteRoute != "http://adrf.example"+
			consumer.AdrfDataRetrievalSubscriptionsPath+"/peer-sub-1" {
		t.Fatalf(
			"delete response=%+v target=%q route=%q",
			response,
			proxy.deleteTarget,
			proxy.deleteRoute,
		)
	}
}

func TestAdrfRetrievalRouteRejectsCrossOriginPeerLocation(t *testing.T) {
	proxy := &adrfRetrievalProxyStub{
		createRoute: "http://attacker.example" +
			consumer.AdrfDataRetrievalSubscriptionsPath + "/peer-sub-1",
	}
	processor := NewProcessor(nil)
	processor.adrfRetrieval = proxy
	body := []byte(`{
		"notifCorrId":"11111111-1111-4111-8111-111111111111",
		"notificationURI":"http://nwdaf.example/collector/retrieval-notify",
		"timePeriod":{"startTime":"2026-07-24T00:00:00Z","stopTime":"2026-07-24T01:00:00Z"},
		"dataSub":{"smfDataSub":{"supi":"imsi-001"}},
		"consTrigNotif":true
	}`)

	_, err := processor.CreateAdrfRetrievalSubscription(
		context.Background(),
		"http://adrf.example",
		body,
	)

	if err == nil {
		t.Fatal("cross-origin ADRF retrieval Location was accepted")
	}
}

func TestAdrfRetrievalRouteResolvesRelativePeerLocation(t *testing.T) {
	proxy := &adrfRetrievalProxyStub{
		createRoute: consumer.AdrfDataRetrievalSubscriptionsPath + "/peer-sub-relative",
	}
	processor := NewProcessor(nil)
	processor.adrfRetrieval = proxy
	body := []byte(`{
		"notifCorrId":"11111111-1111-4111-8111-111111111111",
		"notificationURI":"http://nwdaf.example/collector/retrieval-notify",
		"timePeriod":{"startTime":"2026-07-24T00:00:00Z","stopTime":"2026-07-24T01:00:00Z"},
		"dataSub":{"smfDataSub":{"supi":"imsi-001"}},
		"consTrigNotif":true
	}`)

	_, err := processor.CreateAdrfRetrievalSubscription(
		context.Background(),
		"http://adrf.example",
		body,
	)
	if err != nil {
		t.Fatalf("relative Location create failed: %v", err)
	}
	_, err = processor.DeleteAdrfRetrievalSubscription(
		context.Background(),
		"peer-sub-relative",
	)
	if err != nil {
		t.Fatalf("relative Location delete failed: %v", err)
	}
	want := "http://adrf.example" +
		consumer.AdrfDataRetrievalSubscriptionsPath + "/peer-sub-relative"
	if proxy.deleteRoute != want {
		t.Fatalf("delete route = %q, want %q", proxy.deleteRoute, want)
	}
}
