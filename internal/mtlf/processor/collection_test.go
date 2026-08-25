package processor

import (
	"context"
	"testing"

	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/openapi/models"
)

type collectionProxyStub struct {
	smfTarget       string
	udmTarget       string
	adrfTarget      string
	smfOperation    string
	subscriptionID  string
	udmRegistration string
}

func (s *collectionProxyStub) CreateSmfEventExposure(
	_ context.Context,
	target string,
	_ []byte,
) (*consumer.StandardSmfResponse, error) {
	s.smfTarget = target
	s.smfOperation = "create"
	return &consumer.StandardSmfResponse{StatusCode: 201}, nil
}

func (s *collectionProxyStub) ReadSmfEventExposure(
	_ context.Context,
	target string,
	subscriptionID string,
) (*consumer.StandardSmfResponse, error) {
	s.smfTarget = target
	s.smfOperation = "read"
	s.subscriptionID = subscriptionID
	return &consumer.StandardSmfResponse{StatusCode: 200}, nil
}

func (s *collectionProxyStub) ReplaceSmfEventExposure(
	_ context.Context,
	target string,
	subscriptionID string,
	_ []byte,
) (*consumer.StandardSmfResponse, error) {
	s.smfTarget = target
	s.smfOperation = "replace"
	s.subscriptionID = subscriptionID
	return &consumer.StandardSmfResponse{StatusCode: 200}, nil
}

func (s *collectionProxyStub) DeleteSmfEventExposure(
	_ context.Context,
	target string,
	subscriptionID string,
) (*consumer.StandardSmfResponse, error) {
	s.smfTarget = target
	s.smfOperation = "delete"
	s.subscriptionID = subscriptionID
	return &consumer.StandardSmfResponse{StatusCode: 204}, nil
}

func (s *collectionProxyStub) GetUdmGroupIdentifiers(
	_ context.Context,
	target string,
	_ string,
	_ bool,
) (*consumer.StandardUdmResponse, error) {
	s.udmTarget = target
	return &consumer.StandardUdmResponse{StatusCode: 200}, nil
}

func (s *collectionProxyStub) GetUdmSmfRegistration(
	_ context.Context,
	target string,
	ueID string,
	_ *models.Snssai,
	_ string,
) (*consumer.StandardUdmResponse, error) {
	s.udmTarget = target
	s.udmRegistration = ueID
	return &consumer.StandardUdmResponse{StatusCode: 200}, nil
}

func (s *collectionProxyStub) StoreAdrfDataRecord(
	_ context.Context,
	target string,
	_ []byte,
) (*consumer.StandardAdrfResponse, error) {
	s.adrfTarget = target
	return &consumer.StandardAdrfResponse{StatusCode: 201}, nil
}

func TestCollectionProceduresDelegateToSharedConsumer(t *testing.T) {
	stub := &collectionProxyStub{}
	processor := New(nil, nil, nil, stub)

	if _, err := processor.CreateSmfEventExposure(
		t.Context(), "http://smf.example", []byte(`{}`),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := processor.ReadSmfEventExposure(
		t.Context(), "http://smf.example", "sub-a",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := processor.ReplaceSmfEventExposure(
		t.Context(), "http://smf.example", "sub-a", []byte(`{}`),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := processor.DeleteSmfEventExposure(
		t.Context(), "http://smf.example", "sub-a",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := processor.GetUdmGroupIdentifiers(
		t.Context(), "http://udm.example", "group-a", true,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := processor.GetUdmSmfRegistration(
		t.Context(), "http://udm.example", "imsi-1", nil, "internet",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := processor.StoreAdrfDataRecord(
		t.Context(), "http://adrf.example", []byte(`{}`),
	); err != nil {
		t.Fatal(err)
	}

	if stub.smfTarget != "http://smf.example" ||
		stub.smfOperation != "delete" || stub.subscriptionID != "sub-a" ||
		stub.udmTarget != "http://udm.example" ||
		stub.udmRegistration != "imsi-1" ||
		stub.adrfTarget != "http://adrf.example" {
		t.Fatalf("delegation targets: %+v", stub)
	}
}

func TestCollectionProceduresFailWhenConsumerIsUnavailable(t *testing.T) {
	processor := New(nil, nil, nil)
	if _, err := processor.CreateSmfEventExposure(
		t.Context(), "http://smf.example", nil,
	); err != ErrSmfEventExposureUnavailable {
		t.Fatalf("CreateSmfEventExposure() error = %v", err)
	}
	if _, err := processor.GetUdmGroupIdentifiers(
		t.Context(), "http://udm.example", "group-a", true,
	); err != ErrUdmCollectionUnavailable {
		t.Fatalf("GetUdmGroupIdentifiers() error = %v", err)
	}
	if _, err := processor.StoreAdrfDataRecord(t.Context(), "http://adrf.example", nil); err != ErrAdrfStorageUnavailable {
		t.Fatalf("StoreAdrfDataRecord() error = %v", err)
	}
}
