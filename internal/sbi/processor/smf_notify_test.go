package processor

import (
	"testing"
	"time"

	"github.com/free5gc/openapi/models"
)

// =============================================================================
// SMF Notification Tests
// =============================================================================

func TestHandleSmfNotification_Basic(t *testing.T) {
	setupTestContext()
	p := newTestProcessor()

	ts := time.Now()
	notif := &models.NsmfEventExposureNotification{
		NotifId: "test-corr-001",
		EventNotifs: []models.SmfEventExposureEventNotification{
			{
				Event:     models.SmfEvent_PDU_SES_EST,
				Supi:      "imsi-001",
				PduSeId:   1,
				Dnn:       "internet",
				TimeStamp: &ts,
			},
		},
	}

	err := p.HandleSmfNotification(notif)
	if err != nil {
		t.Fatalf("HandleSmfNotification failed: %v", err)
	}
}

func TestHandleSmfNotification_EnrichTrafficData(t *testing.T) {
	ctx := setupTestContext()
	p := newTestProcessor()

	correlationId := testCorsId

	// Pre-create bucket with traffic data (simulating UPF notification came first)
	bucket := ctx.GetOrCreateTrafficBucket(correlationId)
	data := bucket.GetOrCreate("192.168.1.1")

	snssai := &models.Snssai{Sst: 1, Sd: "010203"}
	ts := time.Now()

	// SMF notification should enrich the data
	notif := &models.NsmfEventExposureNotification{
		NotifId: correlationId,
		EventNotifs: []models.SmfEventExposureEventNotification{
			{
				Event:     models.SmfEvent_PDU_SES_EST,
				Supi:      "imsi-001",
				PduSeId:   1,
				Dnn:       "internet",
				Snssai:    snssai,
				RatType:   models.RatType_NR,
				TimeStamp: &ts,
			},
		},
	}

	if err := p.HandleSmfNotification(notif); err != nil {
		t.Errorf("HandleSmfNotification failed: %v", err)
	}

	// Verify enrichment
	if data.Dnn != testDnn {
		t.Errorf("Dnn = %q, want 'internet'", data.Dnn)
	}
	if data.Snssai == nil || data.Snssai.Sst != 1 {
		t.Errorf("Snssai not enriched correctly")
	}
	if data.RatType != models.RatType_NR {
		t.Errorf("RatType = %v, want NR", data.RatType)
	}
	if data.Supi != "imsi-001" {
		t.Errorf("Supi = %q, want 'imsi-001'", data.Supi)
	}
}

func TestHandleSmfNotification_MultipleEvents(t *testing.T) {
	setupTestContext()
	p := newTestProcessor()

	ts := time.Now()
	notif := &models.NsmfEventExposureNotification{
		NotifId: "test-corr-001",
		EventNotifs: []models.SmfEventExposureEventNotification{
			{
				Event:     models.SmfEvent_PDU_SES_EST,
				Supi:      "imsi-001",
				PduSeId:   1,
				TimeStamp: &ts,
			},
			{
				Event:     models.SmfEvent_PDU_SES_REL,
				Supi:      "imsi-001",
				PduSeId:   1,
				TimeStamp: &ts,
			},
		},
	}

	err := p.HandleSmfNotification(notif)
	if err != nil {
		t.Fatalf("HandleSmfNotification with multiple events failed: %v", err)
	}
}

func TestHandleSmfNotification_MissingSupi(t *testing.T) {
	setupTestContext()
	p := newTestProcessor()

	notif := &models.NsmfEventExposureNotification{
		NotifId: "test-corr-001",
		EventNotifs: []models.SmfEventExposureEventNotification{
			{
				Event:   models.SmfEvent_PDU_SES_EST,
				Supi:    "", // Missing
				PduSeId: 1,
			},
		},
	}

	// Should not error, just skip the event
	err := p.HandleSmfNotification(notif)
	if err != nil {
		t.Errorf("Should not error on missing SUPI: %v", err)
	}
}

func TestHandleSmfNotification_NoMatchingBucket(t *testing.T) {
	ctx := setupTestContext()
	p := newTestProcessor()

	correlationId := testCorsId
	// Don't create bucket - simulating SMF notification before UPF

	ts := time.Now()
	notif := &models.NsmfEventExposureNotification{
		NotifId: correlationId,
		EventNotifs: []models.SmfEventExposureEventNotification{
			{
				Event:     models.SmfEvent_PDU_SES_EST,
				Supi:      "imsi-001",
				PduSeId:   1,
				Dnn:       "internet",
				TimeStamp: &ts,
			},
		},
	}

	// Should not error even without a bucket
	err := p.HandleSmfNotification(notif)
	if err != nil {
		t.Fatalf("HandleSmfNotification failed: %v", err)
	}

	// Bucket should NOT be created by SMF notification
	if ctx.GetTrafficBucket(correlationId) != nil {
		t.Error("SMF notification should not create traffic bucket")
	}
}

func TestHandleSmfNotification_EnrichMultipleTrafficData(t *testing.T) {
	ctx := setupTestContext()
	p := newTestProcessor()

	correlationId := testCorsId

	// Pre-create bucket with multiple IPs
	bucket := ctx.GetOrCreateTrafficBucket(correlationId)
	data1 := bucket.GetOrCreate("192.168.1.1")
	data2 := bucket.GetOrCreate("192.168.1.2")

	ts := time.Now()
	notif := &models.NsmfEventExposureNotification{
		NotifId: correlationId,
		EventNotifs: []models.SmfEventExposureEventNotification{
			{
				Event:     models.SmfEvent_PDU_SES_EST,
				Supi:      "imsi-001",
				Dnn:       "internet",
				TimeStamp: &ts,
			},
		},
	}

	if err := p.HandleSmfNotification(notif); err != nil {
		t.Errorf("HandleSmfNotification failed: %v", err)
	}

	// Both should be enriched
	if data1.Dnn != "internet" {
		t.Errorf("data1.Dnn = %q, want 'internet'", data1.Dnn)
	}
	if data2.Dnn != "internet" {
		t.Errorf("data2.Dnn = %q, want 'internet'", data2.Dnn)
	}
}
