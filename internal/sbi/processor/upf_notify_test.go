package processor

import (
	"context"
	"testing"
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/openapi/models"
)

const (
	testCorsId = "test-corr-001"
	testDnn    = "internet"
)

// mockNwdafApp implements NwdafApp for testing
type mockNwdafApp struct{}

func (m *mockNwdafApp) CancelContext() context.Context {
	return context.Background()
}

func (m *mockNwdafApp) Consumer() *consumer.Consumer {
	return nil
}

func setupTestContext() *nwdaf_context.NWDAFContext {
	nwdaf_context.Init()
	ctx := nwdaf_context.GetSelf()
	ctx.ClearTrafficDataStore()
	ctx.ClearSmfSubscriptions()
	return ctx
}

func newTestProcessor() *Processor {
	return NewProcessor(&mockNwdafApp{})
}

// =============================================================================
// Basic UPF Notification Tests
// =============================================================================

func TestHandleUpfNotification_Basic(t *testing.T) {
	ctx := setupTestContext()
	p := newTestProcessor()

	correlationId := testCorsId
	ts := time.Now()

	notif := &UpfNotificationData{
		CorrelationId: correlationId,
		NotificationItems: []UpfNotificationItem{
			{
				EventType:  UpfEventType_USER_DATA_USAGE_MEASURES,
				UeIpv4Addr: "192.168.1.1",
				TimeStamp:  ts,
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{
						VolumeMeasurement: &VolumeMeasurement{
							UlVolume: 1000,
							DlVolume: 2000,
						},
					},
				},
			},
		},
	}

	err := p.HandleUpfNotification(notif)
	if err != nil {
		t.Fatalf("HandleUpfNotification failed: %v", err)
	}

	// Verify bucket was created
	bucket := ctx.GetTrafficBucket(correlationId)
	if bucket == nil {
		t.Fatal("Traffic bucket should be created")
	}

	// Verify traffic data was stored
	data := bucket.Get("192.168.1.1")
	if data == nil {
		t.Fatal("Traffic data should be stored for IP")
	} else if len(data.RawUpfData) != 1 {
		t.Fatalf("RawUpfData length = %d, want 1", len(data.RawUpfData))
	}

	point := data.RawUpfData[0]
	if point.UlVolume != 1000 {
		t.Errorf("UlVolume = %d, want 1000", point.UlVolume)
	}
	if point.DlVolume != 2000 {
		t.Errorf("DlVolume = %d, want 2000", point.DlVolume)
	}
}

func TestHandleUpfNotification_MultipleItems(t *testing.T) {
	ctx := setupTestContext()
	p := newTestProcessor()

	correlationId := testCorsId

	notif := &UpfNotificationData{
		CorrelationId: correlationId,
		NotificationItems: []UpfNotificationItem{
			{
				EventType:  UpfEventType_USER_DATA_USAGE_MEASURES,
				UeIpv4Addr: "192.168.1.1",
				TimeStamp:  time.Now(),
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{VolumeMeasurement: &VolumeMeasurement{UlVolume: 100}},
				},
			},
			{
				EventType:  UpfEventType_USER_DATA_USAGE_MEASURES,
				UeIpv4Addr: "192.168.1.2",
				TimeStamp:  time.Now(),
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{VolumeMeasurement: &VolumeMeasurement{UlVolume: 200}},
				},
			},
		},
	}

	err := p.HandleUpfNotification(notif)
	if err != nil {
		t.Fatalf("HandleUpfNotification failed: %v", err)
	}

	bucket := ctx.GetTrafficBucket(correlationId)
	if bucket.Count() != 2 {
		t.Errorf("Bucket count = %d, want 2", bucket.Count())
	}

	// Verify both IPs have data
	if bucket.Get("192.168.1.1") == nil {
		t.Error("192.168.1.1 should have data")
	}
	if bucket.Get("192.168.1.2") == nil {
		t.Error("192.168.1.2 should have data")
	}
}

func TestHandleUpfNotification_DataAccumulation(t *testing.T) {
	ctx := setupTestContext()
	p := newTestProcessor()

	correlationId := testCorsId

	// First notification
	notif1 := &UpfNotificationData{
		CorrelationId: correlationId,
		NotificationItems: []UpfNotificationItem{
			{
				UeIpv4Addr: "192.168.1.1",
				TimeStamp:  time.Now(),
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{VolumeMeasurement: &VolumeMeasurement{UlVolume: 100}},
				},
			},
		},
	}
	if err := p.HandleUpfNotification(notif1); err != nil {
		t.Errorf("First notification failed: %v", err)
	}

	// Second notification
	notif2 := &UpfNotificationData{
		CorrelationId: correlationId,
		NotificationItems: []UpfNotificationItem{
			{
				UeIpv4Addr: "192.168.1.1",
				TimeStamp:  time.Now().Add(time.Second),
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{VolumeMeasurement: &VolumeMeasurement{UlVolume: 200}},
				},
			},
		},
	}
	if err := p.HandleUpfNotification(notif2); err != nil {
		t.Errorf("Second notification failed: %v", err)
	}

	bucket := ctx.GetTrafficBucket(correlationId)
	data := bucket.Get("192.168.1.1")

	// Should have 2 data points accumulated
	if len(data.RawUpfData) != 2 {
		t.Fatalf("RawUpfData length = %d, want 2", len(data.RawUpfData))
	}

	if data.RawUpfData[0].UlVolume != 100 {
		t.Errorf("First UlVolume = %d, want 100", data.RawUpfData[0].UlVolume)
	}
	if data.RawUpfData[1].UlVolume != 200 {
		t.Errorf("Second UlVolume = %d, want 200", data.RawUpfData[1].UlVolume)
	}
}

func TestHandleUpfNotification_MissingCorrelationId(t *testing.T) {
	setupTestContext()
	p := newTestProcessor()

	notif := &UpfNotificationData{
		CorrelationId: "", // Missing
		NotificationItems: []UpfNotificationItem{
			{
				UeIpv4Addr: "192.168.1.1",
				TimeStamp:  time.Now(),
			},
		},
	}

	// Should not error, just skip
	err := p.HandleUpfNotification(notif)
	if err != nil {
		t.Errorf("Should not error on missing correlationId: %v", err)
	}
}

func TestHandleUpfNotification_UpdateSmfSubscription(t *testing.T) {
	ctx := setupTestContext()
	p := newTestProcessor()

	correlationId := testCorsId

	// Create SmfSubscription first
	sub, _ := ctx.GetOrCreateSmfSubscription(correlationId, "nwdaf-sub-001")
	originalLastUpdate := sub.LastUpdate

	time.Sleep(10 * time.Millisecond)

	notif := &UpfNotificationData{
		CorrelationId: correlationId,
		NotificationItems: []UpfNotificationItem{
			{
				UeIpv4Addr: "192.168.1.1",
				TimeStamp:  time.Now(),
			},
		},
	}

	if err := p.HandleUpfNotification(notif); err != nil {
		t.Errorf("HandleUpfNotification failed: %v", err)
	}

	// LastUpdate should be updated
	if !sub.LastUpdate.After(originalLastUpdate) {
		t.Error("SmfSubscription.LastUpdate should be updated after notification")
	}
}

func TestHandleUpfNotification_WithMetadata(t *testing.T) {
	ctx := setupTestContext()
	p := newTestProcessor()

	correlationId := testCorsId
	snssai := &models.Snssai{Sst: 1, Sd: "010203"}

	notif := &UpfNotificationData{
		CorrelationId: correlationId,
		NotificationItems: []UpfNotificationItem{
			{
				UeIpv4Addr: "192.168.1.1",
				Dnn:        "internet",
				Snssai:     snssai,
				RatType:    models.RatType_NR,
				TimeStamp:  time.Now(),
				Supi:       "imsi-001",
			},
		},
	}

	if err := p.HandleUpfNotification(notif); err != nil {
		t.Errorf("HandleUpfNotification failed: %v", err)
	}

	bucket := ctx.GetTrafficBucket(correlationId)
	data := bucket.Get("192.168.1.1")

	if data.Dnn != testDnn {
		t.Errorf("Dnn = %q, want 'internet'", data.Dnn)
	}
	if data.Snssai == nil || data.Snssai.Sst != 1 {
		t.Errorf("Snssai not stored correctly")
	}
	if data.RatType != models.RatType_NR {
		t.Errorf("RatType = %v, want NR", data.RatType)
	}
	if data.Supi != "imsi-001" {
		t.Errorf("Supi = %q, want 'imsi-001'", data.Supi)
	}
}

func TestHandleUpfNotification_ThroughputMeasurement(t *testing.T) {
	ctx := setupTestContext()
	p := newTestProcessor()

	correlationId := testCorsId

	notif := &UpfNotificationData{
		CorrelationId: correlationId,
		NotificationItems: []UpfNotificationItem{
			{
				UeIpv4Addr: "192.168.1.1",
				TimeStamp:  time.Now(),
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{
						ThroughputMeasurement: &ThroughputMeasurement{
							UlThroughput: "1.5 Mbps",
							DlThroughput: "10.2 Mbps",
						},
					},
				},
			},
		},
	}

	if err := p.HandleUpfNotification(notif); err != nil {
		t.Errorf("HandleUpfNotification failed: %v", err)
	}

	bucket := ctx.GetTrafficBucket(correlationId)
	data := bucket.Get("192.168.1.1")

	if len(data.RawUpfData) != 1 {
		t.Fatalf("RawUpfData length = %d, want 1", len(data.RawUpfData))
	}

	point := data.RawUpfData[0]
	if point.UlThroughput != 1500000.0 {
		t.Errorf("UlThroughput = %f, want 1500000.0 (parsed from '1.5 Mbps')", point.UlThroughput)
	}
	if point.DlThroughput != 10200000.0 {
		t.Errorf("DlThroughput = %f, want 10200000.0 (parsed from '10.2 Mbps')", point.DlThroughput)
	}
}

func TestHandleUpfNotification_FullVolumeMeasurement(t *testing.T) {
	ctx := setupTestContext()
	p := newTestProcessor()

	correlationId := testCorsId
	ts := time.Now()

	notif := &UpfNotificationData{
		CorrelationId: correlationId,
		NotificationItems: []UpfNotificationItem{
			{
				EventType:  UpfEventType_USER_DATA_USAGE_MEASURES,
				UeIpv4Addr: "10.0.0.1",
				TimeStamp:  ts,
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{
						VolumeMeasurement: &VolumeMeasurement{
							TotalVolume:      3000,
							UlVolume:         1000,
							DlVolume:         2000,
							TotalNbOfPackets: 300,
							UlNbOfPackets:    100,
							DlNbOfPackets:    200,
						},
					},
				},
			},
		},
	}

	if err := p.HandleUpfNotification(notif); err != nil {
		t.Fatalf("HandleUpfNotification failed: %v", err)
	}

	bucket := ctx.GetTrafficBucket(correlationId)
	if bucket == nil {
		t.Fatal("Traffic bucket should be created")
	}
	data := bucket.Get("10.0.0.1")
	if data == nil {
		t.Fatal("Traffic data should be stored for IP")
	} else if len(data.RawUpfData) != 1 {
		t.Fatalf("RawUpfData length = %d, want 1", len(data.RawUpfData))
	}

	point := data.RawUpfData[0]
	if point.TotalVolume != 3000 {
		t.Errorf("TotalVolume = %d, want 3000", point.TotalVolume)
	}
	if point.UlVolume != 1000 {
		t.Errorf("UlVolume = %d, want 1000", point.UlVolume)
	}
	if point.DlVolume != 2000 {
		t.Errorf("DlVolume = %d, want 2000", point.DlVolume)
	}
	if point.TotalNbOfPackets != 300 {
		t.Errorf("TotalNbOfPackets = %d, want 300", point.TotalNbOfPackets)
	}
	if point.UlNbOfPackets != 100 {
		t.Errorf("UlNbOfPackets = %d, want 100", point.UlNbOfPackets)
	}
	if point.DlNbOfPackets != 200 {
		t.Errorf("DlNbOfPackets = %d, want 200", point.DlNbOfPackets)
	}
}

func TestHandleUpfNotification_PacketThroughput(t *testing.T) {
	ctx := setupTestContext()
	p := newTestProcessor()

	correlationId := testCorsId

	notif := &UpfNotificationData{
		CorrelationId: correlationId,
		NotificationItems: []UpfNotificationItem{
			{
				EventType:  UpfEventType_USER_DATA_USAGE_MEASURES,
				UeIpv4Addr: "10.0.0.2",
				TimeStamp:  time.Now(),
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{
						ThroughputMeasurement: &ThroughputMeasurement{
							UlThroughput:       "5 Mbps",
							DlThroughput:       "20 Mbps",
							UlPacketThroughput: "500 pps",
							DlPacketThroughput: "2000 pps",
						},
					},
				},
			},
		},
	}

	if err := p.HandleUpfNotification(notif); err != nil {
		t.Fatalf("HandleUpfNotification failed: %v", err)
	}

	bucket := ctx.GetTrafficBucket(correlationId)
	if bucket == nil {
		t.Fatal("Traffic bucket should be created")
	}
	data := bucket.Get("10.0.0.2")
	if data == nil {
		t.Fatal("Traffic data should be stored for IP")
	} else if len(data.RawUpfData) != 1 {
		t.Fatalf("RawUpfData length = %d, want 1", len(data.RawUpfData))
	}

	point := data.RawUpfData[0]
	if point.UlThroughput != 5000000.0 {
		t.Errorf("UlThroughput = %f, want 5000000.0 (parsed from '5 Mbps')", point.UlThroughput)
	}
	if point.DlThroughput != 20000000.0 {
		t.Errorf("DlThroughput = %f, want 20000000.0 (parsed from '20 Mbps')", point.DlThroughput)
	}
	if point.UlPacketThroughput != 500.0 {
		t.Errorf("UlPacketThroughput = %f, want 500.0 (parsed from '500 pps')", point.UlPacketThroughput)
	}
	if point.DlPacketThroughput != 2000.0 {
		t.Errorf("DlPacketThroughput = %f, want 2000.0 (parsed from '2000 pps')", point.DlPacketThroughput)
	}
}
