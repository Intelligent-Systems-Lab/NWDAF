package processor

import (
	"context"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/mtlf"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/nwdaf/pkg/mockapp"
	"github.com/free5gc/openapi/models"
)

type capturingObservationCoordinator struct {
	observations []contract.SourceObservation
}

func (*capturingObservationCoordinator) ApplyInitialSubscriptionRuntime(
	*nwdaf_context.Subscription,
) (*contract.ApplySubscriptionRuntimeResponse, error) {
	return nil, nil
}

func (*capturingObservationCoordinator) ReleaseSubscriptionRuntime(string) error { return nil }

func (*capturingObservationCoordinator) SyncCurrentObservationBindings(string) error { return nil }

func (*capturingObservationCoordinator) BuildProvisionNotificationURI() string { return "" }

func (*capturingObservationCoordinator) SyncModelProvisionBinding(
	string,
	contract.ModelProvisionBinding,
) error {
	return nil
}

func (c *capturingObservationCoordinator) EnqueueObservations(
	_ string,
	observations []contract.SourceObservation,
) bool {
	c.observations = append(c.observations, observations...)
	return true
}

const (
	testCorsId = "test-corr-001"
	testDnn    = "internet"
)

func setupTestContext() *nwdaf_context.NWDAFContext {
	nwdaf_context.Init()
	ctx := nwdaf_context.GetSelf()
	ctx.ClearTrafficDataStore()
	ctx.ClearSmfSubscriptions()
	return ctx
}

func newTestProcessor(t *testing.T) *Processor {
	return newTestProcessorWithConfig(t, nil)
}

func newTestProcessorWithConfig(t *testing.T, cfg *factory.Config) *Processor {
	t.Helper()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockApp := mockapp.NewMockApp(ctrl)
	mockApp.EXPECT().CancelContext().Return(context.Background()).AnyTimes()
	mockApp.EXPECT().Consumer().Return(nil).AnyTimes()
	mockApp.EXPECT().Config().Return(cfg).AnyTimes()

	anlfService := &capturingObservationCoordinator{}
	mtlfService := mtlf.NewMtlfService(mockApp, nil, nil)
	return NewProcessor(mockApp, anlfService, mtlfService)
}

// =============================================================================
// Basic UPF Notification Tests
// =============================================================================

func TestHandleUpfNotification_Basic(t *testing.T) {
	ctx := setupTestContext()
	p := newTestProcessor(t)

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

	// The Go-side bucket retains session metadata, while raw observations are
	// forwarded to the AnLF backend.
	data := bucket.Get("192.168.1.1")
	if data == nil {
		t.Fatal("Traffic data should be stored for IP")
	}

	got := p.anlf.(*capturingObservationCoordinator).observations
	if len(got) != 1 {
		t.Fatalf("forwarded observations = %d, want 1", len(got))
	}
	point := got[0]
	if point.UplinkVolume != 1000 {
		t.Errorf("UplinkVolume = %f, want 1000", point.UplinkVolume)
	}
	if point.DownlinkVolume != 2000 {
		t.Errorf("DownlinkVolume = %f, want 2000", point.DownlinkVolume)
	}
}

func TestHandleUpfNotification_MultipleItems(t *testing.T) {
	ctx := setupTestContext()
	p := newTestProcessor(t)

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
	p := newTestProcessor(t)

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

	if data := ctx.GetTrafficBucket(correlationId).Get("192.168.1.1"); data == nil {
		t.Fatal("Traffic data should retain session metadata")
	}
	got := p.anlf.(*capturingObservationCoordinator).observations
	if len(got) != 2 {
		t.Fatalf("forwarded observations = %d, want 2", len(got))
	}
	if got[0].UplinkVolume != 100 {
		t.Errorf("first uplink volume = %f, want 100", got[0].UplinkVolume)
	}
	if got[1].UplinkVolume != 200 {
		t.Errorf("second uplink volume = %f, want 200", got[1].UplinkVolume)
	}
}

func TestHandleUpfNotification_MissingCorrelationId(t *testing.T) {
	setupTestContext()
	p := newTestProcessor(t)

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
	p := newTestProcessor(t)

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
	p := newTestProcessor(t)

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
	p := newTestProcessor(t)

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

	if data := ctx.GetTrafficBucket(correlationId).Get("192.168.1.1"); data == nil {
		t.Fatal("Traffic data should retain session metadata")
	}
	got := p.anlf.(*capturingObservationCoordinator).observations
	if len(got) != 1 {
		t.Fatalf("forwarded observations = %d, want 1", len(got))
	}
	point := got[0]
	if point.UplinkThroughput != 1500000.0 {
		t.Errorf("UplinkThroughput = %f, want 1500000.0 (parsed from '1.5 Mbps')", point.UplinkThroughput)
	}
	if point.DownlinkThroughput != 10200000.0 {
		t.Errorf("DownlinkThroughput = %f, want 10200000.0 (parsed from '10.2 Mbps')", point.DownlinkThroughput)
	}
}

func TestHandleUpfNotification_FullVolumeMeasurement(t *testing.T) {
	ctx := setupTestContext()
	p := newTestProcessor(t)

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
	}

	got := p.anlf.(*capturingObservationCoordinator).observations
	if len(got) != 1 {
		t.Fatalf("forwarded observations = %d, want 1", len(got))
	}
	point := got[0]
	if point.TotalVolume != 3000 {
		t.Errorf("TotalVolume = %f, want 3000", point.TotalVolume)
	}
	if point.UplinkVolume != 1000 {
		t.Errorf("UplinkVolume = %f, want 1000", point.UplinkVolume)
	}
	if point.DownlinkVolume != 2000 {
		t.Errorf("DownlinkVolume = %f, want 2000", point.DownlinkVolume)
	}
	if point.TotalPacketCount != 300 {
		t.Errorf("TotalPacketCount = %f, want 300", point.TotalPacketCount)
	}
	if point.UplinkPacketCount != 100 {
		t.Errorf("UplinkPacketCount = %f, want 100", point.UplinkPacketCount)
	}
	if point.DownlinkPacketCount != 200 {
		t.Errorf("DownlinkPacketCount = %f, want 200", point.DownlinkPacketCount)
	}
}

func TestHandleUpfNotification_PacketThroughput(t *testing.T) {
	ctx := setupTestContext()
	p := newTestProcessor(t)

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
	}

	got := p.anlf.(*capturingObservationCoordinator).observations
	if len(got) != 1 {
		t.Fatalf("forwarded observations = %d, want 1", len(got))
	}
	point := got[0]
	if point.UplinkThroughput != 5000000.0 {
		t.Errorf("UplinkThroughput = %f, want 5000000.0 (parsed from '5 Mbps')", point.UplinkThroughput)
	}
	if point.DownlinkThroughput != 20000000.0 {
		t.Errorf("DownlinkThroughput = %f, want 20000000.0 (parsed from '20 Mbps')", point.DownlinkThroughput)
	}
	if point.UplinkPacketThroughput != 500.0 {
		t.Errorf("UplinkPacketThroughput = %f, want 500.0 (parsed from '500 pps')", point.UplinkPacketThroughput)
	}
	if point.DownlinkPacketThroughput != 2000.0 {
		t.Errorf("DownlinkPacketThroughput = %f, want 2000.0 (parsed from '2000 pps')", point.DownlinkPacketThroughput)
	}
}
