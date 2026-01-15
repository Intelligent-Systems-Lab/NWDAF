package processor

import (
	"testing"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/openapi/models"
)

// TestHandleUpfNotification tests basic UPF notification processing
func TestHandleUpfNotification(t *testing.T) {
	nwdaf_context.Init()
	ctx := nwdaf_context.GetSelf()
	ctx.ClearUeData()

	p := &Processor{}

	notification := &UpfNotificationData{
		CorrelationId: "upf-notif-001",
		NotificationItems: []UpfNotificationItem{
			{
				EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
				Supi:      "imsi-208930000000003",
				Dnn:       "internet",
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{
						VolumeMeasurement: &VolumeMeasurement{
							UlVolume: 1024,
							DlVolume: 2048,
						},
						ThroughputMeasurement: &ThroughputMeasurement{
							UlThroughput: "10 Mbps",
							DlThroughput: "50 Mbps",
						},
					},
				},
			},
		},
	}

	err := p.HandleUpfNotification(notification)
	if err != nil {
		t.Errorf("HandleUpfNotification() error = %v", err)
	}

	// Verify UE data was stored
	data, ok := ctx.GetUeData("imsi-208930000000003")
	if !ok {
		t.Fatal("HandleUpfNotification() did not store UE data")
	}
	if data.TotalUlVolume != 1024 {
		t.Errorf("TotalUlVolume = %v, want 1024", data.TotalUlVolume)
	}
	if data.TotalDlVolume != 2048 {
		t.Errorf("TotalDlVolume = %v, want 2048", data.TotalDlVolume)
	}
	if data.LastUlThroughput != "10 Mbps" {
		t.Errorf("LastUlThroughput = %v, want '10 Mbps'", data.LastUlThroughput)
	}
	if data.LastDlThroughput != "50 Mbps" {
		t.Errorf("LastDlThroughput = %v, want '50 Mbps'", data.LastDlThroughput)
	}
	if data.Dnn != "internet" {
		t.Errorf("Dnn = %v, want 'internet'", data.Dnn)
	}
}

// TestHandleUpfNotification_VolumeAggregation tests that volume is aggregated correctly across multiple notifications
func TestHandleUpfNotification_VolumeAggregation(t *testing.T) {
	nwdaf_context.Init()
	ctx := nwdaf_context.GetSelf()
	ctx.ClearUeData()

	p := &Processor{}
	supi := "imsi-208930000000003"

	// First notification - initial volume
	notif1 := &UpfNotificationData{
		CorrelationId: "upf-notif-001",
		NotificationItems: []UpfNotificationItem{
			{
				EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
				Supi:      supi,
				Dnn:       "internet",
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
	p.HandleUpfNotification(notif1)

	// Verify first notification
	data, _ := ctx.GetUeData(supi)
	if data.TotalUlVolume != 1000 || data.TotalDlVolume != 2000 {
		t.Errorf("First notification: UL=%v DL=%v, want UL=1000 DL=2000",
			data.TotalUlVolume, data.TotalDlVolume)
	}

	// Second notification - additional volume
	notif2 := &UpfNotificationData{
		CorrelationId: "upf-notif-002",
		NotificationItems: []UpfNotificationItem{
			{
				EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
				Supi:      supi,
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{
						VolumeMeasurement: &VolumeMeasurement{
							UlVolume: 500,
							DlVolume: 1000,
						},
					},
				},
			},
		},
	}
	p.HandleUpfNotification(notif2)

	// Verify aggregation
	data, _ = ctx.GetUeData(supi)
	if data.TotalUlVolume != 1500 {
		t.Errorf("Aggregated TotalUlVolume = %v, want 1500", data.TotalUlVolume)
	}
	if data.TotalDlVolume != 3000 {
		t.Errorf("Aggregated TotalDlVolume = %v, want 3000", data.TotalDlVolume)
	}

	// Third notification - even more volume
	notif3 := &UpfNotificationData{
		NotificationItems: []UpfNotificationItem{
			{
				EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
				Supi:      supi,
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{
						VolumeMeasurement: &VolumeMeasurement{
							UlVolume: 2500,
							DlVolume: 5000,
						},
					},
				},
			},
		},
	}
	p.HandleUpfNotification(notif3)

	// Verify total aggregation
	data, _ = ctx.GetUeData(supi)
	if data.TotalUlVolume != 4000 {
		t.Errorf("Final TotalUlVolume = %v, want 4000", data.TotalUlVolume)
	}
	if data.TotalDlVolume != 8000 {
		t.Errorf("Final TotalDlVolume = %v, want 8000", data.TotalDlVolume)
	}
}

// TestHandleUpfNotification_ThroughputUpdate tests throughput value updates
func TestHandleUpfNotification_ThroughputUpdate(t *testing.T) {
	nwdaf_context.Init()
	ctx := nwdaf_context.GetSelf()
	ctx.ClearUeData()

	p := &Processor{}
	supi := "imsi-208930000000003"

	// First throughput measurement
	notif1 := &UpfNotificationData{
		NotificationItems: []UpfNotificationItem{
			{
				EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
				Supi:      supi,
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{
						ThroughputMeasurement: &ThroughputMeasurement{
							UlThroughput: "5 Mbps",
							DlThroughput: "20 Mbps",
						},
					},
				},
			},
		},
	}
	p.HandleUpfNotification(notif1)

	data, _ := ctx.GetUeData(supi)
	if data.LastUlThroughput != "5 Mbps" {
		t.Errorf("LastUlThroughput = %v, want '5 Mbps'", data.LastUlThroughput)
	}

	// Updated throughput measurement - should overwrite
	notif2 := &UpfNotificationData{
		NotificationItems: []UpfNotificationItem{
			{
				EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
				Supi:      supi,
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{
						ThroughputMeasurement: &ThroughputMeasurement{
							UlThroughput: "15 Mbps",
							DlThroughput: "100 Mbps",
						},
					},
				},
			},
		},
	}
	p.HandleUpfNotification(notif2)

	data, _ = ctx.GetUeData(supi)
	if data.LastUlThroughput != "15 Mbps" {
		t.Errorf("Updated LastUlThroughput = %v, want '15 Mbps'", data.LastUlThroughput)
	}
	if data.LastDlThroughput != "100 Mbps" {
		t.Errorf("Updated LastDlThroughput = %v, want '100 Mbps'", data.LastDlThroughput)
	}
}

// TestHandleUpfNotification_MissingSupi tests handling of notification without SUPI
func TestHandleUpfNotification_MissingSupi(t *testing.T) {
	nwdaf_context.Init()
	ctx := nwdaf_context.GetSelf()
	ctx.ClearUeData()

	p := &Processor{}

	// Notification without SUPI (only IP address)
	notification := &UpfNotificationData{
		NotificationItems: []UpfNotificationItem{
			{
				EventType:  UpfEventType_USER_DATA_USAGE_MEASURES,
				UeIpv4Addr: "10.0.0.1", // No SUPI
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{
						VolumeMeasurement: &VolumeMeasurement{
							UlVolume: 1024,
							DlVolume: 2048,
						},
					},
				},
			},
		},
	}

	err := p.HandleUpfNotification(notification)
	if err != nil {
		t.Errorf("HandleUpfNotification() should not error on missing SUPI")
	}

	// No data should be stored without SUPI
	_, ok := ctx.GetUeData("")
	if ok {
		t.Error("Should not store data without SUPI")
	}
}

// TestHandleUpfNotification_MultipleUEs tests notifications for multiple UEs
func TestHandleUpfNotification_MultipleUEs(t *testing.T) {
	nwdaf_context.Init()
	ctx := nwdaf_context.GetSelf()
	ctx.ClearUeData()

	p := &Processor{}

	// Notification with multiple UEs
	notification := &UpfNotificationData{
		NotificationItems: []UpfNotificationItem{
			{
				EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
				Supi:      "imsi-001",
				Dnn:       "internet",
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{
						VolumeMeasurement: &VolumeMeasurement{UlVolume: 100, DlVolume: 200},
					},
				},
			},
			{
				EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
				Supi:      "imsi-002",
				Dnn:       "iot",
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{
						VolumeMeasurement: &VolumeMeasurement{UlVolume: 300, DlVolume: 400},
					},
				},
			},
			{
				EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
				Supi:      "imsi-003",
				Dnn:       "voice",
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{
						VolumeMeasurement: &VolumeMeasurement{UlVolume: 500, DlVolume: 600},
					},
				},
			},
		},
	}

	p.HandleUpfNotification(notification)

	// Verify all UEs have data
	ue1, ok := ctx.GetUeData("imsi-001")
	if !ok || ue1.TotalUlVolume != 100 || ue1.Dnn != "internet" {
		t.Error("imsi-001 data incorrect")
	}

	ue2, ok := ctx.GetUeData("imsi-002")
	if !ok || ue2.TotalUlVolume != 300 || ue2.Dnn != "iot" {
		t.Error("imsi-002 data incorrect")
	}

	ue3, ok := ctx.GetUeData("imsi-003")
	if !ok || ue3.TotalUlVolume != 500 || ue3.Dnn != "voice" {
		t.Error("imsi-003 data incorrect")
	}
}

// TestHandleUpfNotification_MultipleMeasurements tests multiple measurements in one item
func TestHandleUpfNotification_MultipleMeasurements(t *testing.T) {
	nwdaf_context.Init()
	ctx := nwdaf_context.GetSelf()
	ctx.ClearUeData()

	p := &Processor{}
	supi := "imsi-208930000000003"

	// Notification with multiple measurements in one item
	notification := &UpfNotificationData{
		NotificationItems: []UpfNotificationItem{
			{
				EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
				Supi:      supi,
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{
						VolumeMeasurement: &VolumeMeasurement{UlVolume: 100, DlVolume: 200},
					},
					{
						VolumeMeasurement: &VolumeMeasurement{UlVolume: 50, DlVolume: 100},
					},
					{
						VolumeMeasurement: &VolumeMeasurement{UlVolume: 25, DlVolume: 50},
					},
				},
			},
		},
	}

	p.HandleUpfNotification(notification)

	data, _ := ctx.GetUeData(supi)
	// All measurements should be aggregated
	if data.TotalUlVolume != 175 {
		t.Errorf("TotalUlVolume = %v, want 175 (100+50+25)", data.TotalUlVolume)
	}
	if data.TotalDlVolume != 350 {
		t.Errorf("TotalDlVolume = %v, want 350 (200+100+50)", data.TotalDlVolume)
	}
}

// TestHandleUpfNotification_SessionMetadata tests session metadata update
func TestHandleUpfNotification_SessionMetadata(t *testing.T) {
	nwdaf_context.Init()
	ctx := nwdaf_context.GetSelf()
	ctx.ClearUeData()

	p := &Processor{}
	supi := "imsi-208930000000003"

	notification := &UpfNotificationData{
		NotificationItems: []UpfNotificationItem{
			{
				EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
				Supi:      supi,
				Dnn:       "enterprise",
				Snssai:    &models.Snssai{Sst: 1, Sd: "010203"},
				RatType:   models.RatType_NR,
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{
						VolumeMeasurement: &VolumeMeasurement{UlVolume: 100, DlVolume: 200},
					},
				},
			},
		},
	}

	p.HandleUpfNotification(notification)

	data, _ := ctx.GetUeData(supi)
	if data.Dnn != "enterprise" {
		t.Errorf("Dnn = %v, want 'enterprise'", data.Dnn)
	}
	if data.Snssai == nil || data.Snssai.Sst != 1 {
		t.Error("Snssai not stored correctly")
	}
	if data.RatType != models.RatType_NR {
		t.Errorf("RatType = %v, want NR", data.RatType)
	}
}

// TestHandleUpfNotification_VolumeOnlyMeasurement tests volume-only measurement
func TestHandleUpfNotification_VolumeOnlyMeasurement(t *testing.T) {
	nwdaf_context.Init()
	ctx := nwdaf_context.GetSelf()
	ctx.ClearUeData()

	p := &Processor{}
	supi := "imsi-208930000000003"

	notification := &UpfNotificationData{
		NotificationItems: []UpfNotificationItem{
			{
				EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
				Supi:      supi,
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{
						VolumeMeasurement: &VolumeMeasurement{
							UlVolume: 5000,
							DlVolume: 10000,
						},
						// No ThroughputMeasurement
					},
				},
			},
		},
	}

	p.HandleUpfNotification(notification)

	data, ok := ctx.GetUeData(supi)
	if !ok {
		t.Fatal("UE data not stored")
	}
	if data.TotalUlVolume != 5000 {
		t.Errorf("TotalUlVolume = %v, want 5000", data.TotalUlVolume)
	}
	// Throughput should remain empty
	if data.LastUlThroughput != "" {
		t.Errorf("LastUlThroughput should be empty, got %v", data.LastUlThroughput)
	}
}

// TestHandleUpfNotification_ThroughputOnlyMeasurement tests throughput-only measurement
func TestHandleUpfNotification_ThroughputOnlyMeasurement(t *testing.T) {
	nwdaf_context.Init()
	ctx := nwdaf_context.GetSelf()
	ctx.ClearUeData()

	p := &Processor{}
	supi := "imsi-208930000000003"

	notification := &UpfNotificationData{
		NotificationItems: []UpfNotificationItem{
			{
				EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
				Supi:      supi,
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{
						// No VolumeMeasurement
						ThroughputMeasurement: &ThroughputMeasurement{
							UlThroughput: "25 Mbps",
							DlThroughput: "150 Mbps",
						},
					},
				},
			},
		},
	}

	p.HandleUpfNotification(notification)

	data, ok := ctx.GetUeData(supi)
	if !ok {
		t.Fatal("UE data not stored")
	}
	// Volume should be 0
	if data.TotalUlVolume != 0 {
		t.Errorf("TotalUlVolume = %v, want 0", data.TotalUlVolume)
	}
	// Throughput should be set
	if data.LastUlThroughput != "25 Mbps" {
		t.Errorf("LastUlThroughput = %v, want '25 Mbps'", data.LastUlThroughput)
	}
}

// TestHandleUpfNotification_EmptyNotification tests handling of empty notification
func TestHandleUpfNotification_EmptyNotification(t *testing.T) {
	p := &Processor{}

	notification := &UpfNotificationData{
		NotificationItems: []UpfNotificationItem{},
	}

	err := p.HandleUpfNotification(notification)
	if err != nil {
		t.Errorf("HandleUpfNotification() should not error on empty notification")
	}
}
