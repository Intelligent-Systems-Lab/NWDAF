package processor

import (
	"testing"
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/openapi/models"
)

// =============================================================================
// Setup Helper
// =============================================================================

func setupTest() (*Processor, *nwdaf_context.NWDAFContext) {
	nwdaf_context.Init()
	ctx := nwdaf_context.GetSelf()
	ctx.ClearUeData()
	return &Processor{}, ctx
}

// =============================================================================
// Basic Notification Processing Tests
// =============================================================================

func TestHandleUpfNotification_Basic(t *testing.T) {
	p, ctx := setupTest()

	supi := "imsi-208930000000001"
	ts := time.Now()

	notification := &UpfNotificationData{
		CorrelationId: "test-corr-001",
		NotificationItems: []UpfNotificationItem{
			{
				EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
				Supi:      supi,
				Dnn:       "internet",
				TimeStamp: ts,
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
		t.Fatalf("HandleUpfNotification() error = %v", err)
	}

	// Verify UE data was created
	data, ok := ctx.GetUeData(supi)
	if !ok {
		t.Fatal("UE data not created")
	}

	// Verify raw data point stored correctly
	if len(data.RawUpfData) != 1 {
		t.Fatalf("RawUpfData length = %v, want 1", len(data.RawUpfData))
	}

	dp := data.RawUpfData[0]
	if dp.UlVolume != 1024 {
		t.Errorf("UlVolume = %v, want 1024", dp.UlVolume)
	}
	if dp.DlVolume != 2048 {
		t.Errorf("DlVolume = %v, want 2048", dp.DlVolume)
	}
	if dp.UlThroughput != "10 Mbps" {
		t.Errorf("UlThroughput = %v, want '10 Mbps'", dp.UlThroughput)
	}
	if dp.DlThroughput != "50 Mbps" {
		t.Errorf("DlThroughput = %v, want '50 Mbps'", dp.DlThroughput)
	}
	if !dp.Timestamp.Equal(ts) {
		t.Errorf("Timestamp = %v, want %v", dp.Timestamp, ts)
	}

	// Verify metadata
	if data.Dnn != "internet" {
		t.Errorf("Dnn = %v, want 'internet'", data.Dnn)
	}
}

// =============================================================================
// Raw Data Accumulation Tests
// =============================================================================

func TestHandleUpfNotification_MultipleNotifications(t *testing.T) {
	p, ctx := setupTest()

	supi := "imsi-208930000000001"
	baseTime := time.Now()

	notifications := []struct {
		ulVol int64
		dlVol int64
		ts    time.Time
	}{
		{1000, 2000, baseTime},
		{500, 1000, baseTime.Add(10 * time.Second)},
		{2500, 5000, baseTime.Add(20 * time.Second)},
	}

	for _, n := range notifications {
		notif := &UpfNotificationData{
			NotificationItems: []UpfNotificationItem{
				{
					EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
					Supi:      supi,
					TimeStamp: n.ts,
					UserDataUsageMeasurements: []UserDataUsageMeasurements{
						{
							VolumeMeasurement: &VolumeMeasurement{
								UlVolume: n.ulVol,
								DlVolume: n.dlVol,
							},
						},
					},
				},
			},
		}
		p.HandleUpfNotification(notif)
	}

	data, _ := ctx.GetUeData(supi)

	// Verify all data points preserved
	if len(data.RawUpfData) != 3 {
		t.Fatalf("RawUpfData length = %v, want 3", len(data.RawUpfData))
	}

	// Verify each data point
	for i, n := range notifications {
		dp := data.RawUpfData[i]
		if dp.UlVolume != n.ulVol {
			t.Errorf("RawUpfData[%d].UlVolume = %v, want %v", i, dp.UlVolume, n.ulVol)
		}
		if dp.DlVolume != n.dlVol {
			t.Errorf("RawUpfData[%d].DlVolume = %v, want %v", i, dp.DlVolume, n.dlVol)
		}
		if !dp.Timestamp.Equal(n.ts) {
			t.Errorf("RawUpfData[%d].Timestamp mismatch", i)
		}
	}

	// Verify aggregated totals (for analytics verification)
	var totalUl, totalDl int64
	for _, dp := range data.RawUpfData {
		totalUl += dp.UlVolume
		totalDl += dp.DlVolume
	}
	if totalUl != 4000 {
		t.Errorf("Total UlVolume = %v, want 4000", totalUl)
	}
	if totalDl != 8000 {
		t.Errorf("Total DlVolume = %v, want 8000", totalDl)
	}
}

func TestHandleUpfNotification_MultipleMeasurementsInOneItem(t *testing.T) {
	p, ctx := setupTest()

	supi := "imsi-208930000000001"
	ts := time.Now()

	notification := &UpfNotificationData{
		NotificationItems: []UpfNotificationItem{
			{
				EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
				Supi:      supi,
				TimeStamp: ts,
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{VolumeMeasurement: &VolumeMeasurement{UlVolume: 100, DlVolume: 200}},
					{VolumeMeasurement: &VolumeMeasurement{UlVolume: 50, DlVolume: 100}},
					{VolumeMeasurement: &VolumeMeasurement{UlVolume: 25, DlVolume: 50}},
				},
			},
		},
	}

	p.HandleUpfNotification(notification)

	data, _ := ctx.GetUeData(supi)

	// Each measurement creates a separate data point
	if len(data.RawUpfData) != 3 {
		t.Fatalf("RawUpfData length = %v, want 3", len(data.RawUpfData))
	}

	expectedVolumes := []struct{ ul, dl int64 }{
		{100, 200},
		{50, 100},
		{25, 50},
	}

	for i, exp := range expectedVolumes {
		if data.RawUpfData[i].UlVolume != exp.ul {
			t.Errorf("RawUpfData[%d].UlVolume = %v, want %v", i, data.RawUpfData[i].UlVolume, exp.ul)
		}
		if data.RawUpfData[i].DlVolume != exp.dl {
			t.Errorf("RawUpfData[%d].DlVolume = %v, want %v", i, data.RawUpfData[i].DlVolume, exp.dl)
		}
	}
}

// =============================================================================
// Timestamp Tests
// =============================================================================

func TestHandleUpfNotification_TimestampPreservation(t *testing.T) {
	p, ctx := setupTest()

	supi := "imsi-208930000000001"

	timestamps := []time.Time{
		time.Date(2026, 1, 28, 10, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 28, 10, 15, 0, 0, time.UTC),
		time.Date(2026, 1, 28, 10, 30, 0, 0, time.UTC),
	}

	for _, ts := range timestamps {
		notif := &UpfNotificationData{
			NotificationItems: []UpfNotificationItem{
				{
					EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
					Supi:      supi,
					TimeStamp: ts,
					UserDataUsageMeasurements: []UserDataUsageMeasurements{
						{VolumeMeasurement: &VolumeMeasurement{UlVolume: 100}},
					},
				},
			},
		}
		p.HandleUpfNotification(notif)
	}

	data, _ := ctx.GetUeData(supi)

	for i, expected := range timestamps {
		if !data.RawUpfData[i].Timestamp.Equal(expected) {
			t.Errorf("Timestamp[%d] = %v, want %v", i, data.RawUpfData[i].Timestamp, expected)
		}
	}
}

func TestHandleUpfNotification_LastUpdateTracking(t *testing.T) {
	p, ctx := setupTest()

	supi := "imsi-208930000000001"
	ts1 := time.Date(2026, 1, 28, 10, 0, 0, 0, time.UTC)
	ts2 := time.Date(2026, 1, 28, 12, 0, 0, 0, time.UTC)

	// First notification
	p.HandleUpfNotification(&UpfNotificationData{
		NotificationItems: []UpfNotificationItem{
			{
				EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
				Supi:      supi,
				TimeStamp: ts1,
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{VolumeMeasurement: &VolumeMeasurement{UlVolume: 100}},
				},
			},
		},
	})

	data, _ := ctx.GetUeData(supi)
	if !data.LastUpdate.Equal(ts1) {
		t.Errorf("LastUpdate after first notif = %v, want %v", data.LastUpdate, ts1)
	}

	// Second notification
	p.HandleUpfNotification(&UpfNotificationData{
		NotificationItems: []UpfNotificationItem{
			{
				EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
				Supi:      supi,
				TimeStamp: ts2,
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{VolumeMeasurement: &VolumeMeasurement{UlVolume: 200}},
				},
			},
		},
	})

	data, _ = ctx.GetUeData(supi)
	if !data.LastUpdate.Equal(ts2) {
		t.Errorf("LastUpdate after second notif = %v, want %v", data.LastUpdate, ts2)
	}
}

// =============================================================================
// Metadata Tests
// =============================================================================

func TestHandleUpfNotification_SessionMetadata(t *testing.T) {
	tests := []struct {
		name    string
		dnn     string
		snssai  *models.Snssai
		ratType models.RatType
	}{
		{
			name:    "internet with NR",
			dnn:     "internet",
			snssai:  &models.Snssai{Sst: 1, Sd: "010203"},
			ratType: models.RatType_NR,
		},
		{
			name:    "iot with LTE",
			dnn:     "iot",
			snssai:  &models.Snssai{Sst: 2, Sd: "112233"},
			ratType: models.RatType_EUTRA,
		},
		{
			name:    "enterprise without snssai",
			dnn:     "enterprise",
			snssai:  nil,
			ratType: models.RatType_NR,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, ctx := setupTest()

			supi := "imsi-208930000000001"
			p.HandleUpfNotification(&UpfNotificationData{
				NotificationItems: []UpfNotificationItem{
					{
						EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
						Supi:      supi,
						Dnn:       tt.dnn,
						Snssai:    tt.snssai,
						RatType:   tt.ratType,
						TimeStamp: time.Now(),
						UserDataUsageMeasurements: []UserDataUsageMeasurements{
							{VolumeMeasurement: &VolumeMeasurement{UlVolume: 100}},
						},
					},
				},
			})

			data, _ := ctx.GetUeData(supi)

			if data.Dnn != tt.dnn {
				t.Errorf("Dnn = %v, want %v", data.Dnn, tt.dnn)
			}
			if tt.snssai != nil {
				if data.Snssai == nil || data.Snssai.Sst != tt.snssai.Sst {
					t.Errorf("Snssai mismatch")
				}
			}
			if data.RatType != tt.ratType {
				t.Errorf("RatType = %v, want %v", data.RatType, tt.ratType)
			}
		})
	}
}

// =============================================================================
// Multiple UE Tests
// =============================================================================

func TestHandleUpfNotification_MultipleUEs(t *testing.T) {
	p, ctx := setupTest()

	ts := time.Now()

	notification := &UpfNotificationData{
		NotificationItems: []UpfNotificationItem{
			{
				EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
				Supi:      "imsi-001",
				Dnn:       "internet",
				TimeStamp: ts,
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{VolumeMeasurement: &VolumeMeasurement{UlVolume: 100, DlVolume: 200}},
				},
			},
			{
				EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
				Supi:      "imsi-002",
				Dnn:       "iot",
				TimeStamp: ts,
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{VolumeMeasurement: &VolumeMeasurement{UlVolume: 300, DlVolume: 400}},
				},
			},
			{
				EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
				Supi:      "imsi-003",
				Dnn:       "voice",
				TimeStamp: ts,
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{VolumeMeasurement: &VolumeMeasurement{UlVolume: 500, DlVolume: 600}},
				},
			},
		},
	}

	p.HandleUpfNotification(notification)

	// Verify each UE has correct data
	testCases := []struct {
		supi  string
		dnn   string
		ulVol int64
		dlVol int64
	}{
		{"imsi-001", "internet", 100, 200},
		{"imsi-002", "iot", 300, 400},
		{"imsi-003", "voice", 500, 600},
	}

	for _, tc := range testCases {
		data, ok := ctx.GetUeData(tc.supi)
		if !ok {
			t.Errorf("UE %s not found", tc.supi)
			continue
		}
		if len(data.RawUpfData) != 1 {
			t.Errorf("UE %s: RawUpfData length = %v, want 1", tc.supi, len(data.RawUpfData))
			continue
		}
		if data.RawUpfData[0].UlVolume != tc.ulVol {
			t.Errorf("UE %s: UlVolume = %v, want %v", tc.supi, data.RawUpfData[0].UlVolume, tc.ulVol)
		}
		if data.Dnn != tc.dnn {
			t.Errorf("UE %s: Dnn = %v, want %v", tc.supi, data.Dnn, tc.dnn)
		}
	}
}

// =============================================================================
// Volume-Only and Throughput-Only Tests
// =============================================================================

func TestHandleUpfNotification_VolumeOnly(t *testing.T) {
	p, ctx := setupTest()

	supi := "imsi-208930000000001"
	p.HandleUpfNotification(&UpfNotificationData{
		NotificationItems: []UpfNotificationItem{
			{
				EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
				Supi:      supi,
				TimeStamp: time.Now(),
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{
						VolumeMeasurement: &VolumeMeasurement{UlVolume: 5000, DlVolume: 10000},
						// No ThroughputMeasurement
					},
				},
			},
		},
	})

	data, _ := ctx.GetUeData(supi)
	dp := data.RawUpfData[0]

	if dp.UlVolume != 5000 {
		t.Errorf("UlVolume = %v, want 5000", dp.UlVolume)
	}
	if dp.UlThroughput != "" {
		t.Errorf("UlThroughput should be empty, got %v", dp.UlThroughput)
	}
}

func TestHandleUpfNotification_ThroughputOnly(t *testing.T) {
	p, ctx := setupTest()

	supi := "imsi-208930000000001"
	p.HandleUpfNotification(&UpfNotificationData{
		NotificationItems: []UpfNotificationItem{
			{
				EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
				Supi:      supi,
				TimeStamp: time.Now(),
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
	})

	data, _ := ctx.GetUeData(supi)
	dp := data.RawUpfData[0]

	if dp.UlVolume != 0 {
		t.Errorf("UlVolume = %v, want 0", dp.UlVolume)
	}
	if dp.UlThroughput != "25 Mbps" {
		t.Errorf("UlThroughput = %v, want '25 Mbps'", dp.UlThroughput)
	}
}

// =============================================================================
// Edge Cases
// =============================================================================

func TestHandleUpfNotification_MissingSupi(t *testing.T) {
	p, ctx := setupTest()

	notification := &UpfNotificationData{
		NotificationItems: []UpfNotificationItem{
			{
				EventType:  UpfEventType_USER_DATA_USAGE_MEASURES,
				UeIpv4Addr: "10.0.0.1", // No SUPI
				TimeStamp:  time.Now(),
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{VolumeMeasurement: &VolumeMeasurement{UlVolume: 1024}},
				},
			},
		},
	}

	err := p.HandleUpfNotification(notification)
	if err != nil {
		t.Errorf("Should not error on missing SUPI, got %v", err)
	}

	// Nothing should be stored
	_, ok := ctx.GetUeData("")
	if ok {
		t.Error("Should not store data without SUPI")
	}
}

func TestHandleUpfNotification_EmptyNotification(t *testing.T) {
	p, _ := setupTest()

	notification := &UpfNotificationData{
		NotificationItems: []UpfNotificationItem{},
	}

	err := p.HandleUpfNotification(notification)
	if err != nil {
		t.Errorf("Should not error on empty notification, got %v", err)
	}
}

func TestHandleUpfNotification_EmptyMeasurements(t *testing.T) {
	p, ctx := setupTest()

	supi := "imsi-208930000000001"
	notification := &UpfNotificationData{
		NotificationItems: []UpfNotificationItem{
			{
				EventType:                 UpfEventType_USER_DATA_USAGE_MEASURES,
				Supi:                      supi,
				TimeStamp:                 time.Now(),
				UserDataUsageMeasurements: []UserDataUsageMeasurements{}, // Empty
			},
		},
	}

	p.HandleUpfNotification(notification)

	data, ok := ctx.GetUeData(supi)
	if !ok {
		t.Fatal("UE data should be created even with empty measurements")
	}
	if len(data.RawUpfData) != 0 {
		t.Errorf("RawUpfData should be empty, got %d items", len(data.RawUpfData))
	}
}

// =============================================================================
// Correlation ID Resolution Tests
// =============================================================================

func TestHandleUpfNotification_CorrelationIdResolution(t *testing.T) {
	p, ctx := setupTest()

	supi := "imsi-208930000000001"
	correlationId := "corr-123"

	// Register correlation ID
	ctx.StoreCorrelationToSupi(correlationId, supi)

	// Notification without SUPI but with correlationId
	notification := &UpfNotificationData{
		CorrelationId: correlationId,
		NotificationItems: []UpfNotificationItem{
			{
				EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
				// No Supi - should be resolved from correlationId
				TimeStamp: time.Now(),
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{VolumeMeasurement: &VolumeMeasurement{UlVolume: 1000}},
				},
			},
		},
	}

	p.HandleUpfNotification(notification)

	data, ok := ctx.GetUeData(supi)
	if !ok {
		t.Fatal("SUPI should be resolved from correlationId")
	}
	if len(data.RawUpfData) != 1 {
		t.Errorf("RawUpfData length = %v, want 1", len(data.RawUpfData))
	}
	if data.RawUpfData[0].UlVolume != 1000 {
		t.Errorf("UlVolume = %v, want 1000", data.RawUpfData[0].UlVolume)
	}
}
