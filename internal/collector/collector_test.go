package collector

import (
	"testing"
	"time"

	"github.com/free5gc/openapi/models"
)

func TestCollectorContext_StoreAndRetrieveSubscription(t *testing.T) {
	ctx := GetSelf()
	ctx.ClearSubscriptions() // Clean up before test

	sub := &SmfSubscription{
		SubscriptionId: "sub-123",
		SmfEndpoint:    "http://smf:29502",
		TargetSupi:     "imsi-123456789012345",
		NotifId:        "notif-001",
		Events:         []models.SmfEvent{models.SmfEvent_PDU_SES_EST},
		CreatedAt:      time.Now(),
	}

	// Store
	ctx.StoreSubscription(sub)

	// Retrieve
	retrieved, ok := ctx.GetSubscription("sub-123")
	if !ok {
		t.Fatal("Expected subscription to be found")
	}
	if retrieved.SubscriptionId != "sub-123" {
		t.Errorf("Expected subscriptionId 'sub-123', got '%s'", retrieved.SubscriptionId)
	}
	if retrieved.TargetSupi != "imsi-123456789012345" {
		t.Errorf("Expected supi 'imsi-123456789012345', got '%s'", retrieved.TargetSupi)
	}

	// Delete
	ctx.DeleteSubscription("sub-123")
	_, ok = ctx.GetSubscription("sub-123")
	if ok {
		t.Error("Expected subscription to be deleted")
	}
}

func TestCollectorContext_StoreAndRetrieveUeData(t *testing.T) {
	ctx := GetSelf()
	ctx.ClearUeData() // Clean up before test

	data := &UeCommunicationData{
		Supi:          "imsi-123456789012345",
		Dnn:           "internet",
		PduSessId:     1,
		StartTime:     time.Now(),
		TotalUlVolume: 1024,
		TotalDlVolume: 2048,
	}

	// Store
	ctx.StoreUeData(data)

	// Retrieve
	retrieved, ok := ctx.GetUeData("imsi-123456789012345")
	if !ok {
		t.Fatal("Expected UE data to be found")
	}
	if retrieved.Dnn != "internet" {
		t.Errorf("Expected dnn 'internet', got '%s'", retrieved.Dnn)
	}
	if retrieved.TotalUlVolume != 1024 {
		t.Errorf("Expected ulVolume 1024, got %d", retrieved.TotalUlVolume)
	}
}

func TestCollectorContext_GetOrCreateUeData(t *testing.T) {
	ctx := GetSelf()
	ctx.ClearUeData()

	// Create new
	data := ctx.GetOrCreateUeData("imsi-new-ue")
	if data.Supi != "imsi-new-ue" {
		t.Errorf("Expected supi 'imsi-new-ue', got '%s'", data.Supi)
	}
	if data.Events == nil {
		t.Error("Expected Events slice to be initialized")
	}

	// Get existing
	data2 := ctx.GetOrCreateUeData("imsi-new-ue")
	if data2.Supi != "imsi-new-ue" {
		t.Errorf("Expected to get same UE data")
	}
}

func TestCollectorContext_AppendEvent(t *testing.T) {
	ctx := GetSelf()
	ctx.ClearUeData()

	now := time.Now()
	event := models.SmfEventExposureEventNotification{
		Event:     models.SmfEvent_PDU_SES_EST,
		Supi:      "imsi-123456789012345",
		Dnn:       "internet",
		PduSeId:   1,
		TimeStamp: &now,
	}

	ctx.AppendEvent("imsi-123456789012345", event)

	data, ok := ctx.GetUeData("imsi-123456789012345")
	if !ok {
		t.Fatal("Expected UE data to be created")
	}
	if len(data.Events) != 1 {
		t.Errorf("Expected 1 event, got %d", len(data.Events))
	}
	if data.Dnn != "internet" {
		t.Errorf("Expected dnn 'internet', got '%s'", data.Dnn)
	}
	if data.PduSessId != 1 {
		t.Errorf("Expected pduSessId 1, got %d", data.PduSessId)
	}
}

func TestHandleNotification(t *testing.T) {
	ctx := GetSelf()
	ctx.ClearUeData()

	now := time.Now()
	notification := &models.NsmfEventExposureNotification{
		NotifId: "notif-001",
		EventNotifs: []models.SmfEventExposureEventNotification{
			{
				Event:     models.SmfEvent_PDU_SES_EST,
				Supi:      "imsi-111111111111111",
				Dnn:       "internet",
				PduSeId:   1,
				TimeStamp: &now,
			},
			{
				Event:     models.SmfEvent_PDU_SES_REL,
				Supi:      "imsi-111111111111111",
				PduSeId:   1,
				TimeStamp: &now,
			},
		},
	}

	err := ctx.HandleNotification(notification)
	if err != nil {
		t.Fatalf("HandleNotification failed: %v", err)
	}

	data, ok := ctx.GetUeData("imsi-111111111111111")
	if !ok {
		t.Fatal("Expected UE data to be stored")
	}
	if len(data.Events) != 2 {
		t.Errorf("Expected 2 events, got %d", len(data.Events))
	}
	if data.SessionCount != 1 {
		t.Errorf("Expected sessionCount 1, got %d", data.SessionCount)
	}
}

func TestHandleUpfNotification(t *testing.T) {
	ctx := GetSelf()
	ctx.ClearUeData()

	now := time.Now()
	notification := &UpfNotificationData{
		NotificationItems: []UpfNotificationItem{
			{
				EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
				Supi:      "imsi-222222222222222",
				Dnn:       "internet",
				TimeStamp: now,
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
		CorrelationId: "upf-notif-001",
	}

	err := ctx.HandleUpfNotification(notification)
	if err != nil {
		t.Fatalf("HandleUpfNotification failed: %v", err)
	}

	data, ok := ctx.GetUeData("imsi-222222222222222")
	if !ok {
		t.Fatal("Expected UE data to be stored")
	}
	if data.TotalUlVolume != 1024 {
		t.Errorf("Expected ulVolume 1024, got %d", data.TotalUlVolume)
	}
	if data.TotalDlVolume != 2048 {
		t.Errorf("Expected dlVolume 2048, got %d", data.TotalDlVolume)
	}
	if data.Dnn != "internet" {
		t.Errorf("Expected dnn 'internet', got '%s'", data.Dnn)
	}
	if data.LastUlThroughput != "10 Mbps" {
		t.Errorf("Expected ulThroughput '10 Mbps', got '%s'", data.LastUlThroughput)
	}
	if data.LastDlThroughput != "50 Mbps" {
		t.Errorf("Expected dlThroughput '50 Mbps', got '%s'", data.LastDlThroughput)
	}
}

func TestHandleUpfNotification_VolumeAggregation(t *testing.T) {
	ctx := GetSelf()
	ctx.ClearUeData()

	supi := "imsi-333333333333333"

	// First notification
	notif1 := &UpfNotificationData{
		NotificationItems: []UpfNotificationItem{
			{
				EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
				Supi:      supi,
				TimeStamp: time.Now(),
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
	ctx.HandleUpfNotification(notif1)

	// Second notification - should aggregate
	notif2 := &UpfNotificationData{
		NotificationItems: []UpfNotificationItem{
			{
				EventType: UpfEventType_USER_DATA_USAGE_MEASURES,
				Supi:      supi,
				TimeStamp: time.Now(),
				UserDataUsageMeasurements: []UserDataUsageMeasurements{
					{
						VolumeMeasurement: &VolumeMeasurement{
							UlVolume: 500,
							DlVolume: 1000,
						},
						ThroughputMeasurement: &ThroughputMeasurement{
							UlThroughput: "5 Mbps",
							DlThroughput: "20 Mbps",
						},
					},
				},
			},
		},
	}
	ctx.HandleUpfNotification(notif2)

	data, ok := ctx.GetUeData(supi)
	if !ok {
		t.Fatal("Expected UE data to be stored")
	}

	// Volume should be aggregated (1000+500, 2000+1000)
	if data.TotalUlVolume != 1500 {
		t.Errorf("Expected aggregated ulVolume 1500, got %d", data.TotalUlVolume)
	}
	if data.TotalDlVolume != 3000 {
		t.Errorf("Expected aggregated dlVolume 3000, got %d", data.TotalDlVolume)
	}

	// Throughput should be latest value
	if data.LastUlThroughput != "5 Mbps" {
		t.Errorf("Expected latest ulThroughput '5 Mbps', got '%s'", data.LastUlThroughput)
	}
	if data.LastDlThroughput != "20 Mbps" {
		t.Errorf("Expected latest dlThroughput '20 Mbps', got '%s'", data.LastDlThroughput)
	}
}
