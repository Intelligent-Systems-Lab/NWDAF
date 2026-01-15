package processor

import (
	"testing"
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/openapi/models"
)

// TestHandleSmfNotification tests SMF notification processing
func TestHandleSmfNotification(t *testing.T) {
	nwdaf_context.Init()
	ctx := nwdaf_context.GetSelf()
	ctx.ClearUeData()

	p := &Processor{}

	notification := &models.NsmfEventExposureNotification{
		NotifId: "notif-001",
		EventNotifs: []models.SmfEventExposureEventNotification{
			{
				Supi:    "imsi-208930000000003",
				Dnn:     "internet",
				PduSeId: 1,
				Event:   models.SmfEvent_PDU_SES_EST,
			},
		},
	}

	err := p.HandleSmfNotification(notification)
	if err != nil {
		t.Errorf("HandleSmfNotification() error = %v", err)
	}

	// Verify UE data was stored
	data, ok := ctx.GetUeData("imsi-208930000000003")
	if !ok {
		t.Error("HandleSmfNotification() did not store UE data")
	}
	if data.Dnn != "internet" {
		t.Errorf("Dnn = %v, want internet", data.Dnn)
	}
	if !data.IsActive {
		t.Error("IsActive should be true after PDU_SES_EST")
	}
}

// TestHandlePduSessionLifecycle tests PDU session establishment and release
func TestHandlePduSessionLifecycle(t *testing.T) {
	nwdaf_context.Init()
	ctx := nwdaf_context.GetSelf()
	ctx.ClearUeData()

	p := &Processor{}
	supi := "imsi-208930000000003"

	// Simulate PDU session establishment
	estTime := time.Now()
	estEvent := &models.SmfEventExposureEventNotification{
		Supi:      supi,
		Dnn:       "internet",
		PduSeId:   1,
		Event:     models.SmfEvent_PDU_SES_EST,
		TimeStamp: &estTime,
	}

	p.handlePduSessionEstablished(ctx, estEvent)

	data, ok := ctx.GetUeData(supi)
	if !ok {
		t.Fatal("UE data not created after PDU_SES_EST")
	}
	if data.SessionCount != 1 {
		t.Errorf("SessionCount = %v, want 1", data.SessionCount)
	}
	if !data.IsActive {
		t.Error("IsActive should be true after establishment")
	}

	// Simulate PDU session release (after 2 seconds)
	relTime := estTime.Add(2 * time.Second)
	relEvent := &models.SmfEventExposureEventNotification{
		Supi:      supi,
		PduSeId:   1,
		Event:     models.SmfEvent_PDU_SES_REL,
		TimeStamp: &relTime,
	}

	p.handlePduSessionReleased(ctx, relEvent)

	data, _ = ctx.GetUeData(supi)
	if data.IsActive {
		t.Error("IsActive should be false after release")
	}
	if data.TotalCommDuration < 2*time.Second {
		t.Errorf("TotalCommDuration = %v, expected >= 2s", data.TotalCommDuration)
	}
}

// TestHandleSmfNotification_MissingSupi tests handling without SUPI
func TestHandleSmfNotification_MissingSupi(t *testing.T) {
	nwdaf_context.Init()
	ctx := nwdaf_context.GetSelf()
	ctx.ClearUeData()

	p := &Processor{}

	// Event without SUPI should be skipped
	notification := &models.NsmfEventExposureNotification{
		NotifId: "notif-002",
		EventNotifs: []models.SmfEventExposureEventNotification{
			{
				Dnn:   "internet",
				Event: models.SmfEvent_PDU_SES_EST,
			},
		},
	}

	err := p.HandleSmfNotification(notification)
	if err != nil {
		t.Errorf("HandleSmfNotification() should not error on missing SUPI")
	}
}

// TestHandleMultipleEvents tests processing multiple events in one notification
func TestHandleMultipleEvents(t *testing.T) {
	nwdaf_context.Init()
	ctx := nwdaf_context.GetSelf()
	ctx.ClearUeData()

	p := &Processor{}

	now := time.Now()
	notification := &models.NsmfEventExposureNotification{
		NotifId: "notif-003",
		EventNotifs: []models.SmfEventExposureEventNotification{
			{
				Supi:      "imsi-001",
				Dnn:       "internet",
				Event:     models.SmfEvent_PDU_SES_EST,
				TimeStamp: &now,
			},
			{
				Supi:      "imsi-002",
				Dnn:       "internet",
				Event:     models.SmfEvent_PDU_SES_EST,
				TimeStamp: &now,
			},
		},
	}

	p.HandleSmfNotification(notification)

	// Both UEs should have data
	_, ok1 := ctx.GetUeData("imsi-001")
	_, ok2 := ctx.GetUeData("imsi-002")

	if !ok1 || !ok2 {
		t.Error("HandleSmfNotification should process all events")
	}
}
