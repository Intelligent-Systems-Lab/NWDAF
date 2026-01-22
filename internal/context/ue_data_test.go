package context

import (
	"sync"
	"testing"
	"time"

	"github.com/free5gc/openapi/models"
)

// TestUeDataCRUD tests UE communication data storage operations
func TestUeDataCRUD(t *testing.T) {
	Init()
	ctx := GetSelf()

	// Clear any existing data
	ctx.ClearUeData()

	supi := "imsi-208930000000003"

	// Test Store
	data := &UeCommunicationData{
		Supi:          supi,
		Dnn:           "internet",
		TotalUlVolume: 1024000,
		TotalDlVolume: 5120000,
		StartTime:     time.Now(),
		LastUpdate:    time.Now(),
		IsActive:      true,
		SessionCount:  1,
	}

	ctx.StoreUeData(data)

	// Test Get
	retrieved, ok := ctx.GetUeData(supi)
	if !ok {
		t.Error("GetUeData() returned false for existing data")
	}
	if retrieved.Dnn != data.Dnn {
		t.Errorf("Dnn = %v, want %v", retrieved.Dnn, data.Dnn)
	}
	if retrieved.TotalUlVolume != data.TotalUlVolume {
		t.Errorf("TotalUlVolume = %v, want %v", retrieved.TotalUlVolume, data.TotalUlVolume)
	}

	// Test Get non-existent
	_, ok = ctx.GetUeData("non-existent-supi")
	if ok {
		t.Error("GetUeData() should return false for non-existent SUPI")
	}

	// Test GetOrCreate for existing
	existing := ctx.GetOrCreateUeData(supi)
	if existing.TotalUlVolume != data.TotalUlVolume {
		t.Error("GetOrCreateUeData() should return existing data")
	}

	// Test GetOrCreate for new
	newData := ctx.GetOrCreateUeData("imsi-new-supi")
	if newData == nil {
		t.Error("GetOrCreateUeData() returned nil")
	}
	if newData.Supi != "imsi-new-supi" {
		t.Errorf("Supi = %v, want imsi-new-supi", newData.Supi)
	}
}

// TestAppendEvent tests appending SMF events to UE data
func TestAppendEvent(t *testing.T) {
	Init()
	ctx := GetSelf()
	ctx.ClearUeData()

	supi := "imsi-208930000000003"

	// Append event to non-existent UE (should create new)
	event := models.SmfEventExposureEventNotification{
		Supi:    supi,
		Dnn:     "internet",
		PduSeId: 1,
		Event:   models.SmfEvent_PDU_SES_EST,
		Snssai:  &models.Snssai{Sst: 1, Sd: "010203"},
	}

	ctx.AppendEvent(supi, event)

	// Verify data was created and updated
	data, ok := ctx.GetUeData(supi)
	if !ok {
		t.Error("AppendEvent should create UE data if not exists")
	}
	if data.Dnn != "internet" {
		t.Errorf("AppendEvent did not update Dnn, got %v", data.Dnn)
	}
	if data.PduSessId != 1 {
		t.Errorf("AppendEvent did not update PduSessId, got %v", data.PduSessId)
	}
	if len(data.Events) != 1 {
		t.Errorf("Events count = %v, want 1", len(data.Events))
	}

	// Append another event
	event2 := models.SmfEventExposureEventNotification{
		Supi:  supi,
		Event: models.SmfEvent_PDU_SES_REL,
	}
	ctx.AppendEvent(supi, event2)

	data, _ = ctx.GetUeData(supi)
	if len(data.Events) != 2 {
		t.Errorf("Events count = %v, want 2", len(data.Events))
	}
}

// TestClearUeData tests clearing all UE data
func TestClearUeData(t *testing.T) {
	Init()
	ctx := GetSelf()

	// Add some data
	ctx.StoreUeData(&UeCommunicationData{Supi: "supi-1"})
	ctx.StoreUeData(&UeCommunicationData{Supi: "supi-2"})

	// Clear
	ctx.ClearUeData()

	// Verify
	_, ok1 := ctx.GetUeData("supi-1")
	_, ok2 := ctx.GetUeData("supi-2")
	if ok1 || ok2 {
		t.Error("ClearUeData() did not remove all data")
	}
}

// TestUeDataConcurrentUpdate tests thread safety of UE data updates
func TestUeDataConcurrentUpdate(t *testing.T) {
	Init()
	ctx := GetSelf()
	ctx.ClearUeData()

	supi := "imsi-concurrent-test"
	var wg sync.WaitGroup

	// 100 goroutines concurrently incrementing the volume
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data := ctx.GetOrCreateUeData(supi)
			data.Lock()
			data.TotalUlVolume += 1
			data.Unlock()
		}()
	}
	wg.Wait()

	data, ok := ctx.GetUeData(supi)
	if !ok {
		t.Fatal("GetUeData returned false")
	}
	if data.TotalUlVolume != 100 {
		t.Errorf("Expected TotalUlVolume=100, got %d (race condition detected!)", data.TotalUlVolume)
	}
}
