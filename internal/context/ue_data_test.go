package context

import (
	"sync"
	"testing"
	"time"

	"github.com/free5gc/openapi/models"
)

// =============================================================================
// UE Data CRUD Tests
// =============================================================================

func TestUeDataStore(t *testing.T) {
	tests := []struct {
		name string
		supi string
		dnn  string
	}{
		{"basic store", "imsi-208930000000001", "internet"},
		{"different supi", "imsi-208930000000002", "iot"},
		{"different dnn", "imsi-208930000000003", "enterprise"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Init()
			ctx := GetSelf()
			ctx.ClearUeData()

			data := &UeCommunicationData{
				Supi: tt.supi,
				Dnn:  tt.dnn,
			}
			ctx.StoreUeData(data)

			retrieved, ok := ctx.GetUeData(tt.supi)
			if !ok {
				t.Fatal("GetUeData() returned false for stored data")
			}
			if retrieved.Supi != tt.supi {
				t.Errorf("Supi = %v, want %v", retrieved.Supi, tt.supi)
			}
			if retrieved.Dnn != tt.dnn {
				t.Errorf("Dnn = %v, want %v", retrieved.Dnn, tt.dnn)
			}
		})
	}
}

func TestUeDataGet_NotFound(t *testing.T) {
	Init()
	ctx := GetSelf()
	ctx.ClearUeData()

	_, ok := ctx.GetUeData("non-existent-supi")
	if ok {
		t.Error("GetUeData() should return false for non-existent SUPI")
	}
}

func TestGetOrCreateUeData(t *testing.T) {
	Init()
	ctx := GetSelf()
	ctx.ClearUeData()

	supi := "imsi-208930000000001"

	// First call should create new data
	data1 := ctx.GetOrCreateUeData(supi)
	if data1 == nil {
		t.Fatal("GetOrCreateUeData() returned nil")
	}
	if data1.Supi != supi {
		t.Errorf("Supi = %v, want %v", data1.Supi, supi)
	}

	// Add some data
	data1.Lock()
	data1.RawUpfData = append(data1.RawUpfData, UpfDataPoint{UlVolume: 100})
	data1.Unlock()

	// Second call should return existing data
	data2 := ctx.GetOrCreateUeData(supi)
	if len(data2.RawUpfData) != 1 {
		t.Errorf("GetOrCreateUeData() should return existing data, got new data")
	}
	if data2.RawUpfData[0].UlVolume != 100 {
		t.Errorf("UlVolume = %v, want 100", data2.RawUpfData[0].UlVolume)
	}
}

func TestClearUeData(t *testing.T) {
	Init()
	ctx := GetSelf()

	// Store some data
	ctx.StoreUeData(&UeCommunicationData{Supi: "supi-1"})
	ctx.StoreUeData(&UeCommunicationData{Supi: "supi-2"})
	ctx.StoreUeData(&UeCommunicationData{Supi: "supi-3"})

	// Clear all data
	ctx.ClearUeData()

	// Verify all cleared
	for _, supi := range []string{"supi-1", "supi-2", "supi-3"} {
		if _, ok := ctx.GetUeData(supi); ok {
			t.Errorf("ClearUeData() did not remove %s", supi)
		}
	}
}

// =============================================================================
// RawUpfData Tests
// =============================================================================

func TestRawUpfData_Append(t *testing.T) {
	Init()
	ctx := GetSelf()
	ctx.ClearUeData()

	supi := "imsi-208930000000001"
	data := ctx.GetOrCreateUeData(supi)

	// Append multiple data points
	dataPoints := []UpfDataPoint{
		{Timestamp: time.Now(), UlVolume: 100, DlVolume: 200},
		{Timestamp: time.Now().Add(10 * time.Second), UlVolume: 150, DlVolume: 300},
		{Timestamp: time.Now().Add(20 * time.Second), UlVolume: 200, DlVolume: 400},
	}

	data.Lock()
	for _, dp := range dataPoints {
		data.RawUpfData = append(data.RawUpfData, dp)
	}
	data.Unlock()

	// Verify
	if len(data.RawUpfData) != 3 {
		t.Errorf("RawUpfData length = %v, want 3", len(data.RawUpfData))
	}

	for i, dp := range dataPoints {
		if data.RawUpfData[i].UlVolume != dp.UlVolume {
			t.Errorf("RawUpfData[%d].UlVolume = %v, want %v", i, data.RawUpfData[i].UlVolume, dp.UlVolume)
		}
		if data.RawUpfData[i].DlVolume != dp.DlVolume {
			t.Errorf("RawUpfData[%d].DlVolume = %v, want %v", i, data.RawUpfData[i].DlVolume, dp.DlVolume)
		}
	}
}

func TestRawUpfData_TimestampPreservation(t *testing.T) {
	Init()
	ctx := GetSelf()
	ctx.ClearUeData()

	supi := "imsi-208930000000001"
	data := ctx.GetOrCreateUeData(supi)

	ts1 := time.Date(2026, 1, 28, 10, 0, 0, 0, time.UTC)
	ts2 := time.Date(2026, 1, 28, 10, 5, 0, 0, time.UTC)
	ts3 := time.Date(2026, 1, 28, 10, 10, 0, 0, time.UTC)

	data.Lock()
	data.RawUpfData = append(data.RawUpfData,
		UpfDataPoint{Timestamp: ts1, UlVolume: 100},
		UpfDataPoint{Timestamp: ts2, UlVolume: 200},
		UpfDataPoint{Timestamp: ts3, UlVolume: 300},
	)
	data.Unlock()

	// Verify timestamps are preserved exactly
	if !data.RawUpfData[0].Timestamp.Equal(ts1) {
		t.Errorf("Timestamp[0] not preserved: got %v, want %v", data.RawUpfData[0].Timestamp, ts1)
	}
	if !data.RawUpfData[1].Timestamp.Equal(ts2) {
		t.Errorf("Timestamp[1] not preserved: got %v, want %v", data.RawUpfData[1].Timestamp, ts2)
	}
	if !data.RawUpfData[2].Timestamp.Equal(ts3) {
		t.Errorf("Timestamp[2] not preserved: got %v, want %v", data.RawUpfData[2].Timestamp, ts3)
	}
}

func TestRawUpfData_ThroughputStorage(t *testing.T) {
	Init()
	ctx := GetSelf()
	ctx.ClearUeData()

	supi := "imsi-208930000000001"
	data := ctx.GetOrCreateUeData(supi)

	data.Lock()
	data.RawUpfData = append(data.RawUpfData,
		UpfDataPoint{UlThroughput: "10 Mbps", DlThroughput: "50 Mbps"},
		UpfDataPoint{UlThroughput: "15 Mbps", DlThroughput: "100 Mbps"},
	)
	data.Unlock()

	if data.RawUpfData[0].UlThroughput != "10 Mbps" {
		t.Errorf("UlThroughput[0] = %v, want '10 Mbps'", data.RawUpfData[0].UlThroughput)
	}
	if data.RawUpfData[1].DlThroughput != "100 Mbps" {
		t.Errorf("DlThroughput[1] = %v, want '100 Mbps'", data.RawUpfData[1].DlThroughput)
	}
}

// =============================================================================
// AppendEvent Tests
// =============================================================================

func TestAppendEvent(t *testing.T) {
	Init()
	ctx := GetSelf()
	ctx.ClearUeData()

	supi := "imsi-208930000000001"

	event1 := models.SmfEventExposureEventNotification{
		Supi:    supi,
		Dnn:     "internet",
		PduSeId: 1,
		Event:   models.SmfEvent_PDU_SES_EST,
		Snssai:  &models.Snssai{Sst: 1, Sd: "010203"},
	}

	ctx.AppendEvent(supi, event1)

	data, ok := ctx.GetUeData(supi)
	if !ok {
		t.Fatal("AppendEvent should create UE data if not exists")
	}

	if len(data.Events) != 1 {
		t.Errorf("Events count = %v, want 1", len(data.Events))
	}
	if data.Dnn != "internet" {
		t.Errorf("Dnn = %v, want 'internet'", data.Dnn)
	}
	if data.PduSessId != 1 {
		t.Errorf("PduSessId = %v, want 1", data.PduSessId)
	}

	// Append second event
	event2 := models.SmfEventExposureEventNotification{
		Supi:  supi,
		Event: models.SmfEvent_PDU_SES_REL,
	}
	ctx.AppendEvent(supi, event2)

	if len(data.Events) != 2 {
		t.Errorf("Events count = %v, want 2", len(data.Events))
	}
}

// =============================================================================
// Concurrency Tests
// =============================================================================

func TestConcurrentRawDataAppend(t *testing.T) {
	Init()
	ctx := GetSelf()
	ctx.ClearUeData()

	supi := "imsi-concurrent-test"
	iterations := 100
	var wg sync.WaitGroup

	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func(vol int64) {
			defer wg.Done()
			data := ctx.GetOrCreateUeData(supi)
			data.Lock()
			data.RawUpfData = append(data.RawUpfData, UpfDataPoint{
				Timestamp: time.Now(),
				UlVolume:  vol,
			})
			data.Unlock()
		}(int64(i + 1))
	}
	wg.Wait()

	data, ok := ctx.GetUeData(supi)
	if !ok {
		t.Fatal("GetUeData returned false after concurrent appends")
	}

	// Verify all data points were appended
	if len(data.RawUpfData) != iterations {
		t.Errorf("RawUpfData length = %v, want %v", len(data.RawUpfData), iterations)
	}

	// Verify total volume (1+2+3+...+100 = 5050)
	var totalVol int64
	for _, dp := range data.RawUpfData {
		totalVol += dp.UlVolume
	}
	expectedTotal := int64(iterations * (iterations + 1) / 2)
	if totalVol != expectedTotal {
		t.Errorf("Total UlVolume = %v, want %v", totalVol, expectedTotal)
	}
}

func TestConcurrentGetOrCreate(t *testing.T) {
	Init()
	ctx := GetSelf()
	ctx.ClearUeData()

	supi := "imsi-concurrent-getorcreate"
	iterations := 50
	var wg sync.WaitGroup

	// Multiple goroutines calling GetOrCreateUeData concurrently
	results := make([]*UeCommunicationData, iterations)
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = ctx.GetOrCreateUeData(supi)
		}(i)
	}
	wg.Wait()

	// All should return the same instance
	for i := 1; i < iterations; i++ {
		if results[i] != results[0] {
			t.Error("GetOrCreateUeData returned different instances for same SUPI")
		}
	}
}
