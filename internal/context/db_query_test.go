package context

import (
	"context"
	"testing"
	"time"
)

// =============================================================================
// reverseRecords Tests
// =============================================================================

func TestReverseRecords_Empty(t *testing.T) {
	records := []UpfTrafficRecord{}
	reverseRecords(records)
	if len(records) != 0 {
		t.Errorf("expected empty slice after reverse, got %d", len(records))
	}
}

func TestReverseRecords_Single(t *testing.T) {
	records := []UpfTrafficRecord{
		{UlVolume: 100},
	}
	reverseRecords(records)
	if records[0].UlVolume != 100 {
		t.Errorf("single element should be unchanged, got %d", records[0].UlVolume)
	}
}

func TestReverseRecords_Multiple(t *testing.T) {
	t0 := time.Now()
	t1 := t0.Add(10 * time.Second)
	t2 := t0.Add(20 * time.Second)

	// Simulate DESC order returned by MongoDB (newest first)
	records := []UpfTrafficRecord{
		{Timestamp: t2, UlVolume: 300},
		{Timestamp: t1, UlVolume: 200},
		{Timestamp: t0, UlVolume: 100},
	}
	reverseRecords(records)

	// After reverse should be ASC (oldest first) — correct ML input order
	if records[0].UlVolume != 100 {
		t.Errorf("records[0].UlVolume = %d, want 100 (oldest)", records[0].UlVolume)
	}
	if records[1].UlVolume != 200 {
		t.Errorf("records[1].UlVolume = %d, want 200", records[1].UlVolume)
	}
	if records[2].UlVolume != 300 {
		t.Errorf("records[2].UlVolume = %d, want 300 (newest)", records[2].UlVolume)
	}

	// Timestamps should now be ascending
	if !records[0].Timestamp.Before(records[1].Timestamp) {
		t.Error("after reverse, timestamps should be ascending")
	}
	if !records[1].Timestamp.Before(records[2].Timestamp) {
		t.Error("after reverse, timestamps should be ascending")
	}
}

func TestReverseRecords_Even(t *testing.T) {
	records := []UpfTrafficRecord{
		{UlVolume: 4},
		{UlVolume: 3},
		{UlVolume: 2},
		{UlVolume: 1},
	}
	reverseRecords(records)
	expected := []int64{1, 2, 3, 4}
	for i, exp := range expected {
		if records[i].UlVolume != exp {
			t.Errorf("records[%d].UlVolume = %d, want %d", i, records[i].UlVolume, exp)
		}
	}
}

// =============================================================================
// IsMongoAvailable Tests
// =============================================================================

func TestIsMongoAvailable_ReturnsFalseWithoutClient(t *testing.T) {
	// In unit tests mongoapi.Client is always nil (no real MongoDB)
	if IsMongoAvailable() {
		t.Error("IsMongoAvailable should return false when mongoapi.Client is nil")
	}
}

// =============================================================================
// Query functions return nil,nil when MongoDB unavailable
// =============================================================================

func TestQueryTrafficByCorrelationId_NoMongo(t *testing.T) {
	records, err := QueryTrafficByCorrelationId(
		context.Background(),
		"testdb", "corr-001", time.Now().Add(-5*time.Minute), 30,
	)
	if err != nil {
		t.Errorf("expected nil error when MongoDB unavailable, got %v", err)
	}
	if records != nil {
		t.Errorf("expected nil records when MongoDB unavailable, got %v", records)
	}
}

func TestQueryTrafficByMultipleCorrelationIds_NoMongo(t *testing.T) {
	records, err := QueryTrafficByMultipleCorrelationIds(
		context.Background(),
		"testdb", []string{"corr-001", "corr-002"}, time.Now().Add(-5*time.Minute), 30,
	)
	if err != nil {
		t.Errorf("expected nil error when MongoDB unavailable, got %v", err)
	}
	if records != nil {
		t.Errorf("expected nil records when MongoDB unavailable, got %v", records)
	}
}

func TestQueryTrafficByMultipleCorrelationIds_EmptyIds(t *testing.T) {
	records, err := QueryTrafficByMultipleCorrelationIds(
		context.Background(),
		"testdb", []string{}, time.Now().Add(-5*time.Minute), 30,
	)
	if err != nil {
		t.Errorf("expected nil error for empty correlationIds, got %v", err)
	}
	if records != nil {
		t.Errorf("expected nil records for empty correlationIds, got %v", records)
	}
}

func TestQueryTrafficInTimeRange_NoMongo(t *testing.T) {
	from := time.Now().Add(-10 * time.Second)
	to := time.Now()
	records, err := QueryTrafficInTimeRange(
		context.Background(),
		"testdb", []string{"corr-001"}, from, to,
	)
	if err != nil {
		t.Errorf("expected nil error when MongoDB unavailable, got %v", err)
	}
	if records != nil {
		t.Errorf("expected nil records when MongoDB unavailable, got %v", records)
	}
}

func TestQueryTrafficInTimeRange_EmptyIds(t *testing.T) {
	from := time.Now().Add(-10 * time.Second)
	to := time.Now()
	records, err := QueryTrafficInTimeRange(
		context.Background(),
		"testdb", []string{}, from, to,
	)
	if err != nil {
		t.Errorf("expected nil error for empty correlationIds, got %v", err)
	}
	if records != nil {
		t.Errorf("expected nil records for empty correlationIds, got %v", records)
	}
}
