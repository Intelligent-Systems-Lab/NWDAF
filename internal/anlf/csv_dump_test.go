package anlf

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAccuracyCSVWriter_WritesHeadersAndRows(t *testing.T) {
	dir := t.TempDir()
	ts := time.Date(2026, 4, 22, 13, 45, 0, 0, time.UTC)

	writer, err := newAccuracyCSVWriter(dir, ts)
	if err != nil {
		t.Fatalf("newAccuracyCSVWriter() error = %v", err)
	}

	actualUl := int64(120)
	actualDl := int64(220)
	err = writer.WritePairs([]pairCSVRecord{
		{
			CheckTime:   ts,
			ModelURL:    "file:///test/model.pth",
			ScopeKey:    "group:group-a",
			NwdafSubID:  "sub-a",
			PredictedAt: ts.Add(-time.Minute),
			TargetTime:  ts.Add(time.Minute),
			PredUl:      100,
			ActualUl:    &actualUl,
			PredDl:      200,
			ActualDl:    &actualDl,
		},
	})
	if err != nil {
		t.Fatalf("WritePairs() error = %v", err)
	}

	err = writer.WriteMetrics(ts, []AccuracyReport{
		{
			ModelURL:     "file:///test/model.pth",
			ScopeKey:     "group:group-a",
			Metrics:      map[string]float64{"MAE": 10},
			SampleCount:  1,
			InferenceNum: 3,
			WindowStart:  ts.Add(-time.Minute),
			WindowEnd:    ts,
		},
	})
	if err != nil {
		t.Fatalf("WriteMetrics() error = %v", err)
	}

	closeErr := writer.Close()
	if closeErr != nil {
		t.Fatalf("Close() error = %v", closeErr)
	}

	metricsBytes, err := os.ReadFile(filepath.Join(dir, "metrics_20260422_134500.csv"))
	if err != nil {
		t.Fatalf("ReadFile(metrics) error = %v", err)
	}
	metrics := string(metricsBytes)
	if !strings.Contains(metrics, "timestamp,model,scope,metric,current,sampleCount,inferenceNum,windowStart,windowEnd") {
		t.Fatalf("metrics CSV missing header: %s", metrics)
	}
	if !strings.Contains(metrics, "group:group-a,MAE,10,1,3") {
		t.Fatalf("metrics CSV missing data row: %s", metrics)
	}

	pairsBytes, err := os.ReadFile(filepath.Join(dir, "pairs_20260422_134500.csv"))
	if err != nil {
		t.Fatalf("ReadFile(pairs) error = %v", err)
	}
	pairs := string(pairsBytes)
	if !strings.Contains(
		pairs,
		"checkTime,model,scope,nwdafSubId,predictedAt,targetTime,predUl,actualUl,predDl,actualDl",
	) {
		t.Fatalf("pairs CSV missing header: %s", pairs)
	}
	if !strings.Contains(pairs, "group:group-a,sub-a") {
		t.Fatalf("pairs CSV missing scope/subscriber row: %s", pairs)
	}
	if !strings.Contains(pairs, ",100,120,200,220") {
		t.Fatalf("pairs CSV missing predicted/actual values: %s", pairs)
	}
}

func TestAccuracyCSVWriter_WritesEmptyActualColumnsWhenGroundTruthMissing(t *testing.T) {
	dir := t.TempDir()
	ts := time.Date(2026, 4, 22, 13, 45, 0, 0, time.UTC)

	writer, err := newAccuracyCSVWriter(dir, ts)
	if err != nil {
		t.Fatalf("newAccuracyCSVWriter() error = %v", err)
	}

	err = writer.WritePairs([]pairCSVRecord{
		{
			CheckTime:   ts,
			ModelURL:    "file:///test/model.pth",
			ScopeKey:    "supi:imsi-001",
			NwdafSubID:  "sub-a",
			PredictedAt: ts.Add(-time.Minute),
			TargetTime:  ts.Add(time.Minute),
			PredUl:      100,
			PredDl:      200,
		},
	})
	if err != nil {
		t.Fatalf("WritePairs() error = %v", err)
	}

	closeErr := writer.Close()
	if closeErr != nil {
		t.Fatalf("Close() error = %v", closeErr)
	}

	pairsBytes, err := os.ReadFile(filepath.Join(dir, "pairs_20260422_134500.csv"))
	if err != nil {
		t.Fatalf("ReadFile(pairs) error = %v", err)
	}
	pairs := string(pairsBytes)
	if !strings.Contains(pairs, ",100,,200,") {
		t.Fatalf("pairs CSV missing empty actual columns: %s", pairs)
	}
}
