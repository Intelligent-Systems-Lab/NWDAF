package anlf

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/free5gc/nwdaf/pkg/factory"
)

type pairCSVRecord struct {
	CheckTime   time.Time
	ModelURL    string
	ScopeKey    string
	NwdafSubID  string
	PredictedAt time.Time
	TargetTime  time.Time
	PredUl      int64
	ActualUl    *int64
	PredDl      int64
	ActualDl    *int64
}

type accuracyCSVWriter struct {
	mu          sync.Mutex
	metricsFile *os.File
	pairsFile   *os.File
	metricsCSV  *csv.Writer
	pairsCSV    *csv.Writer
	closed      bool
}

type accuracyCSVManager struct {
	once      sync.Once
	closeOnce sync.Once
	writer    *accuracyCSVWriter
	err       error
}

var globalAccuracyCSVManager accuracyCSVManager

func getAccuracyCSVWriter(
	ctx context.Context,
	accCfg *factory.AccuracyMonitorConfig,
) (*accuracyCSVWriter, error) {
	if accCfg == nil || !accCfg.CSVDumpEnabledOrDefault() {
		return nil, nil
	}

	globalAccuracyCSVManager.once.Do(func() {
		globalAccuracyCSVManager.writer, globalAccuracyCSVManager.err = newAccuracyCSVWriter(
			accCfg.CSVDumpDirOrDefault(),
			time.Now(),
		)
		if globalAccuracyCSVManager.err == nil && ctx != nil {
			go func() {
				<-ctx.Done()
				globalAccuracyCSVManager.closeOnce.Do(func() {
					if closeErr := globalAccuracyCSVManager.writer.Close(); closeErr != nil {
						anlfLog.Warnf("Accuracy CSV close failed: %v", closeErr)
					}
				})
			}()
		}
	})

	return globalAccuracyCSVManager.writer, globalAccuracyCSVManager.err
}

func newAccuracyCSVWriter(dir string, ts time.Time) (*accuracyCSVWriter, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create CSV dump dir: %w", err)
	}

	suffix := ts.Format("20060102_150405")
	metricsPath := filepath.Join(dir, "metrics_"+suffix+".csv")
	pairsPath := filepath.Join(dir, "pairs_"+suffix+".csv")

	metricsFile, err := os.Create(metricsPath)
	if err != nil {
		return nil, fmt.Errorf("create metrics CSV: %w", err)
	}

	pairsFile, err := os.Create(pairsPath)
	if err != nil {
		if closeErr := metricsFile.Close(); closeErr != nil {
			anlfLog.Debugf("failed to close metrics CSV after pairs open error: %v", closeErr)
		}
		return nil, fmt.Errorf("create pairs CSV: %w", err)
	}

	writer := &accuracyCSVWriter{
		metricsFile: metricsFile,
		pairsFile:   pairsFile,
		metricsCSV:  csv.NewWriter(metricsFile),
		pairsCSV:    csv.NewWriter(pairsFile),
	}

	writeHeaderErr := writer.metricsCSV.Write([]string{
		"timestamp", "model", "scope", "metric", "current",
		"sampleCount", "inferenceNum", "windowStart", "windowEnd",
	})
	if writeHeaderErr != nil {
		if closeErr := writer.Close(); closeErr != nil {
			anlfLog.Debugf("failed to close CSV writer after metrics header error: %v", closeErr)
		}
		return nil, fmt.Errorf("write metrics CSV header: %w", writeHeaderErr)
	}

	writeHeaderErr = writer.pairsCSV.Write([]string{
		"checkTime", "model", "scope", "nwdafSubId", "predictedAt",
		"targetTime", "predUl", "actualUl", "predDl", "actualDl",
	})
	if writeHeaderErr != nil {
		if closeErr := writer.Close(); closeErr != nil {
			anlfLog.Debugf("failed to close CSV writer after pairs header error: %v", closeErr)
		}
		return nil, fmt.Errorf("write pairs CSV header: %w", writeHeaderErr)
	}

	writer.metricsCSV.Flush()
	writer.pairsCSV.Flush()
	flushErr := writer.flushErrors()
	if flushErr != nil {
		if closeErr := writer.Close(); closeErr != nil {
			anlfLog.Debugf("failed to close CSV writer after flush error: %v", closeErr)
		}
		return nil, flushErr
	}

	return writer, nil
}

func (w *accuracyCSVWriter) WriteMetrics(timestamp time.Time, reports []AccuracyReport) error {
	if w == nil || len(reports) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	for _, report := range reports {
		for metric, value := range report.Metrics {
			if err := w.metricsCSV.Write([]string{
				timestamp.UTC().Format(time.RFC3339),
				report.ModelURL,
				report.ScopeKey,
				metric,
				strconv.FormatFloat(value, 'f', -1, 64),
				strconv.Itoa(report.SampleCount),
				strconv.Itoa(report.InferenceNum),
				report.WindowStart.UTC().Format(time.RFC3339),
				report.WindowEnd.UTC().Format(time.RFC3339),
			}); err != nil {
				return fmt.Errorf("write metrics CSV row: %w", err)
			}
		}
	}

	w.metricsCSV.Flush()
	return w.flushErrors()
}

func (w *accuracyCSVWriter) WritePairs(rows []pairCSVRecord) error {
	if w == nil || len(rows) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	for _, row := range rows {
		record := []string{
			row.CheckTime.UTC().Format(time.RFC3339),
			row.ModelURL,
			row.ScopeKey,
			row.NwdafSubID,
			row.PredictedAt.UTC().Format(time.RFC3339),
			row.TargetTime.UTC().Format(time.RFC3339),
			strconv.FormatInt(row.PredUl, 10),
			"",
			strconv.FormatInt(row.PredDl, 10),
			"",
		}
		if row.ActualUl != nil {
			record[7] = strconv.FormatInt(*row.ActualUl, 10)
		}
		if row.ActualDl != nil {
			record[9] = strconv.FormatInt(*row.ActualDl, 10)
		}

		if err := w.pairsCSV.Write(record); err != nil {
			return fmt.Errorf("write pairs CSV row: %w", err)
		}
	}

	w.pairsCSV.Flush()
	return w.flushErrors()
}

func (w *accuracyCSVWriter) Close() error {
	if w == nil {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	w.metricsCSV.Flush()
	w.pairsCSV.Flush()
	if err := w.flushErrors(); err != nil {
		return err
	}

	if err := w.metricsFile.Close(); err != nil {
		return fmt.Errorf("close metrics CSV: %w", err)
	}
	if err := w.pairsFile.Close(); err != nil {
		return fmt.Errorf("close pairs CSV: %w", err)
	}

	w.closed = true
	return nil
}

func (w *accuracyCSVWriter) flushErrors() error {
	if err := w.metricsCSV.Error(); err != nil {
		return fmt.Errorf("metrics CSV flush: %w", err)
	}
	if err := w.pairsCSV.Error(); err != nil {
		return fmt.Errorf("pairs CSV flush: %w", err)
	}
	return nil
}
