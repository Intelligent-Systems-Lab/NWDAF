// Package anlf provides the AnLF inference pipeline for NWDAF analytics.
package anlf

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

var anlfLog = logger.AnlfLog

// errNoHistoricalData is returned when no UPF traffic data is available yet.
// This is expected during startup before SMF delivers the first measurements.
var errNoHistoricalData = fmt.Errorf("no historical data available yet")

// TrafficObservation for ML prediction request
type TrafficObservation = consumer.TrafficObservation

// getMlServiceClient returns a new ML service client if configured
func getMlServiceClient() *consumer.MlServiceClient {
	cfg := factory.NwdafConfig
	if cfg == nil || cfg.Configuration == nil ||
		cfg.Configuration.MlService == nil || !cfg.Configuration.MlService.Enabled {
		return nil
	}

	endpoint := cfg.Configuration.MlService.Endpoint
	if endpoint == "" {
		return nil
	}

	return consumer.NewMlServiceClient(endpoint)
}

// GenerateMockAbnormalBehaviours generates mock DDoS detection analytics data
// TODO: Replace with real analytics from ML model and data collection
func GenerateMockAbnormalBehaviours() []models.AbnormalBehaviour {
	now := time.Now()

	return []models.AbnormalBehaviour{
		{
			Excep: &models.Exception{
				ExcepId:    models.ExceptionId_SUSPICION_OF_DDOS_ATTACK,
				ExcepLevel: 3,
				ExcepTrend: models.ExceptionTrend_UP,
			},
			Supis:      []string{"imsi-123456789012345"},
			Dnn:        "internet",
			Ratio:      15, // 15% abnormal ratio
			Confidence: 85, // 85% confidence
			AddtMeasInfo: &models.AdditionalMeasurement{
				DdosAttack: &models.AddressList{
					Ipv4Addrs: []string{"192.168.1.100", "192.168.1.101"},
				},
				Circums: []models.CircumstanceDescription{
					{
						Tm: &now,
					},
				},
			},
		},
	}
}

// GenerateUeCommunicationAnalytics generates UE Communication analytics
// Per TS 23.288 §6.7.3: Analytics based on collected UPF traffic data
// Uses ML-based prediction if model is ready, returns 0 confidence when insufficient resources
func GenerateUeCommunicationAnalytics(nwdafSubId string) models.UeCommunication {
	ctx := nwdaf_context.GetSelf()
	now := time.Now()

	// Check if ML model is available for this subscription
	mlInfo := ctx.GetMlModelInfo(nwdafSubId)
	if mlInfo != nil && mlInfo.IsReady() {
		result, err := generateMlBasedUeCommunication(nwdafSubId, mlInfo, ctx)
		if err == nil {
			return result
		}
		if err == errNoHistoricalData {
			anlfLog.Debugf("ML prediction skipped for %s: %v", nwdafSubId, err)
		} else {
			anlfLog.Warnf("ML prediction failed for %s: %v", nwdafSubId, err)
		}
	}

	// Per TS 23.288: Return 0 confidence when insufficient resources for analytics
	anlfLog.Debugf("Insufficient resources for ML analytics, returning 0 confidence for %s", nwdafSubId)
	return models.UeCommunication{
		CommDur:    0,
		Ts:         &now,
		TrafChar:   &models.TrafficCharacterization{Dnn: "internet"},
		Confidence: 0,
	}
}

// generateMlBasedUeCommunication uses ML service for prediction
func generateMlBasedUeCommunication(
	nwdafSubId string,
	mlInfo *nwdaf_context.MlModelInfo,
	ctx *nwdaf_context.NWDAFContext,
) (models.UeCommunication, error) {
	now := time.Now()

	// Get ML service client
	mlClient := getMlServiceClient()
	if mlClient == nil {
		return models.UeCommunication{}, fmt.Errorf("ML service client not available")
	}

	// Resolve model params (with defaults)
	params := getUeCommunicationModelParams()
	outputWindow := params.OutputWindowOrDefault()
	samplingInterval := params.SamplingIntervalOrDefault()

	// Snap now to the current period boundary so TargetTime aligns with UPF startTime,
	// and so fetchHistoricalData can derive clean output timestamps.
	si64 := int64(samplingInterval)
	snappedNow := time.Unix((now.Unix()/si64)*si64, 0)

	// Fetch historical data from in-memory ring buffer
	historicalData, dnn := fetchHistoricalData(nwdafSubId, ctx, params, snappedNow)
	if len(historicalData) == 0 {
		return models.UeCommunication{}, errNoHistoricalData
	}

	// Call ML service for prediction
	modelId := mlInfo.GetModelId()
	resp, err := mlClient.Predict(modelId, historicalData)
	if err != nil {
		return models.UeCommunication{}, err
	}

	if len(resp.PredictedData) == 0 {
		return models.UeCommunication{}, fmt.Errorf("no prediction data returned")
	}

	// Aggregate predicted steps
	var totalUl, totalDl int64
	var totalConfidence int32
	for i, pred := range resp.PredictedData {
		totalUl += pred.TrafChar.UlVol
		totalDl += pred.TrafChar.DlVol
		totalConfidence += pred.Confidence

		// Record individual predictions for accuracy monitoring
		// Per TS 23.288 §5C: store predictions for ground truth comparison
		if isAccuracyMonitorEnabled() {
			store := ctx.GetModelAccuracyStore(mlInfo.ModelUrl)
			if store != nil {
				store.AddPrediction(nwdaf_context.PredictionRecord{
					ModelUrl:    mlInfo.ModelUrl,
					PredictedAt: now,
					// Target: i-th step ahead from the snapped period boundary,
					// so TargetTime always aligns with UPF period startTime.
					TargetTime: snappedNow.Add(time.Duration((i+1)*samplingInterval) * time.Second),
					PredUlVol:  pred.TrafChar.UlVol,
					PredDlVol:  pred.TrafChar.DlVol,
					NwdafSubId: nwdafSubId,
				})
			}
		}
	}
	avgConfidence := totalConfidence / int32(len(resp.PredictedData))

	// commDur = total prediction horizon in seconds
	commDur := int32(outputWindow * samplingInterval)

	anlfLog.Infof("ML inference: sub=%s %s steps=%d ulVol=%d dlVol=%d confidence=%d commDur=%ds",
		nwdafSubId, inferenceTargetLabel(nwdafSubId, ctx), len(resp.PredictedData), totalUl, totalDl, avgConfidence, commDur)

	return models.UeCommunication{
		CommDur: commDur,
		Ts:      &now,
		TrafChar: &models.TrafficCharacterization{
			Dnn:   dnn,
			UlVol: totalUl,
			DlVol: totalDl,
		},
		Confidence: avgConfidence,
	}, nil
}

// getUeCommunicationModelParams returns the configured ModelParams for UE_COMMUNICATION.
// Returns a zero-value ModelParams (all defaults) when not configured.
func getUeCommunicationModelParams() *factory.ModelParams {
	cfg := factory.NwdafConfig
	if cfg != nil && cfg.Configuration != nil &&
		cfg.Configuration.Analytics != nil &&
		cfg.Configuration.Analytics.UeCommunication != nil {
		return cfg.Configuration.Analytics.UeCommunication
	}
	return &factory.ModelParams{} // zero value → all helpers return defaults
}

// trafficPoint holds the numeric fields of a single UPF measurement.
// Used as the common intermediate type for sequence alignment.
type trafficPoint struct {
	TotalVol, UlVol, DlVol           float64
	TotalNbPkts, UlNbPkts, DlNbPkts  float64
	UlThr, DlThr, UlPktThr, DlPktThr float64
}

// fetchHistoricalData retrieves recent UPF traffic data for ML prediction from the
// in-memory ring buffer. The buffer is the sole source for inference; MongoDB is
// only consulted by the accuracy monitor for ground truth.
func fetchHistoricalData(
	nwdafSubId string,
	ctx *nwdaf_context.NWDAFContext,
	params *factory.ModelParams,
	snappedNow time.Time,
) ([]TrafficObservation, string) {
	inputWindow := params.InputWindowOrDefault()
	samplingInterval := params.SamplingIntervalOrDefault()
	corrIds := ctx.GetCorrelationIdsByNwdafSubId(nwdafSubId)
	return alignAndZipInMemory(corrIds, ctx, inputWindow, samplingInterval, snappedNow)
}

// globalBucket accumulates traffic values and snapped center timestamps for all
// IP sessions that are mapped to the same global time slot.
//
// centerSum / centerCount is used to compute the mean center timestamp for the
// output Ts, which reflects the true measurement time across IPs with different
// anchor offsets rather than forcing an artificial grid point.
type globalBucket struct {
	agg         trafficPoint
	centerSum   int64 // sum of per-IP snapped center unix timestamps (seconds)
	centerCount int   // number of IP sessions that contributed to this slot
}

// alignAndZipInMemory reads the in-memory ring buffer for all corrIds belonging
// to an NWDAF subscription, aggregates the UPF traffic reports into a fixed-length
// time-slot sequence, and returns it ready for the ML inference service.
//
// # The alignment problem
//
// UPF reports arrive with a startTime set by the UPF itself. Even though every UE
// is supposed to report at a fixed interval (samplingInterval seconds), two sources
// of misalignment exist:
//
//  1. Anchor drift: each IP session's first report lands at an arbitrary sub-second
//     offset from the absolute Unix grid (e.g. t=0.3, 5.3, 10.3 instead of 0, 5, 10).
//     A naive floor(t/si)*si would assign a report at t=9.8 to slot 5 instead of 10.
//
//  2. Late joiners: an IP that subscribes much later than the rest of the group has
//     a completely different anchor. Using the raw sequence index to merge streams
//     would co-locate data from different real measurement periods.
//
// # Two-step alignment
//
// Step 1 — Per-IP anchor round (eliminates drift, provides dedup):
//
//	anchor := first report's Unix timestamp for this IP session
//	n      := round( (t - anchor) / si )          // nearest integer step
//	center := anchor + n*si                        // snapped to IP's own grid
//
// Because we round relative to the anchor, a report at t=9.8 with anchor=0.3 gives
// n=round(9.5/5)=2, center=0.3+10=10.3 — correctly placed in the third slot.
// The tolerance window is ±si/2 around each expected report time.
//
// The center value is used as the map key within each IP session: if two reports
// from the same session map to the same center (e.g. a duplicate or a retransmit),
// the later entry overwrites the earlier one (last-wins dedup).
//
// Step 2 — Global round (aligns IPs with different anchors):
//
//	globalIndex := round( (center - snappedNow) / si )
//
// snappedNow is the current inference time floored to the grid. The global index
// is negative for historical slots (e.g. -1 = one period before now). Two IP
// sessions whose per-IP centers differ by less than si/2 — because they started
// reporting at slightly different real times — will receive the same globalIndex
// and are therefore summed together into the correct output slot.
//
// # Output Ts
//
// Rather than forcing all slots to the absolute grid, the representative timestamp
// for each output slot is the mean of the contributing IPs' per-IP centers:
//
//	Ts = mean( center_ip  for all IPs in globalIndex )
//
// This preserves the true measurement time (e.g. 26s instead of 25s or 27s when
// two IPs contributed centers at 25 and 27).
//
// # Return value
//
// Returns the last min(inputWindow, available) slots in ascending time order.
// Returns nil when no data is available for any corrId.
func alignAndZipInMemory(
	corrIds []string,
	ctx *nwdaf_context.NWDAFContext,
	inputWindow, samplingInterval int,
	snappedNow time.Time,
) ([]TrafficObservation, string) {
	dnn := "internet"
	si := int64(samplingInterval)
	snappedNowUnix := snappedNow.Unix()

	// buckets maps globalIndex → accumulated traffic + center metadata.
	// Negative indices represent past slots (e.g. -1 = one period before snappedNow).
	buckets := make(map[int]*globalBucket)

	for _, corrId := range corrIds {
		for _, td := range ctx.GetAllTrafficDataForCorrelation(corrId) {
			td.Lock()
			if td.Dnn != "" {
				dnn = td.Dnn
			}
			if len(td.RawUpfData) == 0 {
				td.Unlock()
				continue
			}

			// anchor is this IP session's first report time, used as the reference
			// for all subsequent round operations (Step 1).
			anchorUnix := td.RawUpfData[0].Timestamp.Unix()

			// Step 1: snap every data point to the nearest anchor-relative grid
			// center and deduplicate within the same session.
			// key = anchorUnix + n*si  (exact integer, always a multiple of si
			// offset from this session's anchor)
			// If two reports round to the same key, the later one overwrites.
			perIP := make(map[int64]nwdaf_context.UpfDataPoint, len(td.RawUpfData))
			for _, dp := range td.RawUpfData {
				n := int64(math.Round(float64(dp.Timestamp.Unix()-anchorUnix) / float64(si)))
				centerUnix := anchorUnix + n*si
				if _, exists := perIP[centerUnix]; exists {
					anlfLog.Warnf("dedup collision ip=%s: t=%d and previous both snap to center=%d (anchor=%d si=%d), keeping later",
						td.IpAddress, dp.Timestamp.Unix(), centerUnix, anchorUnix, si)
				}
				perIP[centerUnix] = dp
			}
			td.Unlock() // release lock before touching shared buckets map

			// Step 2: map each session-local center to a global slot index and
			// accumulate into the shared bucket.
			for centerUnix, dp := range perIP {
				idx := int(math.Round(float64(centerUnix-snappedNowUnix) / float64(si)))
				b, ok := buckets[idx]
				if !ok {
					b = &globalBucket{}
					buckets[idx] = b
				}
				b.agg.TotalVol += float64(dp.TotalVolume)
				b.agg.UlVol += float64(dp.UlVolume)
				b.agg.DlVol += float64(dp.DlVolume)
				b.agg.TotalNbPkts += float64(dp.TotalNbOfPackets)
				b.agg.UlNbPkts += float64(dp.UlNbOfPackets)
				b.agg.DlNbPkts += float64(dp.DlNbOfPackets)
				b.agg.UlThr += dp.UlThroughput
				b.agg.DlThr += dp.DlThroughput
				b.agg.UlPktThr += dp.UlPacketThroughput
				b.agg.DlPktThr += dp.DlPacketThroughput
				b.centerSum += centerUnix // accumulated for mean Ts computation
				b.centerCount++
			}
		}
	}

	if len(buckets) == 0 {
		return nil, dnn
	}

	// Determine the continuous output range [startIdx, endIdx].
	// endIdx = the most recent slot with data.
	// startIdx = endIdx - inputWindow + 1, clamped to the earliest slot with data
	// so we do not emit leading zero-pads before any real observation exists.
	keys := make([]int, 0, len(buckets))
	for k := range buckets {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	endIdx := keys[len(keys)-1]
	startIdx := endIdx - inputWindow + 1
	if keys[0] > startIdx {
		startIdx = keys[0]
	}

	// Build the result slice over [startIdx, endIdx], filling gaps with zeros.
	// Zero-padded slots get a Ts derived from snappedNow so the sequence remains
	// evenly spaced for the ML model.
	outputLen := endIdx - startIdx + 1
	result := make([]TrafficObservation, outputLen)
	counts := make([]string, outputLen) // for debug log
	for i := range outputLen {
		idx := startIdx + i
		ts := time.Unix(snappedNowUnix+int64(idx)*si, 0).UTC()
		if b, ok := buckets[idx]; ok {
			// Mean center: integer-round the float average to the nearest second.
			meanCenter := int64(math.Round(float64(b.centerSum) / float64(b.centerCount)))
			ts = time.Unix(meanCenter, 0).UTC()
			result[i] = TrafficObservation{
				Ts:          ts.Format(time.RFC3339),
				TotalVol:    b.agg.TotalVol,
				UlVol:       b.agg.UlVol,
				DlVol:       b.agg.DlVol,
				TotalNbPkts: b.agg.TotalNbPkts,
				UlNbPkts:    b.agg.UlNbPkts,
				DlNbPkts:    b.agg.DlNbPkts,
				UlThr:       b.agg.UlThr,
				DlThr:       b.agg.DlThr,
				UlPktThr:    b.agg.UlPktThr,
				DlPktThr:    b.agg.DlPktThr,
			}
			counts[i] = strconv.Itoa(b.centerCount)
		} else {
			result[i] = TrafficObservation{Ts: ts.Format(time.RFC3339)}
			counts[i] = "0"
		}
	}
	if anlfLog.Logger.IsLevelEnabled(logrus.DebugLevel) {
		anlfLog.Debugf("Aggregated %d global slots from %d corrIds (inputWindow=%d) ipCount/slot:[%s]",
			len(result), len(corrIds), inputWindow, strings.Join(counts, ","))
	}
	return result, dnn
}

// inferenceTargetLabel returns a concise target description for log context:
// "group=<id>", "supi=<supi>", or "supis=[s1,s2,...]" for multi-SUPI subscriptions.
func inferenceTargetLabel(nwdafSubId string, ctx *nwdaf_context.NWDAFContext) string {
	resources := ctx.GetNwdafSubResources(nwdafSubId)
	if len(resources) == 0 {
		return ""
	}
	if resources[0].OriginalGroupId != "" {
		return "group=" + resources[0].OriginalGroupId
	}
	supis := make([]string, 0, len(resources))
	for _, r := range resources {
		if r.Supi != "" {
			supis = append(supis, r.Supi)
		}
	}
	switch len(supis) {
	case 0:
		return ""
	case 1:
		return "supi=" + supis[0]
	default:
		return "supis=[" + strings.Join(supis, ",") + "]"
	}
}

// isAccuracyMonitorEnabled checks if accuracy monitoring is configured and enabled
func isAccuracyMonitorEnabled() bool {
	cfg := factory.NwdafConfig
	return cfg != nil && cfg.Configuration != nil &&
		cfg.Configuration.Mtlf != nil && cfg.Configuration.Mtlf.Enabled &&
		cfg.Configuration.Mtlf.AccuracyMonitor != nil && cfg.Configuration.Mtlf.AccuracyMonitor.Enabled
}
