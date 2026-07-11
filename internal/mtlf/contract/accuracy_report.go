package contract

import "time"

// AccuracyReport is the MTLF policy input for one model generation and scope.
type AccuracyReport struct {
	ModelURL              string
	ScopeKey              string
	NwdafSubID            string
	Metrics               map[string]float64
	TrafficScale          float64
	PredictedTrafficScale float64
	SampleCount           int
	InferenceNum          int
	WindowStart           time.Time
	WindowEnd             time.Time
}
