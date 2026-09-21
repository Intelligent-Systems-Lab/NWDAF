// Package mlmodeltraining contains Release 18 Nnwdaf_MLModelTraining wire
// models that are absent from the pinned free5GC OpenAPI dependency, plus the
// project candidate extensions declared in candidate.go.
//
// Source: 3GPP TS 29.520 V18.14.0,
// TS29520_Nnwdaf_MLModelTraining.yaml.
package mlmodeltraining

import (
	"encoding/json"

	"github.com/free5gc/nwdaf/internal/compat/mlmodel"
	"github.com/free5gc/openapi/models"
)

type NwdafMLModelTrainSubsc struct {
	MLEventSubscriptions      []mlmodel.MLEventSubscription     `json:"mLEventSubscs"`
	NotificationURI           string                            `json:"notifUri"`
	SupportedFeatures         string                            `json:"suppFeats,omitempty"`
	EventRequest              *mlmodel.ReportingInformation     `json:"eventReq,omitempty"`
	FailureEventReports       []FailureEventInfoForMLModelTrain `json:"failEventReports,omitempty"`
	MLCorrelationID           string                            `json:"mlCorreId,omitempty"`
	MLModelInfos              []mlmodel.MLEventNotification     `json:"mLModelInfos,omitempty"`
	ImmediateReport           *NwdafMLModelTrainNotif           `json:"immReport,omitempty"`
	MLModelTrainingInfos      []MLModelTrainInfo                `json:"mLModelTrainInfos,omitempty"`
	MLPreparationFlag         *bool                             `json:"mLPreFlag,omitempty"`
	MLAccuracyCheckFlag       *bool                             `json:"mLAccChkFlg,omitempty"`
	MLTrainingReportInfo      *MLTrainReportInfo                `json:"mLTrainRepInfo,omitempty"`
	NotificationCorrelationID string                            `json:"notifCorreId"`
	RoundIndicator            *int64                            `json:"roundInd,omitempty"`
	TargetReportingUE         *models.TargetUeInformation       `json:"tgtRepUe,omitempty"`
	SkipFLIndicator           *bool                             `json:"skipFlInd,omitempty"`
	FLTopology                *FlTopologyNode                   `json:"flTopology,omitempty"`
	RetainedResultRequest     *bool                             `json:"retainedResultReq,omitempty"`
}

type NwdafMLModelTrainSubscPatch struct {
	NotificationURI              *string                       `json:"notifUri,omitempty"`
	EventRequest                 *mlmodel.ReportingInformation `json:"eventReq,omitempty"`
	MLModelInfos                 []mlmodel.MLEventNotification `json:"mLModelInfos,omitempty"`
	MLModelTrainingInfos         []MLModelTrainInfo            `json:"mLModelTrainInfos,omitempty"`
	MLPreparationFlag            *bool                         `json:"mLPreFlag,omitempty"`
	MLAccuracyCheckFlag          *bool                         `json:"mLAccChkFlg,omitempty"`
	MLTrainingReportInfo         *MLTrainReportInfo            `json:"mLTrainRepInfo,omitempty"`
	RoundIndicator               *int64                        `json:"roundInd,omitempty"`
	TargetReportingUE            *models.TargetUeInformation   `json:"tgtRepUe,omitempty"`
	SkipFLIndicator              *bool                         `json:"skipFlInd,omitempty"`
	FLTopology                   *FlTopologyNode               `json:"flTopology,omitempty"`
	RetainedResultRequest        *bool                         `json:"retainedResultReq,omitempty"`
	flTopologyPresent            bool
	retainedResultRequestPresent bool
	rawBody                      json.RawMessage
}

type NwdafMLModelTrainNotif struct {
	DelayEventNotification    *DelayEventNotif              `json:"delayEventNotif,omitempty"`
	MLCorrelationID           string                        `json:"mlCorreId,omitempty"`
	MLModelInfos              []mlmodel.MLEventNotification `json:"mLModelInfos,omitempty"`
	NotificationCorrelationID string                        `json:"notifCorreId"`
	RoundIndicator            *int64                        `json:"roundInd,omitempty"`
	StatusReport              *StatusReportInfo             `json:"statusReport,omitempty"`
	TerminationRequest        TermTrainCause                `json:"termTrainReq,omitempty"`
	FLTopologyReport          *FlTopologyReport             `json:"flTopologyReport,omitempty"`
	RetainedResultStatus      string                        `json:"retainedResultStatus,omitempty"`
}

type MLModelTrainInfo struct {
	DataAvailabilityRequirement *DataAvReq `json:"dataAvReq,omitempty"`
	TimeAvailabilityRequirement *string    `json:"timeAvReq,omitempty"`
}

type MLTrainReportInfo struct {
	MaximumResponseTime *int64 `json:"maxResTime,omitempty"`
}

type FailureEventInfoForMLModelTrain struct {
	MLTrainingEvent models.NwdafEvent `json:"mLTrainEvent"`
	FailureCode     FailureCodeTrain  `json:"failureCodeTrain"`
}

type DataAvReq struct {
	DataStatisticalProperties []models.DatasetStatisticalProperty `json:"dataStatProps,omitempty"`
	InputEvents               []DCCFEvent                         `json:"inpEvents"`
	MinimumSampleCount        *int64                              `json:"minNumSamples,omitempty"`
	TimeWindows               []models.TimeWindow                 `json:"timeWindows,omitempty"`
}

// DCCFEvent is the Release 18 DccfEvent union. String-valued event fields are
// represented as pointers so future enum values remain wire-compatible.
type DCCFEvent struct {
	NWDAFEvent *models.NwdafEvent `json:"nwdafEvent,omitempty"`
	SMFEvent   *string            `json:"smfEvent,omitempty"`
	AMFEvent   *string            `json:"amfEvent,omitempty"`
	NEFEvent   *string            `json:"nefEvent,omitempty"`
	UDMEvent   *string            `json:"udmEvent,omitempty"`
	AFEvent    *string            `json:"afEvent,omitempty"`
	SACEvent   json.RawMessage    `json:"sacEvent,omitempty"`
	NRFEvent   *string            `json:"nrfEvent,omitempty"`
	GMLCEvent  *string            `json:"gmlcEvent,omitempty"`
	UPFEvent   *string            `json:"upfEvent,omitempty"`
}

type DelayEventNotif struct {
	DelayEventIndicator    *bool      `json:"delayEventInd"`
	DelayCause             DelayCause `json:"delayCause,omitempty"`
	ExpectedCompletionTime *int64     `json:"expCompTime,omitempty"`
}

type StatusReportInfo struct {
	MLModelAccuracy  *int64         `json:"mlModelAcc,omitempty"`
	TrainingDataInfo *TrainDataInfo `json:"trainInDataInfo,omitempty"`
}

type TrainDataInfo struct {
	AreaInformation *models.NetworkAreaInfo `json:"areaInfo,omitempty"`
	MaximumValues   []string                `json:"maxValues,omitempty"`
	MinimumValues   []string                `json:"minValues,omitempty"`
	SamplingRatio   *int64                  `json:"samplRatio,omitempty"`
}

type FailureCodeTrain string

const FailureCodeTrainUnavailable FailureCodeTrain = "UNAVAILABLE_ML_MODEL_TRAIN"

type TermTrainCause string

const (
	TermTrainCauseNWDAFOverload       TermTrainCause = "NWDAF_OVERLOAD"
	TermTrainCauseNotAvailableMLTrain TermTrainCause = "NOT_AVAILABLE_ML_TRAIN"
	TermTrainCauseOthers              TermTrainCause = "OTHERS"
)

type DelayCause string

const (
	DelayCauseMLModelTrainFailure DelayCause = "ML_MODEL_TRAIN_FAILURE"
	DelayCauseNeedMoreTime        DelayCause = "NEED_MORE_TIME"
	DelayCauseOthers              DelayCause = "OTHERS"
)

const (
	CauseMLModelTrainingRequirementsNotMet      = "ML_MODEL_TRAINING_REQS_NOT_MET"
	CauseMLTrainingNotComplete                  = "ML_TRAINING_NOT_COMPLETE"
	CauseOverload                               = "OVERLOAD"
	CauseNotAvailableForFLProcessAnymore        = "NOT_AVAILABLE_FOR_FL_PROCESS_ANYMORE"
	CauseUnavailableMLModelTrainingForAllEvents = "UNAVAILABLE_ML_MODEL_TRAINING_FOR_ALLEVENTS"
)

type TrainingResourceIdentity struct {
	SubscriptionID               string
	MLCorrelationID              string
	NotificationCorrelationID    string
	ExpectedRoundIndicator       *int64
	NotificationMethod           *string
	BoundParticipantNFInstanceID string
}

type InvalidParameter struct {
	Parameter string
	Reason    string
}

type RequirementsError struct {
	Violations []InvalidParameter
}

type InvalidMessageError struct {
	Violations []InvalidParameter
}

func (e *InvalidMessageError) Error() string {
	return "invalid hierarchical FL message"
}

func (e *RequirementsError) Error() string {
	return CauseMLModelTrainingRequirementsNotMet
}
