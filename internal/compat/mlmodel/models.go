// Package mlmodel contains Release 18 ML Model Provision and Monitor wire
// models that are absent from the pinned free5GC OpenAPI dependency.
//
// Source: 3GPP TS 29.520 V18.13.0,
// TS29520_Nnwdaf_MLModelProvision.yaml and TS29520_Nnwdaf_MLModelMonitor.yaml.
package mlmodel

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/google/uuid"

	"github.com/free5gc/openapi/models"
)

// MLModelProvisionSubscription follows TS 29.520 V18.13.0
// NwdafMLModelProvSubsc. Raw JSON is retained by callers for lossless proxying.
type MLModelProvisionSubscription struct {
	MLEventSubscriptions []MLEventSubscription `json:"mLEventSubscs"`
	NotificationURI      string                `json:"notifUri"`
	MLEventNotifications []MLEventNotification `json:"mLEventNotifs,omitempty"`
	SupportedFeatures    string                `json:"suppFeats,omitempty"`
	NotificationID       string                `json:"notifCorreId,omitempty"`
	EventRequest         json.RawMessage       `json:"eventReq,omitempty"`
	FailureEventReports  []json.RawMessage     `json:"failEventReports,omitempty"`
}

type MLEventSubscription struct {
	MLEvent               models.NwdafEvent           `json:"mLEvent"`
	MLEventFilter         json.RawMessage             `json:"mLEventFilter"`
	TargetUE              *models.TargetUeInformation `json:"tgtUe,omitempty"`
	TargetPeriod          json.RawMessage             `json:"mLTargetPeriod,omitempty"`
	ExpiryTime            *string                     `json:"expiryTime,omitempty"`
	TimeModelNeeded       *string                     `json:"timeModelNeeded,omitempty"`
	ReportCondition       json.RawMessage             `json:"mlEvRepCon,omitempty"`
	ModelInteroperability string                      `json:"modelInterInfo,omitempty"`
	NFConsumerInfo        json.RawMessage             `json:"nfConsumerInfo,omitempty"`
	ModelProvisionExt     json.RawMessage             `json:"modelProvExt,omitempty"`
	UseCaseContext        string                      `json:"useCaseCxt,omitempty"`
	InferenceData         json.RawMessage             `json:"inferDataForModel,omitempty"`
	ModelID               *int64                      `json:"modelId,omitempty"`
}

type MLModelProvisionNotification struct {
	EventNotifications []MLEventNotification `json:"eventNotifs"`
	SubscriptionID     string                `json:"subscriptionId"`
}

type MLEventNotification struct {
	Event               models.NwdafEvent           `json:"event"`
	NotificationID      string                      `json:"notifCorreId,omitempty"`
	MLFile              string                      `json:"mlFile,omitempty"`
	MLFileAddress       *MLModelAddress             `json:"mLFileAddr,omitempty"`
	MLModelADRF         json.RawMessage             `json:"mLModelAdrf,omitempty"`
	ValidityPeriod      json.RawMessage             `json:"validityPeriod,omitempty"`
	SpatialValidity     json.RawMessage             `json:"spatialValidity,omitempty"`
	AdditionalModelInfo []json.RawMessage           `json:"addModelInfo,omitempty"`
	ModelUniqueID       *int64                      `json:"modelUniqueId,omitempty"`
	UseCaseContext      string                      `json:"useCaseCxt,omitempty"`
	MLEventFilter       json.RawMessage             `json:"mLEventFilter,omitempty"`
	TargetUE            *models.TargetUeInformation `json:"tgtUe,omitempty"`
}

type MLModelAddress struct {
	MLModelURL string `json:"mLModelUrl,omitempty"`
	MLFileFQDN string `json:"mlFileFqdn,omitempty"`
}

type MLModelMonitorRegistration struct {
	ConsumerID       string                      `json:"consumerId,omitempty"`
	ConsumerSetID    string                      `json:"consumerSetId,omitempty"`
	ModelID          *int64                      `json:"modelId"`
	ModelAccuracyInd *bool                       `json:"modelAccuInd,omitempty"`
	MLEvent          models.NwdafEvent           `json:"mLEvent,omitempty"`
	MLEventFilter    json.RawMessage             `json:"mLEventFilter,omitempty"`
	TargetUE         *models.TargetUeInformation `json:"tgtUe,omitempty"`
}

type MLModelMonitorSubscription struct {
	ModelIDs           []int64                     `json:"modelIds"`
	NotificationURI    string                      `json:"notificationUri"`
	NotificationID     string                      `json:"notifCorrId"`
	ModelMetric        string                      `json:"modelMetric,omitempty"`
	AccuracyThreshold  *int64                      `json:"accuThreshold,omitempty"`
	EventReportRequest json.RawMessage             `json:"eventReportReq,omitempty"`
	ImmediateReport    *MLModelMonitorNotification `json:"immReport,omitempty"`
	MLEvent            models.NwdafEvent           `json:"mLEvent,omitempty"`
	MLEventFilter      json.RawMessage             `json:"mLEventFilter,omitempty"`
	TargetUE           *models.TargetUeInformation `json:"tgtUe,omitempty"`
	SupportedFeatures  string                      `json:"suppFeat,omitempty"`
}

type MLModelMonitorNotification struct {
	NotificationID    string                      `json:"notifCorrId"`
	ModelAccuracyInfo []MLModelAccuracyInfo       `json:"modelAccuInfos,omitempty"`
	AnalyticsFeedback []AnalyticsFeedback         `json:"anaFeedbacks,omitempty"`
	AccuracyMet       *bool                       `json:"accuMeetInd,omitempty"`
	MLEvent           models.NwdafEvent           `json:"mLEvent,omitempty"`
	MLEventFilter     json.RawMessage             `json:"mLEventFilter,omitempty"`
	TargetUE          *models.TargetUeInformation `json:"tgtUe,omitempty"`
}

type MLModelAccuracyInfo struct {
	ModelID         *int64          `json:"modelId"`
	Deviation       *float32        `json:"deviation,omitempty"`
	InferenceCount  *int64          `json:"inferenceNum,omitempty"`
	ADRFID          string          `json:"adrfId,omitempty"`
	ADRFSetID       string          `json:"adrfSetId,omitempty"`
	DataSetTag      json.RawMessage `json:"dataSetTag,omitempty"`
	ModelMetric     string          `json:"modelMetric,omitempty"`
	MLModelAccuracy *int64          `json:"mlModelAcc,omitempty"`
	MonitorInterval json.RawMessage `json:"monitorInterval,omitempty"`
}

type AnalyticsFeedback struct {
	Events           []models.NwdafEvent `json:"events"`
	ModelIDs         []int64             `json:"modelIds"`
	GroundDataImpact *bool               `json:"groundDataImpactInd,omitempty"`
	Timestamp        *string             `json:"timeStamp,omitempty"`
}

func ParseMLModelProvisionSubscription(body []byte) (*MLModelProvisionSubscription, error) {
	var value MLModelProvisionSubscription
	if err := decodeObject(body, &value); err != nil {
		return nil, err
	}
	if len(value.MLEventSubscriptions) == 0 {
		return nil, errors.New("mLEventSubscs must contain at least one item")
	}
	if strings.TrimSpace(value.NotificationURI) == "" {
		return nil, errors.New("notifUri is required")
	}
	if err := validateHTTPURI(value.NotificationURI); err != nil {
		return nil, fmt.Errorf("notifUri: %w", err)
	}
	for index, event := range value.MLEventSubscriptions {
		if event.MLEvent == "" {
			return nil, fmt.Errorf("mLEventSubscs[%d].mLEvent is required", index)
		}
		if !rawValuePresent(event.MLEventFilter) {
			return nil, fmt.Errorf("mLEventSubscs[%d].mLEventFilter is required", index)
		}
		if event.ModelID != nil && *event.ModelID < 0 {
			return nil, fmt.Errorf("mLEventSubscs[%d].modelId must be non-negative", index)
		}
	}
	if value.MLEventNotifications != nil && len(value.MLEventNotifications) == 0 {
		return nil, errors.New("mLEventNotifs must contain at least one item when present")
	}
	if value.FailureEventReports != nil && len(value.FailureEventReports) == 0 {
		return nil, errors.New("failEventReports must contain at least one item when present")
	}
	return &value, nil
}

func ParseMLModelProvisionNotifications(body []byte) ([]MLModelProvisionNotification, error) {
	var values []MLModelProvisionNotification
	if err := json.Unmarshal(body, &values); err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, errors.New("notification array must contain at least one item")
	}
	for index, value := range values {
		if strings.TrimSpace(value.SubscriptionID) == "" {
			return nil, fmt.Errorf("notifications[%d].subscriptionId is required", index)
		}
		if len(value.EventNotifications) == 0 {
			return nil, fmt.Errorf("notifications[%d].eventNotifs must contain at least one item", index)
		}
		for eventIndex, event := range value.EventNotifications {
			if err := validateMLEventNotification(event); err != nil {
				return nil, fmt.Errorf("notifications[%d].eventNotifs[%d]: %w", index, eventIndex, err)
			}
		}
	}
	return values, nil
}

func ParseMLModelMonitorRegistration(body []byte) (*MLModelMonitorRegistration, error) {
	var value MLModelMonitorRegistration
	if err := decodeObject(body, &value); err != nil {
		return nil, err
	}
	if value.ModelID == nil || *value.ModelID < 0 {
		return nil, errors.New("modelId is required and must be non-negative")
	}
	consumerID := strings.TrimSpace(value.ConsumerID) != ""
	consumerSetID := strings.TrimSpace(value.ConsumerSetID) != ""
	if consumerID == consumerSetID {
		return nil, errors.New("exactly one of consumerId or consumerSetId is required")
	}
	if consumerID {
		parsedID, err := uuid.Parse(value.ConsumerID)
		if err != nil || parsedID.Version() != 4 {
			return nil, errors.New("consumerId must be a UUIDv4 NF instance ID")
		}
	}
	return &value, nil
}

func ParseMLModelMonitorSubscription(body []byte) (*MLModelMonitorSubscription, error) {
	var value MLModelMonitorSubscription
	if err := decodeObject(body, &value); err != nil {
		return nil, err
	}
	if len(value.ModelIDs) == 0 {
		return nil, errors.New("modelIds must contain at least one item")
	}
	for index, modelID := range value.ModelIDs {
		if modelID < 0 {
			return nil, fmt.Errorf("modelIds[%d] must be non-negative", index)
		}
	}
	if strings.TrimSpace(value.NotificationURI) == "" {
		return nil, errors.New("notificationUri is required")
	}
	if err := validateHTTPURI(value.NotificationURI); err != nil {
		return nil, fmt.Errorf("notificationUri: %w", err)
	}
	if strings.TrimSpace(value.NotificationID) == "" {
		return nil, errors.New("notifCorrId is required")
	}
	if value.AccuracyThreshold != nil && *value.AccuracyThreshold < 0 {
		return nil, errors.New("accuThreshold must be non-negative")
	}
	if value.ImmediateReport != nil {
		if err := validateMLModelMonitorNotification(*value.ImmediateReport); err != nil {
			return nil, fmt.Errorf("immReport: %w", err)
		}
	}
	return &value, nil
}

func ParseMLModelMonitorNotification(body []byte) (*MLModelMonitorNotification, error) {
	var value MLModelMonitorNotification
	if err := decodeObject(body, &value); err != nil {
		return nil, err
	}
	if err := validateMLModelMonitorNotification(value); err != nil {
		return nil, err
	}
	return &value, nil
}

// ReplaceStringField changes one top-level string field while retaining every
// other known or unknown JSON member as raw JSON.
func ReplaceStringField(body []byte, field, value string) ([]byte, error) {
	var object map[string]json.RawMessage
	if err := decodeObject(body, &object); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	object[field] = encoded
	return json.Marshal(object)
}

func validateMLEventNotification(value MLEventNotification) error {
	if value.Event == "" {
		return errors.New("event is required")
	}
	hasAddress := value.MLFileAddress != nil
	hasADRF := rawValuePresent(value.MLModelADRF)
	if hasAddress == hasADRF {
		return errors.New("exactly one of mLFileAddr or mLModelAdrf is required")
	}
	if hasAddress {
		hasURL := strings.TrimSpace(value.MLFileAddress.MLModelURL) != ""
		hasFQDN := strings.TrimSpace(value.MLFileAddress.MLFileFQDN) != ""
		if hasURL == hasFQDN {
			return errors.New("mLFileAddr requires exactly one of mLModelUrl or mlFileFqdn")
		}
		if hasURL {
			if err := validateHTTPURI(value.MLFileAddress.MLModelURL); err != nil {
				return fmt.Errorf("mLFileAddr.mLModelUrl: %w", err)
			}
		}
	}
	if value.ModelUniqueID != nil && *value.ModelUniqueID < 0 {
		return errors.New("modelUniqueId must be non-negative")
	}
	return nil
}

func validateMLModelMonitorNotification(value MLModelMonitorNotification) error {
	if strings.TrimSpace(value.NotificationID) == "" {
		return errors.New("notifCorrId is required")
	}
	hasAccuracy := len(value.ModelAccuracyInfo) > 0
	hasFeedback := len(value.AnalyticsFeedback) > 0
	if !hasAccuracy && !hasFeedback {
		return errors.New("at least one non-empty modelAccuInfos or anaFeedbacks array is required")
	}
	for index, info := range value.ModelAccuracyInfo {
		if info.ModelID == nil || *info.ModelID < 0 {
			return fmt.Errorf("modelAccuInfos[%d].modelId is required and must be non-negative", index)
		}
		if info.InferenceCount != nil && *info.InferenceCount < 0 {
			return fmt.Errorf("modelAccuInfos[%d].inferenceNum must be non-negative", index)
		}
		if info.MLModelAccuracy != nil && (*info.MLModelAccuracy < 0 || *info.MLModelAccuracy > 100) {
			return fmt.Errorf("modelAccuInfos[%d].mlModelAcc must be between 0 and 100", index)
		}
		if strings.TrimSpace(info.ADRFID) != "" && strings.TrimSpace(info.ADRFSetID) != "" {
			return fmt.Errorf("modelAccuInfos[%d] cannot contain both adrfId and adrfSetId", index)
		}
	}
	for index, feedback := range value.AnalyticsFeedback {
		if len(feedback.Events) == 0 || len(feedback.ModelIDs) == 0 {
			return fmt.Errorf("anaFeedbacks[%d] requires non-empty events and modelIds", index)
		}
		for _, modelID := range feedback.ModelIDs {
			if modelID < 0 {
				return fmt.Errorf("anaFeedbacks[%d].modelIds must be non-negative", index)
			}
		}
	}
	return nil
}

func decodeObject(body []byte, target any) error {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) || trimmed[0] != '{' {
		return errors.New("request body must be a JSON object")
	}
	return json.Unmarshal(trimmed, target)
}

func rawValuePresent(value json.RawMessage) bool {
	trimmed := bytes.TrimSpace(value)
	return len(trimmed) > 0 && !bytes.Equal(trimmed, []byte("null"))
}

func validateHTTPURI(value string) error {
	parsed, err := url.ParseRequestURI(strings.TrimSpace(value))
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return errors.New("must be an absolute HTTP(S) URI")
	}
	return nil
}
