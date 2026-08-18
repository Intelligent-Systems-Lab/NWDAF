package mlmodeltraining

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/free5gc/nwdaf/internal/compat/mlmodel"
)

const notificationMethodOnEventDetection = "ON_EVENT_DETECTION"

func ParseNwdafMLModelTrainSubsc(body []byte) (*NwdafMLModelTrainSubsc, error) {
	var value NwdafMLModelTrainSubsc
	if err := decodeObject(body, &value); err != nil {
		return nil, err
	}
	if err := validateSubscriptionShape(&value); err != nil {
		return nil, err
	}
	return &value, nil
}

func ParseNwdafMLModelTrainSubscPatch(body []byte) (*NwdafMLModelTrainSubscPatch, error) {
	var value NwdafMLModelTrainSubscPatch
	if err := decodeObject(body, &value); err != nil {
		return nil, err
	}
	if value.NotificationURI != nil {
		if err := validateHTTPURI(*value.NotificationURI); err != nil {
			return nil, fmt.Errorf("notifUri: %w", err)
		}
	}
	if err := mlmodel.ValidateReportingInformation(value.EventRequest); err != nil {
		return nil, fmt.Errorf("eventReq: %w", err)
	}
	if err := validateModelInfos(value.MLModelInfos, "mLModelInfos"); err != nil {
		return nil, err
	}
	if err := validateTrainingInfos(value.MLModelTrainingInfos); err != nil {
		return nil, err
	}
	if err := validateTrainReportInfo(value.MLTrainingReportInfo); err != nil {
		return nil, err
	}
	if err := validateNonNegative("roundInd", value.RoundIndicator); err != nil {
		return nil, err
	}
	return &value, nil
}

func ParseNwdafMLModelTrainNotif(body []byte) (*NwdafMLModelTrainNotif, error) {
	var value NwdafMLModelTrainNotif
	if err := decodeObject(body, &value); err != nil {
		return nil, err
	}
	if err := validateNotificationShape(&value); err != nil {
		return nil, err
	}
	return &value, nil
}

func ApplySubscriptionPatch(
	current *NwdafMLModelTrainSubsc,
	patch *NwdafMLModelTrainSubscPatch,
) (*NwdafMLModelTrainSubsc, error) {
	if current == nil || patch == nil {
		return nil, errors.New("current subscription and patch are required")
	}
	body, err := json.Marshal(current)
	if err != nil {
		return nil, err
	}
	var effective NwdafMLModelTrainSubsc
	if unmarshalErr := json.Unmarshal(body, &effective); unmarshalErr != nil {
		return nil, unmarshalErr
	}
	if patch.NotificationURI != nil {
		effective.NotificationURI = *patch.NotificationURI
	}
	if patch.EventRequest != nil {
		effective.EventRequest = patch.EventRequest
	}
	if patch.MLModelInfos != nil {
		effective.MLModelInfos = patch.MLModelInfos
	}
	if patch.MLModelTrainingInfos != nil {
		effective.MLModelTrainingInfos = patch.MLModelTrainingInfos
	}
	if patch.MLPreparationFlag != nil {
		effective.MLPreparationFlag = patch.MLPreparationFlag
	}
	if patch.MLAccuracyCheckFlag != nil {
		effective.MLAccuracyCheckFlag = patch.MLAccuracyCheckFlag
	}
	if patch.MLTrainingReportInfo != nil {
		effective.MLTrainingReportInfo = patch.MLTrainingReportInfo
	}
	if patch.RoundIndicator != nil {
		effective.RoundIndicator = patch.RoundIndicator
	}
	if patch.TargetReportingUE != nil {
		effective.TargetReportingUE = patch.TargetReportingUE
	}
	if patch.SkipFLIndicator != nil {
		effective.SkipFLIndicator = patch.SkipFLIndicator
	}
	effectiveBody, err := json.Marshal(effective)
	if err != nil {
		return nil, err
	}
	return ParseNwdafMLModelTrainSubsc(effectiveBody)
}

func ValidateFLSubscription(
	value *NwdafMLModelTrainSubsc,
	existing *TrainingResourceIdentity,
) error {
	if value == nil {
		return errors.New("subscription is required")
	}
	violations := make([]InvalidParameter, 0)
	if strings.TrimSpace(value.MLCorrelationID) == "" {
		violations = append(violations, InvalidParameter{
			Parameter: "mlCorreId",
			Reason:    "is required for federated learning",
		})
	}
	for index, event := range value.MLEventSubscriptions {
		if strings.TrimSpace(event.ModelInteroperability) == "" {
			violations = append(violations, InvalidParameter{
				Parameter: fmt.Sprintf("mLEventSubscs[%d].modelInterInfo", index),
				Reason:    "is required for federated learning",
			})
		}
	}
	if value.MLPreparationFlag != nil && *value.MLPreparationFlag {
		if len(value.MLModelTrainingInfos) == 0 {
			violations = append(violations, InvalidParameter{
				Parameter: "mLModelTrainInfos",
				Reason:    "is required for training preparation",
			})
		}
		for index, info := range value.MLModelTrainingInfos {
			if info.DataAvailabilityRequirement == nil {
				violations = append(violations, InvalidParameter{
					Parameter: fmt.Sprintf("mLModelTrainInfos[%d].dataAvReq", index),
					Reason:    "is required for training preparation",
				})
			}
			if info.TimeAvailabilityRequirement == nil ||
				strings.TrimSpace(*info.TimeAvailabilityRequirement) == "" {
				violations = append(violations, InvalidParameter{
					Parameter: fmt.Sprintf("mLModelTrainInfos[%d].timeAvReq", index),
					Reason:    "is required for training preparation",
				})
			}
		}
	}
	if value.MLTrainingReportInfo != nil &&
		(value.EventRequest == nil ||
			value.EventRequest.NotificationMethod == nil ||
			*value.EventRequest.NotificationMethod != notificationMethodOnEventDetection) {
		violations = append(violations, InvalidParameter{
			Parameter: "mLTrainRepInfo",
			Reason:    "requires eventReq.notifMethod ON_EVENT_DETECTION",
		})
	}
	if existing != nil {
		if value.MLCorrelationID != existing.MLCorrelationID {
			violations = append(violations, InvalidParameter{
				Parameter: "mlCorreId",
				Reason:    "must not change for an existing resource",
			})
		}
		if value.NotificationCorrelationID != existing.NotificationCorrelationID {
			violations = append(violations, InvalidParameter{
				Parameter: "notifCorreId",
				Reason:    "must match the existing resource",
			})
		}
	}
	if len(violations) > 0 {
		return &RequirementsError{Violations: violations}
	}
	return nil
}

func ValidateFLPatch(
	value *NwdafMLModelTrainSubscPatch,
	existing *TrainingResourceIdentity,
) error {
	if value == nil {
		return errors.New("patch is required")
	}
	if existing == nil || strings.TrimSpace(existing.MLCorrelationID) == "" {
		return errors.New("existing training resource identity is required")
	}
	effectiveNotificationMethod := existing.NotificationMethod
	if value.EventRequest != nil {
		effectiveNotificationMethod = value.EventRequest.NotificationMethod
	}
	if value.MLTrainingReportInfo != nil &&
		(effectiveNotificationMethod == nil ||
			*effectiveNotificationMethod != notificationMethodOnEventDetection) {
		return &RequirementsError{Violations: []InvalidParameter{{
			Parameter: "mLTrainRepInfo",
			Reason:    "requires the effective eventReq.notifMethod ON_EVENT_DETECTION",
		}}}
	}
	return nil
}

func ValidateFLNotification(
	value *NwdafMLModelTrainNotif,
	existing *TrainingResourceIdentity,
) error {
	if value == nil {
		return errors.New("notification is required")
	}
	if existing == nil {
		return errors.New("existing training resource identity is required")
	}
	violations := make([]InvalidParameter, 0)
	if value.MLCorrelationID != existing.MLCorrelationID {
		violations = append(violations, InvalidParameter{
			Parameter: "mlCorreId",
			Reason:    "must match the existing federated learning process",
		})
	}
	if value.NotificationCorrelationID != existing.NotificationCorrelationID {
		violations = append(violations, InvalidParameter{
			Parameter: "notifCorreId",
			Reason:    "must match the existing resource",
		})
	}
	if existing.ExpectedRoundIndicator != nil {
		if value.RoundIndicator == nil ||
			*value.RoundIndicator != *existing.ExpectedRoundIndicator {
			violations = append(violations, InvalidParameter{
				Parameter: "roundInd",
				Reason:    "must match the expected training round",
			})
		}
	}
	if len(violations) > 0 {
		return &RequirementsError{Violations: violations}
	}
	return nil
}

func validateSubscriptionShape(value *NwdafMLModelTrainSubsc) error {
	if len(value.MLEventSubscriptions) == 0 {
		return errors.New("mLEventSubscs must contain at least one item")
	}
	for index, event := range value.MLEventSubscriptions {
		if event.MLEvent == "" {
			return fmt.Errorf("mLEventSubscs[%d].mLEvent is required", index)
		}
		if !rawObjectPresent(event.MLEventFilter) {
			return fmt.Errorf("mLEventSubscs[%d].mLEventFilter is required", index)
		}
		if err := validateNonNegative(
			fmt.Sprintf("mLEventSubscs[%d].modelId", index),
			event.ModelID,
		); err != nil {
			return err
		}
	}
	if err := validateHTTPURI(value.NotificationURI); err != nil {
		return fmt.Errorf("notifUri: %w", err)
	}
	if strings.TrimSpace(value.NotificationCorrelationID) == "" {
		return errors.New("notifCorreId is required")
	}
	if err := mlmodel.ValidateReportingInformation(value.EventRequest); err != nil {
		return fmt.Errorf("eventReq: %w", err)
	}
	if value.FailureEventReports != nil && len(value.FailureEventReports) == 0 {
		return errors.New("failEventReports must contain at least one item when present")
	}
	for index, report := range value.FailureEventReports {
		if report.MLTrainingEvent == "" || report.FailureCode == "" {
			return fmt.Errorf(
				"failEventReports[%d] requires mLTrainEvent and failureCodeTrain",
				index,
			)
		}
	}
	if err := validateModelInfos(value.MLModelInfos, "mLModelInfos"); err != nil {
		return err
	}
	if value.ImmediateReport != nil {
		if err := validateNotificationShape(value.ImmediateReport); err != nil {
			return fmt.Errorf("immReport: %w", err)
		}
	}
	if err := validateTrainingInfos(value.MLModelTrainingInfos); err != nil {
		return err
	}
	if err := validateTrainReportInfo(value.MLTrainingReportInfo); err != nil {
		return err
	}
	return validateNonNegative("roundInd", value.RoundIndicator)
}

func validateNotificationShape(value *NwdafMLModelTrainNotif) error {
	if strings.TrimSpace(value.NotificationCorrelationID) == "" {
		return errors.New("notifCorreId is required")
	}
	hasDelay := value.DelayEventNotification != nil
	hasModels := len(value.MLModelInfos) > 0
	hasTermination := value.TerminationRequest != ""
	if !hasDelay && !hasModels && !hasTermination {
		return errors.New(
			"at least one of delayEventNotif, mLModelInfos or termTrainReq is required",
		)
	}
	if hasDelay && (hasModels || hasTermination) {
		return errors.New(
			"delayEventNotif cannot coexist with mLModelInfos or termTrainReq",
		)
	}
	if value.MLModelInfos != nil && !hasModels {
		return errors.New("mLModelInfos must contain at least one item when present")
	}
	if err := validateModelInfos(value.MLModelInfos, "mLModelInfos"); err != nil {
		return err
	}
	if value.DelayEventNotification != nil {
		if value.DelayEventNotification.DelayEventIndicator == nil {
			return errors.New("delayEventNotif.delayEventInd is required")
		}
		if err := validateNonNegative(
			"delayEventNotif.expCompTime",
			value.DelayEventNotification.ExpectedCompletionTime,
		); err != nil {
			return err
		}
	}
	if err := validateNonNegative("roundInd", value.RoundIndicator); err != nil {
		return err
	}
	if value.StatusReport != nil {
		if err := validateStatusReport(value.StatusReport); err != nil {
			return err
		}
	}
	return nil
}

func validateModelInfos(values []mlmodel.MLEventNotification, field string) error {
	if values != nil && len(values) == 0 {
		return fmt.Errorf("%s must contain at least one item when present", field)
	}
	for index, value := range values {
		if err := mlmodel.ValidateMLEventNotification(value); err != nil {
			return fmt.Errorf("%s[%d]: %w", field, index, err)
		}
	}
	return nil
}

func validateTrainingInfos(values []MLModelTrainInfo) error {
	if values != nil && len(values) == 0 {
		return errors.New("mLModelTrainInfos must contain at least one item when present")
	}
	for index, info := range values {
		if info.DataAvailabilityRequirement != nil {
			if err := validateDataAvailability(
				info.DataAvailabilityRequirement,
				fmt.Sprintf("mLModelTrainInfos[%d].dataAvReq", index),
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateDataAvailability(value *DataAvReq, path string) error {
	if len(value.InputEvents) == 0 {
		return fmt.Errorf("%s.inpEvents must contain at least one item", path)
	}
	for index, event := range value.InputEvents {
		if event.memberCount() != 1 {
			return fmt.Errorf("%s.inpEvents[%d] must select exactly one event", path, index)
		}
	}
	if value.DataStatisticalProperties != nil && len(value.DataStatisticalProperties) == 0 {
		return fmt.Errorf("%s.dataStatProps must contain at least one item when present", path)
	}
	if err := validateNonNegative(path+".minNumSamples", value.MinimumSampleCount); err != nil {
		return err
	}
	if value.TimeWindows != nil && len(value.TimeWindows) == 0 {
		return fmt.Errorf("%s.timeWindows must contain at least one item when present", path)
	}
	for index, window := range value.TimeWindows {
		if window.StartTime == nil || window.StopTime == nil {
			return fmt.Errorf("%s.timeWindows[%d] requires startTime and stopTime", path, index)
		}
		if window.StopTime.Before(*window.StartTime) {
			return fmt.Errorf("%s.timeWindows[%d] stopTime must not precede startTime", path, index)
		}
	}
	return nil
}

func validateTrainReportInfo(value *MLTrainReportInfo) error {
	if value == nil {
		return nil
	}
	return validateNonNegative("mLTrainRepInfo.maxResTime", value.MaximumResponseTime)
}

func validateStatusReport(value *StatusReportInfo) error {
	if err := validateNonNegative("statusReport.mlModelAcc", value.MLModelAccuracy); err != nil {
		return err
	}
	if value.MLModelAccuracy != nil && *value.MLModelAccuracy > 100 {
		return errors.New("statusReport.mlModelAcc must not exceed 100")
	}
	if value.TrainingDataInfo == nil {
		return nil
	}
	info := value.TrainingDataInfo
	if info.MaximumValues != nil && len(info.MaximumValues) == 0 {
		return errors.New("statusReport.trainInDataInfo.maxValues must be non-empty when present")
	}
	if info.MinimumValues != nil && len(info.MinimumValues) == 0 {
		return errors.New("statusReport.trainInDataInfo.minValues must be non-empty when present")
	}
	return validateNonNegative("statusReport.trainInDataInfo.samplRatio", info.SamplingRatio)
}

func (e DCCFEvent) memberCount() int {
	count := 0
	for _, present := range []bool{
		e.NWDAFEvent != nil,
		e.SMFEvent != nil,
		e.AMFEvent != nil,
		e.NEFEvent != nil,
		e.UDMEvent != nil,
		e.AFEvent != nil,
		rawPresent(e.SACEvent),
		e.NRFEvent != nil,
		e.GMLCEvent != nil,
		e.UPFEvent != nil,
	} {
		if present {
			count++
		}
	}
	return count
}

func decodeObject(body []byte, target any) error {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) || trimmed[0] != '{' {
		return errors.New("request body must be a JSON object")
	}
	return json.Unmarshal(trimmed, target)
}

func rawPresent(value json.RawMessage) bool {
	trimmed := bytes.TrimSpace(value)
	return len(trimmed) > 0 && !bytes.Equal(trimmed, []byte("null"))
}

func rawObjectPresent(value json.RawMessage) bool {
	trimmed := bytes.TrimSpace(value)
	return rawPresent(trimmed) && trimmed[0] == '{'
}

func validateNonNegative(name string, value *int64) error {
	if value != nil && *value < 0 {
		return fmt.Errorf("%s must be non-negative", name)
	}
	return nil
}

func validateHTTPURI(value string) error {
	parsed, err := url.ParseRequestURI(strings.TrimSpace(value))
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return errors.New("must be an absolute HTTP(S) URI")
	}
	return nil
}
