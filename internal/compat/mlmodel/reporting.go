package mlmodel

import (
	"errors"
	"fmt"
	"time"

	"github.com/free5gc/openapi/models"
)

// ReportingInformation follows TS 29.523 V18.7.0 ReportingInformation.
// Pointer scalars preserve the distinction between an omitted field and an
// explicitly supplied zero value.
type ReportingInformation struct {
	ImmediateReport             *bool                         `json:"immRep,omitempty"`
	NotificationMethod          *string                       `json:"notifMethod,omitempty"`
	MaximumReportCount          *int64                        `json:"maxReportNbr,omitempty"`
	MonitoringDuration          *time.Time                    `json:"monDur,omitempty"`
	RepetitionPeriod            *int64                        `json:"repPeriod,omitempty"`
	SamplingRatio               *int64                        `json:"sampRatio,omitempty"`
	PartitionCriteria           []models.PartitioningCriteria `json:"partitionCriteria,omitempty"`
	GroupReportingTime          *int64                        `json:"grpRepTime,omitempty"`
	NotificationFlag            *string                       `json:"notifFlag,omitempty"`
	NotificationFlagInstruction *MutingExceptionInstructions  `json:"notifFlagInstruct,omitempty"`
	MutingSetting               *MutingNotificationsSettings  `json:"mutingSetting,omitempty"`
}

type MutingExceptionInstructions struct {
	BufferedNotifications *string `json:"bufferedNotifs,omitempty"`
	Subscription          *string `json:"subscription,omitempty"`
}

type MutingNotificationsSettings struct {
	MaximumNotificationCount *int64 `json:"maxNoOfNotif,omitempty"`
	BufferedDuration         *int64 `json:"durationBufferedNotif,omitempty"`
}

func ValidateReportingInformation(value *ReportingInformation) error {
	if value == nil {
		return nil
	}
	if err := validateNonNegative("maxReportNbr", value.MaximumReportCount); err != nil {
		return err
	}
	if err := validateNonNegative("repPeriod", value.RepetitionPeriod); err != nil {
		return err
	}
	if err := validateNonNegative("grpRepTime", value.GroupReportingTime); err != nil {
		return err
	}
	if value.SamplingRatio != nil && (*value.SamplingRatio < 0 || *value.SamplingRatio > 100) {
		return errors.New("sampRatio must be between 0 and 100")
	}
	if value.PartitionCriteria != nil && len(value.PartitionCriteria) == 0 {
		return errors.New("partitionCriteria must contain at least one item when present")
	}
	if value.MutingSetting != nil {
		if err := validateNonNegative(
			"mutingSetting.maxNoOfNotif",
			value.MutingSetting.MaximumNotificationCount,
		); err != nil {
			return err
		}
		if err := validateNonNegative(
			"mutingSetting.durationBufferedNotif",
			value.MutingSetting.BufferedDuration,
		); err != nil {
			return err
		}
	}
	return nil
}

func validateNonNegative(name string, value *int64) error {
	if value != nil && *value < 0 {
		return fmt.Errorf("%s must be non-negative", name)
	}
	return nil
}
