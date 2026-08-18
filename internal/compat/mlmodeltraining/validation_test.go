package mlmodeltraining

import (
	"errors"
	"testing"
)

func TestFLPreparationConditionalRequirements(t *testing.T) {
	t.Parallel()

	value, err := ParseNwdafMLModelTrainSubsc([]byte(`{
		"mLEventSubscs":[{
			"mLEvent":"UE_COMMUNICATION",
			"mLEventFilter":{}
		}],
		"notifUri":"http://example.com/callback",
		"notifCorreId":"corr",
		"mLPreFlag":true,
		"mLModelTrainInfos":[{}]
	}`))
	if err != nil {
		t.Fatalf("ParseNwdafMLModelTrainSubsc() error = %v", err)
	}
	err = ValidateFLSubscription(value, nil)
	var requirements *RequirementsError
	if !errors.As(err, &requirements) {
		t.Fatalf("ValidateFLSubscription() error = %v, want RequirementsError", err)
	}
	want := map[string]bool{
		"mlCorreId":                       false,
		"mLEventSubscs[0].modelInterInfo": false,
		"mLModelTrainInfos[0].dataAvReq":  false,
		"mLModelTrainInfos[0].timeAvReq":  false,
	}
	for _, violation := range requirements.Violations {
		if _, exists := want[violation.Parameter]; exists {
			want[violation.Parameter] = true
		}
	}
	for parameter, found := range want {
		if !found {
			t.Errorf("missing violation for %s: %#v", parameter, requirements.Violations)
		}
	}
}

func TestFLPutKeepsProcessIdentity(t *testing.T) {
	t.Parallel()

	value, err := ParseNwdafMLModelTrainSubsc([]byte(validPreparationSubscription))
	if err != nil {
		t.Fatal(err)
	}
	existing := &TrainingResourceIdentity{
		SubscriptionID:            "sub-1",
		MLCorrelationID:           "another-process",
		NotificationCorrelationID: "another-correlation",
	}
	err = ValidateFLSubscription(value, existing)
	var requirements *RequirementsError
	if !errors.As(err, &requirements) || len(requirements.Violations) != 2 {
		t.Fatalf("ValidateFLSubscription() error = %#v, want two identity violations", err)
	}
}

func TestFLNotificationMatchesProcessAndRound(t *testing.T) {
	t.Parallel()

	value, err := ParseNwdafMLModelTrainNotif([]byte(`{
		"notifCorreId":"round-client-a",
		"mlCorreId":"fl-process-001",
		"roundInd":2,
		"mLModelInfos":[{
			"event":"UE_COMMUNICATION",
			"mLFileAddr":{"mLModelUrl":"http://client.example/round-2"}
		}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	round := int64(2)
	existing := &TrainingResourceIdentity{
		SubscriptionID:            "sub-1",
		MLCorrelationID:           "fl-process-001",
		NotificationCorrelationID: "round-client-a",
		ExpectedRoundIndicator:    &round,
	}
	if validationErr := ValidateFLNotification(value, existing); validationErr != nil {
		t.Fatalf("ValidateFLNotification() error = %v", validationErr)
	}
	wrongRound := int64(3)
	value.RoundIndicator = &wrongRound
	if validationErr := ValidateFLNotification(value, existing); validationErr == nil {
		t.Fatal("wrong round unexpectedly passed validation")
	}
}

func TestFLPreparationStatusReportOnlyNotificationIsRejected(t *testing.T) {
	t.Parallel()

	_, err := ParseNwdafMLModelTrainNotif([]byte(`{
		"notifCorreId":"preparation-client-a",
		"mlCorreId":"fl-process-001",
		"statusReport":{
			"trainInDataInfo":{"samplRatio":100}
		}
	}`))
	if err == nil {
		t.Fatal("statusReport-only preparation notification unexpectedly passed validation")
	}
}

func TestTrainingReportInfoRequiresOnEventDetection(t *testing.T) {
	t.Parallel()

	value, err := ParseNwdafMLModelTrainSubsc([]byte(`{
		"mLEventSubscs":[{
			"mLEvent":"UE_COMMUNICATION",
			"mLEventFilter":{},
			"modelInterInfo":"bundle-v1"
		}],
		"notifUri":"http://example.com/callback",
		"notifCorreId":"corr",
		"mlCorreId":"fl-1",
		"eventReq":{"notifMethod":"PERIODIC"},
		"mLTrainRepInfo":{"maxResTime":30}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if validationErr := ValidateFLSubscription(value, nil); validationErr == nil {
		t.Fatal("periodic report with mLTrainRepInfo unexpectedly passed")
	}
	method := notificationMethodOnEventDetection
	value.EventRequest.NotificationMethod = &method
	if validationErr := ValidateFLSubscription(value, nil); validationErr != nil {
		t.Fatalf("ON_EVENT_DETECTION validation error = %v", validationErr)
	}
}

func TestTrainingPatchUsesEffectiveNotificationMethod(t *testing.T) {
	t.Parallel()

	value, err := ParseNwdafMLModelTrainSubscPatch([]byte(`{
		"mLTrainRepInfo":{"maxResTime":30}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	method := notificationMethodOnEventDetection
	existing := &TrainingResourceIdentity{
		SubscriptionID:     "sub-1",
		MLCorrelationID:    "fl-process-001",
		NotificationMethod: &method,
	}
	if validationErr := ValidateFLPatch(value, existing); validationErr != nil {
		t.Fatalf("ValidateFLPatch() error = %v", validationErr)
	}

	periodic := "PERIODIC"
	existing.NotificationMethod = &periodic
	if validationErr := ValidateFLPatch(value, existing); validationErr == nil {
		t.Fatal("effective PERIODIC method unexpectedly passed validation")
	}
}
