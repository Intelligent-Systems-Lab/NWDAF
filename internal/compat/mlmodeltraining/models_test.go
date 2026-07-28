package mlmodeltraining

import (
	"encoding/json"
	"strings"
	"testing"
)

const validPreparationSubscription = `{
	"mLEventSubscs":[{
		"mLEvent":"UE_COMMUNICATION",
		"mLEventFilter":{
			"networkArea":{
				"tais":[{
					"plmnId":{"mcc":"466","mnc":"92"},
					"tac":"000001"
				}]
			}
		},
		"modelInterInfo":"pymtlf-model-bundle-v1"
	}],
	"notifUri":"http://nwdaf-c.example/training/callback",
	"notifCorreId":"prep-client-a",
	"mlCorreId":"fl-process-001",
	"mLPreFlag":true,
	"eventReq":{"immRep":true},
	"tgtRepUe":{"intGroupIds":["group-G"]},
	"mLModelTrainInfos":[{
		"dataAvReq":{
			"inpEvents":[{"upfEvent":"USER_DATA_USAGE_TRENDS"}],
			"minNumSamples":1000,
			"timeWindows":[{
				"startTime":"2026-07-01T00:00:00Z",
				"stopTime":"2026-07-27T00:00:00Z"
			}]
		},
		"timeAvReq":"PT10M"
	}]
}`

func TestTrainingPreparationContract(t *testing.T) {
	t.Parallel()

	value, err := ParseNwdafMLModelTrainSubsc([]byte(validPreparationSubscription))
	if err != nil {
		t.Fatalf("ParseNwdafMLModelTrainSubsc() error = %v", err)
	}
	if validationErr := ValidateFLSubscription(value, nil); validationErr != nil {
		t.Fatalf("ValidateFLSubscription() error = %v", validationErr)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	for _, field := range []string{
		`"mLEventSubscs"`,
		`"notifCorreId"`,
		`"mlCorreId"`,
		`"mLPreFlag":true`,
		`"upfEvent"`,
	} {
		if !strings.Contains(string(encoded), field) {
			t.Fatalf("json.Marshal() dropped %s: %s", field, encoded)
		}
	}
}

func TestTrainingSubscriptionOpenAPIRequiredFields(t *testing.T) {
	t.Parallel()

	for _, body := range []string{
		`{"notifUri":"http://example.com/callback","notifCorreId":"corr"}`,
		`{"mLEventSubscs":[{"mLEvent":"UE_COMMUNICATION","mLEventFilter":{}}],"notifCorreId":"corr"}`,
		`{"mLEventSubscs":[{"mLEvent":"UE_COMMUNICATION","mLEventFilter":{}}],"notifUri":"http://example.com/callback"}`,
		`{"mLEventSubscs":[{"mLEvent":"UE_COMMUNICATION"}],"notifUri":"http://example.com/callback","notifCorreId":"corr"}`,
	} {
		if _, err := ParseNwdafMLModelTrainSubsc([]byte(body)); err == nil {
			t.Fatalf("body %s unexpectedly passed required-field validation", body)
		}
	}
}

func TestTrainingPatchPreservesExplicitFalseAndZero(t *testing.T) {
	t.Parallel()

	value, err := ParseNwdafMLModelTrainSubscPatch([]byte(`{
		"mLPreFlag":false,
		"mLAccChkFlg":false,
		"roundInd":0,
		"skipFlInd":false
	}`))
	if err != nil {
		t.Fatalf("ParseNwdafMLModelTrainSubscPatch() error = %v", err)
	}
	if value.MLPreparationFlag == nil || *value.MLPreparationFlag {
		t.Fatalf("mLPreFlag = %v, want explicit false", value.MLPreparationFlag)
	}
	if value.RoundIndicator == nil || *value.RoundIndicator != 0 {
		t.Fatalf("roundInd = %v, want explicit zero", value.RoundIndicator)
	}
}

func TestFinalValidationWireContract(t *testing.T) {
	t.Parallel()

	patch, err := ParseNwdafMLModelTrainSubscPatch([]byte(`{
		"mLAccChkFlg":true,
		"skipFlInd":true,
		"roundInd":3,
		"mLModelInfos":[{
			"event":"UE_COMMUNICATION",
			"mLFileAddr":{
				"mLModelUrl":"http://nwdaf-c.example/training/fl-process-001/rounds/2/global.tar.gz"
			}
		}]
	}`))
	if err != nil {
		t.Fatalf("ParseNwdafMLModelTrainSubscPatch() error = %v", err)
	}
	if patch.MLAccuracyCheckFlag == nil || !*patch.MLAccuracyCheckFlag {
		t.Fatalf("mLAccChkFlg = %v, want true", patch.MLAccuracyCheckFlag)
	}
	if patch.SkipFLIndicator == nil || !*patch.SkipFLIndicator {
		t.Fatalf("skipFlInd = %v, want true", patch.SkipFLIndicator)
	}
	if len(patch.MLModelInfos) != 1 {
		t.Fatalf("len(mLModelInfos) = %d, want 1", len(patch.MLModelInfos))
	}

	notification, err := ParseNwdafMLModelTrainNotif([]byte(`{
		"notifCorreId":"prep-client-a",
		"mlCorreId":"fl-process-001",
		"roundInd":3,
		"mLModelInfos":[{
			"event":"UE_COMMUNICATION",
			"mLFileAddr":{
				"mLModelUrl":"http://nwdaf-a.example/training/fl-process-001/rounds/3/accuracy-check.tar.gz"
			}
		}],
		"statusReport":{"mlModelAcc":92}
	}`))
	if err != nil {
		t.Fatalf("ParseNwdafMLModelTrainNotif() error = %v", err)
	}
	if notification.StatusReport == nil ||
		notification.StatusReport.MLModelAccuracy == nil ||
		*notification.StatusReport.MLModelAccuracy != 92 {
		t.Fatalf("statusReport.mlModelAcc = %#v, want 92", notification.StatusReport)
	}

	if _, parseErr := ParseNwdafMLModelTrainNotif([]byte(`{
		"notifCorreId":"prep-client-a",
		"mLModelInfos":[{
			"event":"UE_COMMUNICATION",
			"mLFileAddr":{"mLModelUrl":"http://nwdaf-a.example/model.tar.gz"}
		}],
		"statusReport":{"mlModelAcc":101}
	}`)); parseErr == nil {
		t.Fatal("mlModelAcc above 100 unexpectedly passed validation")
	}
}

func TestTrainingNotificationCombinationRules(t *testing.T) {
	t.Parallel()

	valid := []string{
		`{"notifCorreId":"corr","delayEventNotif":{"delayEventInd":true}}`,
		`{"notifCorreId":"corr","mLModelInfos":[{
			"event":"UE_COMMUNICATION",
			"mLFileAddr":{"mLModelUrl":"http://client.example/local-model"}
		}]}`,
		`{"notifCorreId":"corr","termTrainReq":"OTHERS"}`,
		`{"notifCorreId":"corr","mLModelInfos":[{
			"event":"UE_COMMUNICATION",
			"mLFileAddr":{"mLModelUrl":"http://client.example/local-model"}
		}],"termTrainReq":"OTHERS"}`,
	}
	for _, body := range valid {
		if _, err := ParseNwdafMLModelTrainNotif([]byte(body)); err != nil {
			t.Fatalf("valid notification %s error = %v", body, err)
		}
	}

	invalid := []string{
		`{"notifCorreId":"corr"}`,
		`{"notifCorreId":"corr","delayEventNotif":{"delayEventInd":true},"termTrainReq":"OTHERS"}`,
		`{"notifCorreId":"corr","delayEventNotif":{}}`,
		`{"notifCorreId":"corr","mLModelInfos":[]}`,
	}
	for _, body := range invalid {
		if _, err := ParseNwdafMLModelTrainNotif([]byte(body)); err == nil {
			t.Fatalf("invalid notification %s unexpectedly passed", body)
		}
	}
}

func TestDCCFEventRequiresExactlyOneMember(t *testing.T) {
	t.Parallel()

	for _, body := range []string{
		`{"inpEvents":[]}`,
		`{"inpEvents":[{}]}`,
		`{"inpEvents":[{"upfEvent":"A","smfEvent":"B"}]}`,
	} {
		subscription := []byte(`{
			"mLEventSubscs":[{
				"mLEvent":"UE_COMMUNICATION",
				"mLEventFilter":{},
				"modelInterInfo":"bundle-v1"
			}],
			"notifUri":"http://example.com/callback",
			"notifCorreId":"corr",
			"mLModelTrainInfos":[{"dataAvReq":` + body + `}]
		}`)
		if _, err := ParseNwdafMLModelTrainSubsc(subscription); err == nil {
			t.Fatalf("dataAvReq %s unexpectedly passed", body)
		}
	}
}
