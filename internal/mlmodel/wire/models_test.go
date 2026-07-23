package wire

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/free5gc/openapi/models"
)

func TestPinnedGeneratedProvisionModelDropsRelease18Fields(t *testing.T) {
	t.Parallel()

	body := []byte(`{
		"mLEventSubscs":[{
			"mLEvent":"UE_COMMUNICATION",
			"mLEventFilter":{},
			"timeModelNeeded":"2026-07-22T12:00:00Z"
		}],
		"notifUri":"http://consumer.example/callback",
		"release18Extension":{"enabled":true}
	}`)
	var generated models.NwdafMlModelProvSubsc
	if err := json.Unmarshal(body, &generated); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	encoded, err := json.Marshal(generated)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if strings.Contains(string(encoded), "timeModelNeeded") ||
		strings.Contains(string(encoded), "release18Extension") {
		t.Fatalf("pinned generated model unexpectedly preserved Release 18 fields: %s", encoded)
	}
}

func TestProvisionSubscriptionRequiresRelease18EventFilter(t *testing.T) {
	t.Parallel()

	_, err := ParseMLModelProvisionSubscription([]byte(`{
		"mLEventSubscs":[{"mLEvent":"UE_COMMUNICATION"}],
		"notifUri":"http://consumer.example/callback"
	}`))
	if err == nil || !strings.Contains(err.Error(), "mLEventFilter") {
		t.Fatalf("error = %v, want required mLEventFilter", err)
	}
}

func TestProvisionSubscriptionAcceptsAndPreservesUnknownRelease18Fields(t *testing.T) {
	t.Parallel()

	body := []byte(`{
		"mLEventSubscs":[{
			"mLEvent":"UE_COMMUNICATION",
			"mLEventFilter":{},
			"timeModelNeeded":"2026-07-22T12:00:00Z",
			"futureEventField":{"enabled":true}
		}],
		"notifUri":"http://consumer.example/callback",
		"futureTopLevel":{"revision":18}
	}`)
	parsed, err := ParseMLModelProvisionSubscription(body)
	if err != nil {
		t.Fatalf("ParseMLModelProvisionSubscription() error = %v", err)
	}
	if parsed.MLEventSubscriptions[0].TimeModelNeeded == nil {
		t.Fatal("timeModelNeeded was not decoded")
	}
	patched, err := ReplaceStringField(body, "notifUri", "http://go.internal/callback")
	if err != nil {
		t.Fatalf("ReplaceStringField() error = %v", err)
	}
	var object map[string]json.RawMessage
	if err = json.Unmarshal(patched, &object); err != nil {
		t.Fatal(err)
	}
	if _, exists := object["futureTopLevel"]; !exists {
		t.Fatalf("patched body dropped unknown field: %s", patched)
	}
}

func TestMonitorRegistrationValidatesConsumerOneOf(t *testing.T) {
	t.Parallel()

	for _, body := range []string{
		`{"modelId":1}`,
		`{"consumerId":"11111111-1111-4111-8111-111111111111","consumerSetId":"set1.nwdafset.5gc.mnc001.mcc001","modelId":1}`,
	} {
		if _, err := ParseMLModelMonitorRegistration([]byte(body)); err == nil {
			t.Fatalf("body %s unexpectedly passed oneOf validation", body)
		}
	}
	if _, err := ParseMLModelMonitorRegistration([]byte(`{
		"consumerId":"11111111-1111-4111-8111-111111111111",
		"modelId":0,
		"futureField":"preserved-by-the-raw-proxy"
	}`)); err != nil {
		t.Fatalf("valid registration error = %v", err)
	}
}

func TestMonitorSubscriptionValidatesMandatoryFields(t *testing.T) {
	t.Parallel()

	valid := []byte(`{
		"modelIds":[0,2],
		"notificationUri":"http://consumer.example/monitor",
		"notifCorrId":"corr-1",
		"futureField":"preserved"
	}`)
	if _, err := ParseMLModelMonitorSubscription(valid); err != nil {
		t.Fatalf("valid subscription error = %v", err)
	}
	for _, body := range []string{
		`{"modelIds":[],"notificationUri":"http://consumer.example/monitor","notifCorrId":"corr-1"}`,
		`{"modelIds":[1],"notifCorrId":"corr-1"}`,
		`{"modelIds":[1],"notificationUri":"http://consumer.example/monitor"}`,
	} {
		if _, err := ParseMLModelMonitorSubscription([]byte(body)); err == nil {
			t.Fatalf("body %s unexpectedly passed validation", body)
		}
	}
}

func TestMonitorNotificationAllowsAccuracyWithoutDeviation(t *testing.T) {
	t.Parallel()

	for _, body := range []string{
		`{"notifCorrId":"corr-1","modelAccuInfos":[{"modelId":1,"inferenceNum":2}]}`,
		`{"notifCorrId":"corr-1","modelAccuInfos":[{"modelId":1,"deviation":1.4,"modelMetric":"ACCURACY"}]}`,
	} {
		if _, err := ParseMLModelMonitorNotification([]byte(body)); err != nil {
			t.Fatalf("body %s error = %v", body, err)
		}
	}
	for _, body := range []string{
		`{"notifCorrId":"corr-1"}`,
		`{"notifCorrId":"corr-1","modelAccuInfos":[]}`,
		`{"notifCorrId":"corr-1","modelAccuInfos":[{"deviation":0.2}]}`,
	} {
		if _, err := ParseMLModelMonitorNotification([]byte(body)); err == nil {
			t.Fatalf("body %s unexpectedly passed validation", body)
		}
	}
}

func TestProvisionNotificationValidatesCardinalityAndAddressOneOf(t *testing.T) {
	t.Parallel()

	valid := []byte(`[{
		"subscriptionId":"sub-1",
		"eventNotifs":[{
			"event":"UE_COMMUNICATION",
			"modelUniqueId":1,
			"mLFileAddr":{"mLModelUrl":"http://mtlf.example/models/1"}
		}]
	}]`)
	if _, err := ParseMLModelProvisionNotifications(valid); err != nil {
		t.Fatalf("valid notification error = %v", err)
	}
	invalid := []byte(`[{
		"subscriptionId":"sub-1",
		"eventNotifs":[{"event":"UE_COMMUNICATION"}]
	}]`)
	if _, err := ParseMLModelProvisionNotifications(invalid); err == nil {
		t.Fatal("notification without model address unexpectedly passed validation")
	}
}
