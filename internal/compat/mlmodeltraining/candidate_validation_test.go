package mlmodeltraining

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

const validCandidateSubscription = `{
	"mLEventSubscs":[{
		"mLEvent":"UE_COMMUNICATION",
		"mLEventFilter":{},
		"modelInterInfo":"pymtlf-model-bundle-v1"
	}],
	"notifUri":"http://root.example/callback",
	"notifCorreId":"root-branch-a",
	"suppFeats":"4",
	"mlCorreId":"hierarchical-fl-001",
	"mLPreFlag":true,
	"mLModelTrainInfos":[{
		"dataAvReq":{"inpEvents":[{"upfEvent":"USER_DATA_USAGE_TRENDS"}]},
		"timeAvReq":"PT10M"
	}],
	"x-flTopology":{
		"nfInstanceId":"10000000-0000-4000-8000-000000000001",
		"children":[{
			"nfInstanceId":"10000000-0000-4000-8000-000000000101",
			"priority":100
		}],
		"policy":{
			"allowAdditionalCandidates":true,
			"additionalCandidatePriority":0,
			"selectionMethod":"priority",
			"minAvailableNodes":1,
			"fractionTrain":1,
			"minTrainNodes":1,
			"acceptFailures":true,
			"minCompletionRate":0.5
		},
		"strategy":{
			"method":"fedProx",
			"aggregation":"sampleWeighted",
			"methodParameters":{"proximalMu":0.01}
		},
		"reportAfter":{"count":3,"unit":"round"}
	}
}`

func TestCandidateSubscriptionRoundTrip(t *testing.T) {
	t.Parallel()

	body := strings.Replace(
		validCandidateSubscription,
		`"mLPreFlag":true,`,
		`"mLPreFlag":true,"x-retainedResultReq":false,`,
		1,
	)
	body = strings.Replace(
		body,
		`"priority":100`,
		`"enabled":true,"priority":100,"retainedResultReq":false`,
		1,
	)
	value, err := ParseNwdafMLModelTrainSubsc([]byte(body))
	if err != nil {
		t.Fatalf("ParseNwdafMLModelTrainSubsc() error = %v", err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	assertCandidateJSONFieldsEqual(
		t,
		[]byte(body),
		encoded,
		"x-flTopology",
		"x-retainedResultReq",
	)
}

func TestCandidatePatchRoundTrip(t *testing.T) {
	t.Parallel()

	body := []byte(`{
		"x-retainedResultReq":true,
		"x-flTopology":{
			"nfInstanceId":"10000000-0000-4000-8000-000000000001",
			"enabled":true,
			"priority":100,
			"policy":{
				"allowAdditionalCandidates":true,
				"additionalCandidatePriority":5,
				"selectionMethod":"priority",
				"minAvailableNodes":2,
				"fractionTrain":0.5,
				"minTrainNodes":1,
				"acceptFailures":true,
				"minCompletionRate":0.5
			},
			"strategy":{
				"method":"fedProx",
				"aggregation":"sampleWeighted",
				"methodParameters":{"proximalMu":0.01}
			},
			"reportAfter":{"count":3,"unit":"round"},
			"retainedResultReq":true,
			"children":[{
				"nfInstanceId":"10000000-0000-4000-8000-000000000101",
				"enabled":false,
				"priority":80
			}]
		}
	}`)
	value, err := ParseNwdafMLModelTrainSubscPatch(body)
	if err != nil {
		t.Fatalf("ParseNwdafMLModelTrainSubscPatch() error = %v", err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	assertCandidateJSONFieldsEqual(
		t,
		body,
		encoded,
		"x-flTopology",
		"x-retainedResultReq",
	)
}

func TestCandidateNotificationRoundTrip(t *testing.T) {
	t.Parallel()

	body := []byte(`{
		"notifCorreId":"root-branch-a",
		"mlCorreId":"hierarchical-fl-001",
		"x-retainedResultStatus":"NOT_FOUND",
		"x-flTopologyReport":{
			"nfInstanceId":"10000000-0000-4000-8000-000000000001",
			"policy":{
				"allowAdditionalCandidates":true,
				"additionalCandidatePriority":5,
				"selectionMethod":"priority",
				"minAvailableNodes":2,
				"fractionTrain":0.5,
				"minTrainNodes":1,
				"acceptFailures":true,
				"minCompletionRate":0.5
			},
			"strategy":{
				"method":"fedProx",
				"aggregation":"sampleWeighted",
				"methodParameters":{"proximalMu":0.01}
			},
			"reportAfter":{"count":3,"unit":"round"},
			"children":[{
				"nfInstanceId":"10000000-0000-4000-8000-000000000101",
				"status":"ACTIVE",
				"statusTimestamp":"2026-09-02T06:29:10Z",
				"policy":{"minAvailableNodes":1,"minTrainNodes":1},
				"strategy":{
					"method":"fedProx",
					"aggregation":"sampleWeighted",
					"methodParameters":{"proximalMu":0.02}
				},
				"reportAfter":{"count":2,"unit":"round"},
				"children":[{
					"nfInstanceId":"10000000-0000-4000-8000-000000000201",
					"status":"FAILED",
					"statusTimestamp":"2026-09-02T06:30:10Z",
					"statusCause":"RESOURCE_UNAVAILABLE"
				}]
			}]
		}
	}`)
	value, err := ParseNwdafMLModelTrainNotif(body)
	if err != nil {
		t.Fatalf("ParseNwdafMLModelTrainNotif() error = %v", err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	assertCandidateJSONFieldsEqual(
		t,
		body,
		encoded,
		"x-flTopologyReport",
		"x-retainedResultStatus",
	)
}

func assertCandidateJSONFieldsEqual(t *testing.T, original, encoded []byte, fields ...string) {
	t.Helper()
	var want map[string]any
	if err := json.Unmarshal(original, &want); err != nil {
		t.Fatalf("decode original JSON: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("decode encoded JSON: %v", err)
	}
	for _, field := range fields {
		if !reflect.DeepEqual(got[field], want[field]) {
			t.Fatalf("candidate field %s changed during round trip:\ngot:  %#v\nwant: %#v", field, got[field], want[field])
		}
	}
}

func TestCandidateNestedObjectsAreClosed(t *testing.T) {
	t.Parallel()

	body := strings.Replace(
		validCandidateSubscription,
		`"proximalMu":0.01`,
		`"proximalMu":0.01,"unknown":true`,
		1,
	)
	_, err := ParseNwdafMLModelTrainSubsc([]byte(body))
	assertCandidateInvalidPath(t, err, "x-flTopology.strategy.methodParameters.unknown")
}

func TestCandidateTypeErrorsKeepAliasPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		old  string
		new  string
		path string
	}{
		{
			name: "node boolean",
			old:  `"priority":100`,
			new:  `"priority":100,"enabled":"yes"`,
			path: "x-flTopology.children[0].enabled",
		},
		{
			name: "policy number",
			old:  `"fractionTrain":1`,
			new:  `"fractionTrain":"all"`,
			path: "x-flTopology.policy.fractionTrain",
		},
		{
			name: "method parameter",
			old:  `"proximalMu":0.01`,
			new:  `"proximalMu":"small"`,
			path: "x-flTopology.strategy.methodParameters.proximalMu",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			body := strings.Replace(validCandidateSubscription, test.old, test.new, 1)
			_, err := ParseNwdafMLModelTrainSubsc([]byte(body))
			assertCandidateInvalidPath(t, err, test.path)
		})
	}
}

func TestCandidateTopologyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		old         string
		new         string
		invalidPath string
	}{
		{
			name: "duplicate identity",
			old:  `"priority":100`,
			new: `"priority":100,"children":[{` +
				`"nfInstanceId":"10000000-0000-4000-8000-000000000001"}]`,
			invalidPath: "x-flTopology.children[0].children[0].nfInstanceId",
		},
		{
			name: "duplicate sibling identity",
			old:  `"priority":100`,
			new: `"priority":100},{` +
				`"nfInstanceId":"10000000-0000-4000-8000-000000000101",` +
				`"priority":90`,
			invalidPath: "x-flTopology.children[1].nfInstanceId",
		},
		{
			name:        "priority is required",
			old:         `"priority":100`,
			new:         `"enabled":true`,
			invalidPath: "x-flTopology.children[0].priority",
		},
		{
			name:        "minimum availability",
			old:         `"minTrainNodes":1`,
			new:         `"minTrainNodes":2`,
			invalidPath: "x-flTopology.policy.minAvailableNodes",
		},
		{
			name:        "unknown strategy method",
			old:         `"method":"fedProx"`,
			new:         `"method":"fedAvg"`,
			invalidPath: "x-flTopology.strategy.method",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			body := strings.Replace(validCandidateSubscription, test.old, test.new, 1)
			_, err := ParseNwdafMLModelTrainSubsc([]byte(body))
			assertCandidateInvalidPath(t, err, test.invalidPath)
		})
	}
}

func TestCandidateReceiverIdentityUsesUUIDSemantics(t *testing.T) {
	t.Parallel()

	duplicateBody := strings.Replace(
		validCandidateSubscription,
		"10000000-0000-4000-8000-000000000001",
		"10000000-0000-4000-8000-000000000abc",
		1,
	)
	duplicateBody = strings.Replace(
		duplicateBody,
		"10000000-0000-4000-8000-000000000101",
		"10000000-0000-4000-8000-000000000ABC",
		1,
	)
	_, err := ParseNwdafMLModelTrainSubsc([]byte(duplicateBody))
	assertCandidateInvalidPath(t, err, "x-flTopology.children[0].nfInstanceId")

	body := strings.Replace(
		validCandidateSubscription,
		"10000000-0000-4000-8000-000000000001",
		"10000000-0000-4000-8000-000000000ABC",
		1,
	)
	value, err := ParseNwdafMLModelTrainSubsc([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if validationErr := ValidateCandidateSubscriptionReceiver(
		value, "10000000-0000-4000-8000-000000000abc",
	); validationErr != nil {
		t.Fatalf("receiver UUID case changed identity: %v", validationErr)
	}
}

func TestCandidateTopologySafetyBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		topology FlTopologyNode
	}{
		{name: "depth", topology: candidateTopologyChain(CandidateTopologyMaxDepth + 1)},
		{name: "nodes", topology: candidateWideTopology(CandidateTopologyMaxNodes + 1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			body := candidateBodyWithTopology(t, test.topology)
			_, err := ParseNwdafMLModelTrainSubsc(body)
			assertCandidateInvalidPathPrefix(t, err, "x-flTopology")
		})
	}
}

func TestCandidateReportStatusAndForwardCompatibleEnums(t *testing.T) {
	t.Parallel()

	base := `{
		"notifCorreId":"root-branch-a",
		"mlCorreId":"hierarchical-fl-001",
		"x-flTopologyReport":{
			"nfInstanceId":"10000000-0000-4000-8000-000000000001",
			"children":[{
				"nfInstanceId":"10000000-0000-4000-8000-000000000101",
				"status":"%s",
				"statusTimestamp":"2026-09-02T06:29:10Z"%s
			}]
		}
	}`

	_, err := ParseNwdafMLModelTrainNotif([]byte(fmt.Sprintf(base, "FAILED", "")))
	assertCandidateInvalidPath(t, err, "x-flTopologyReport.children[0].statusCause")

	_, err = ParseNwdafMLModelTrainNotif([]byte(fmt.Sprintf(
		base, "ACTIVE", `,"statusCause":"OTHER"`,
	)))
	assertCandidateInvalidPath(t, err, "x-flTopologyReport.children[0].statusCause")

	if _, parseErr := ParseNwdafMLModelTrainNotif([]byte(fmt.Sprintf(
		base, "VENDOR_PENDING", `,"statusCause":"VENDOR_REASON"`,
	))); parseErr != nil {
		t.Fatalf("forward-compatible status/cause error = %v", parseErr)
	}

	body := strings.Replace(validCandidateSubscription, `"selectionMethod":"priority"`, `"selectionMethod":"vendor"`, 1)
	body = strings.Replace(body, `"aggregation":"sampleWeighted"`, `"aggregation":"vendorWeighted"`, 1)
	body = strings.Replace(body, `"unit":"round"`, `"unit":"window"`, 1)
	if _, parseErr := ParseNwdafMLModelTrainSubsc([]byte(body)); parseErr != nil {
		t.Fatalf("forward-compatible instruction enum error = %v", parseErr)
	}
}

func TestCandidateNotificationAndRetainedResultRules(t *testing.T) {
	t.Parallel()

	topologyOnly := `{
		"notifCorreId":"root-branch-a",
		"mlCorreId":"hierarchical-fl-001",
		"x-flTopologyReport":{
			"nfInstanceId":"10000000-0000-4000-8000-000000000001",
			"children":[{
				"nfInstanceId":"10000000-0000-4000-8000-000000000101",
				"status":"ACTIVE",
				"statusTimestamp":"2026-09-02T06:29:10Z"
			}]
		}
	}`
	if _, err := ParseNwdafMLModelTrainNotif([]byte(topologyOnly)); err != nil {
		t.Fatalf("topology-only notification error = %v", err)
	}

	found := `{
		"notifCorreId":"root-branch-a",
		"mlCorreId":"hierarchical-fl-001",
		"x-retainedResultStatus":"FOUND",
		"roundInd":5,
		"mLModelInfos":[{
			"event":"UE_COMMUNICATION",
			"mLFileAddr":{"mLModelUrl":"http://leaf.example/round-5"}
		}]
	}`
	if _, err := ParseNwdafMLModelTrainNotif([]byte(found)); err != nil {
		t.Fatalf("FOUND notification error = %v", err)
	}

	missingRound := strings.Replace(found, `"roundInd":5,`, "", 1)
	_, err := ParseNwdafMLModelTrainNotif([]byte(missingRound))
	assertCandidateInvalidPath(t, err, "roundInd")

	notFoundWithModel := strings.Replace(found, `"FOUND"`, `"NOT_FOUND"`, 1)
	_, err = ParseNwdafMLModelTrainNotif([]byte(notFoundWithModel))
	assertCandidateInvalidPath(t, err, "roundInd")

	notFoundWithModel = strings.Replace(notFoundWithModel, `"roundInd":5,`, "", 1)
	_, err = ParseNwdafMLModelTrainNotif([]byte(notFoundWithModel))
	assertCandidateInvalidPath(t, err, "mLModelInfos")

	failed := `{
		"notifCorreId":"root-branch-a",
		"mlCorreId":"hierarchical-fl-001",
		"x-retainedResultStatus":"FAILED"
	}`
	if _, parseErr := ParseNwdafMLModelTrainNotif([]byte(failed)); parseErr != nil {
		t.Fatalf("FAILED notification error = %v", parseErr)
	}
	unknown := strings.Replace(failed, `"FAILED"`, `"VENDOR_OUTCOME"`, 1)
	if _, parseErr := ParseNwdafMLModelTrainNotif([]byte(unknown)); parseErr != nil {
		t.Fatalf("forward-compatible retained outcome error = %v", parseErr)
	}
}

func TestCandidateTopologyReportRecursiveValidation(t *testing.T) {
	t.Parallel()

	duplicate := `{
		"notifCorreId":"root-branch-a",
		"mlCorreId":"hierarchical-fl-001",
		"x-flTopologyReport":{
			"nfInstanceId":"10000000-0000-4000-8000-000000000001",
			"children":[{
				"nfInstanceId":"10000000-0000-4000-8000-000000000001",
				"status":"ACTIVE",
				"statusTimestamp":"2026-09-02T06:29:10Z"
			}]
		}
	}`
	_, err := ParseNwdafMLModelTrainNotif([]byte(duplicate))
	assertCandidateInvalidPath(t, err, "x-flTopologyReport.children[0].nfInstanceId")

	tests := []struct {
		name   string
		report FlTopologyReport
	}{
		{name: "depth", report: candidateTopologyReportChain(CandidateTopologyMaxDepth + 1)},
		{name: "nodes", report: candidateWideTopologyReport(CandidateTopologyMaxNodes + 1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			body, marshalErr := json.Marshal(map[string]any{
				"notifCorreId":       "root-branch-a",
				"mlCorreId":          "hierarchical-fl-001",
				"x-flTopologyReport": test.report,
			})
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			_, parseErr := ParseNwdafMLModelTrainNotif(body)
			assertCandidateInvalidPathPrefix(t, parseErr, "x-flTopologyReport")
		})
	}
}

func TestCandidateOperationFieldsCanBeRemovedFromPersistentState(t *testing.T) {
	t.Parallel()

	body := strings.Replace(
		validCandidateSubscription,
		`"mLPreFlag":true,`,
		`"mLPreFlag":true,"x-retainedResultReq":true,`,
		1,
	)
	body = strings.Replace(
		body,
		`"priority":100`,
		`"priority":100,"retainedResultReq":true`,
		1,
	)
	value, err := ParseNwdafMLModelTrainSubsc([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	descriptor := StripCandidateOperations(value)
	if !descriptor.TopLevelRetainedResultRequest || len(descriptor.NodeRequests) != 1 {
		t.Fatalf("operation descriptor = %#v", descriptor)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "retainedResultReq") {
		t.Fatalf("operation field persisted after stripping: %s", encoded)
	}
}

func TestCandidateCorrelationRequirementsUseOperationSemantics(t *testing.T) {
	t.Parallel()

	withoutCorrelation := strings.Replace(
		validCandidateSubscription, `"mlCorreId":"hierarchical-fl-001",`, "", 1,
	)
	value, err := ParseNwdafMLModelTrainSubsc([]byte(withoutCorrelation))
	if err != nil {
		t.Fatal(err)
	}
	assertCandidateInvalidPath(t, ValidateFLSubscription(value, nil), "mlCorreId")

	patch, err := ParseNwdafMLModelTrainSubscPatch([]byte(`{
		"x-flTopology":{"policy":{"minTrainNodes":1}}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	assertCandidateInvalidPath(t, ValidateFLPatch(patch, nil), "mlCorreId")

	falseOnly := strings.Replace(
		withoutCorrelation,
		`"x-flTopology":{`,
		`"x-retainedResultReq":false,"unused":{`,
		1,
	)
	falseValue, err := ParseNwdafMLModelTrainSubsc([]byte(falseOnly))
	if err != nil {
		t.Fatal(err)
	}
	var requirements *RequirementsError
	validationErr := ValidateFLSubscription(falseValue, nil)
	if !errors.As(validationErr, &requirements) {
		t.Fatalf(
			"false retained-result request error = %T %v, want standard RequirementsError",
			validationErr, validationErr,
		)
	}
}

func TestCandidateTopologyPatchUsesJSONMergePatchSemantics(t *testing.T) {
	t.Parallel()

	current, err := ParseNwdafMLModelTrainSubsc([]byte(validCandidateSubscription))
	if err != nil {
		t.Fatal(err)
	}
	patch, err := ParseNwdafMLModelTrainSubscPatch([]byte(`{
		"x-flTopology": {
			"policy": {"minTrainNodes": 1}
		}
	}`))
	if err != nil {
		t.Fatalf("ParseNwdafMLModelTrainSubscPatch() error = %v", err)
	}
	effective, err := ApplySubscriptionPatch(current, patch)
	if err != nil {
		t.Fatalf("ApplySubscriptionPatch() error = %v", err)
	}
	if effective.FLTopology == nil ||
		effective.FLTopology.NFInstanceID != current.FLTopology.NFInstanceID ||
		len(effective.FLTopology.Children) != 1 ||
		effective.FLTopology.Policy == nil ||
		effective.FLTopology.Policy.MinimumAvailableNodes == nil ||
		*effective.FLTopology.Policy.MinimumAvailableNodes != 1 {
		t.Fatalf("effective topology = %#v", effective.FLTopology)
	}
}

func TestCandidatePatchCanRemoveOptionalNestedObject(t *testing.T) {
	t.Parallel()

	current, err := ParseNwdafMLModelTrainSubsc([]byte(validCandidateSubscription))
	if err != nil {
		t.Fatal(err)
	}
	patch, err := ParseNwdafMLModelTrainSubscPatch([]byte(`{
		"x-flTopology": {"strategy": null}
	}`))
	if err != nil {
		t.Fatalf("ParseNwdafMLModelTrainSubscPatch() error = %v", err)
	}
	effective, err := ApplySubscriptionPatch(current, patch)
	if err != nil {
		t.Fatalf("ApplySubscriptionPatch() error = %v", err)
	}
	if effective.FLTopology == nil || effective.FLTopology.Strategy != nil {
		t.Fatalf("effective topology = %#v", effective.FLTopology)
	}
}

func TestCandidatePatchReplacesChildrenArrayAndCanRemoveTopology(t *testing.T) {
	t.Parallel()

	current, err := ParseNwdafMLModelTrainSubsc([]byte(validCandidateSubscription))
	if err != nil {
		t.Fatal(err)
	}
	patch, err := ParseNwdafMLModelTrainSubscPatch([]byte(`{
		"x-flTopology":{
			"children":[{
				"nfInstanceId":"10000000-0000-4000-8000-000000000202",
				"priority":80
			}]
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	effective, err := ApplySubscriptionPatch(current, patch)
	if err != nil {
		t.Fatal(err)
	}
	if len(effective.FLTopology.Children) != 1 ||
		effective.FLTopology.Children[0].NFInstanceID != "10000000-0000-4000-8000-000000000202" {
		t.Fatalf("children were not replaced: %#v", effective.FLTopology.Children)
	}

	remove, err := ParseNwdafMLModelTrainSubscPatch([]byte(`{"x-flTopology":null}`))
	if err != nil {
		t.Fatal(err)
	}
	effective, err = ApplySubscriptionPatch(effective, remove)
	if err != nil {
		t.Fatal(err)
	}
	if effective.FLTopology != nil {
		t.Fatalf("topology was not removed: %#v", effective.FLTopology)
	}
}

func TestCandidateCreateRejectsNullScalar(t *testing.T) {
	t.Parallel()

	body := strings.Replace(
		validCandidateSubscription,
		`"priority":100`,
		`"priority":100,"enabled":null`,
		1,
	)
	_, err := ParseNwdafMLModelTrainSubsc([]byte(body))
	assertCandidateInvalidPath(t, err, "x-flTopology.children[0].enabled")
}

func TestCandidateImmediateReportUsesCandidateValidation(t *testing.T) {
	t.Parallel()

	body := strings.Replace(
		validCandidateSubscription,
		`"x-flTopology":{`,
		`"immReport":{
			"notifCorreId":"immediate-report",
			"mlCorreId":"hierarchical-fl-001",
			"x-flTopologyReport":{"nfInstanceId":"not-a-uuid"}
		},
		"x-flTopology":{`,
		1,
	)
	_, err := ParseNwdafMLModelTrainSubsc([]byte(body))
	assertCandidateInvalidPath(t, err, "immReport.x-flTopologyReport.nfInstanceId")
}

func assertCandidateInvalidPath(t *testing.T, err error, path string) {
	t.Helper()
	var invalid *InvalidMessageError
	if !errors.As(err, &invalid) {
		t.Fatalf("error = %v, want InvalidMessageError", err)
	}
	for _, violation := range invalid.Violations {
		if violation.Parameter == path {
			return
		}
	}
	t.Fatalf("missing invalid path %q in %#v", path, invalid.Violations)
}

func assertCandidateInvalidPathPrefix(t *testing.T, err error, prefix string) {
	t.Helper()
	var invalid *InvalidMessageError
	if !errors.As(err, &invalid) {
		t.Fatalf("error = %v, want InvalidMessageError", err)
	}
	for _, violation := range invalid.Violations {
		if strings.HasPrefix(violation.Parameter, prefix) {
			return
		}
	}
	t.Fatalf("missing invalid path prefix %q in %#v", prefix, invalid.Violations)
}

func candidateBodyWithTopology(t *testing.T, topology FlTopologyNode) []byte {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal([]byte(validCandidateSubscription), &body); err != nil {
		t.Fatal(err)
	}
	body["x-flTopology"] = topology
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func candidateTopologyChain(nodes int) FlTopologyNode {
	root := FlTopologyNode{NFInstanceID: candidateNFInstanceID(1)}
	cursor := &root
	for index := 2; index <= nodes; index++ {
		cursor.Children = []FlTopologyNode{{NFInstanceID: candidateNFInstanceID(index)}}
		cursor = &cursor.Children[0]
	}
	return root
}

func candidateWideTopology(nodes int) FlTopologyNode {
	root := FlTopologyNode{NFInstanceID: candidateNFInstanceID(1)}
	for index := 2; index <= nodes; index++ {
		root.Children = append(root.Children, FlTopologyNode{
			NFInstanceID: candidateNFInstanceID(index),
		})
	}
	return root
}

func candidateTopologyReportChain(nodes int) FlTopologyReport {
	root := FlTopologyReport{NFInstanceID: candidateNFInstanceID(1)}
	if nodes <= 1 {
		return root
	}
	root.Children = []FlTopologyReportNode{{
		NFInstanceID:    candidateNFInstanceID(2),
		Status:          "ACTIVE",
		StatusTimestamp: "2026-09-02T06:29:10Z",
	}}
	cursor := &root.Children[0]
	for index := 3; index <= nodes; index++ {
		cursor.Children = []FlTopologyReportNode{{
			NFInstanceID:    candidateNFInstanceID(index),
			Status:          "ACTIVE",
			StatusTimestamp: "2026-09-02T06:29:10Z",
		}}
		cursor = &cursor.Children[0]
	}
	return root
}

func candidateWideTopologyReport(nodes int) FlTopologyReport {
	root := FlTopologyReport{NFInstanceID: candidateNFInstanceID(1)}
	for index := 2; index <= nodes; index++ {
		root.Children = append(root.Children, FlTopologyReportNode{
			NFInstanceID:    candidateNFInstanceID(index),
			Status:          "ACTIVE",
			StatusTimestamp: "2026-09-02T06:29:10Z",
		})
	}
	return root
}

func candidateNFInstanceID(index int) string {
	return fmt.Sprintf("10000000-0000-4000-8000-%012d", index)
}
