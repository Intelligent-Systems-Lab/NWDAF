package mlmodeltraining

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	topologyNodeProperties = propertySet(
		"nfInstanceId", "enabled", "priority", "policy", "strategy", "reportAfter",
		"retainedResultReq", "children",
	)
	topologyReportProperties = propertySet(
		"nfInstanceId", "policy", "strategy", "reportAfter", "children",
	)
	topologyReportNodeProperties = propertySet(
		"nfInstanceId", "status", "statusTimestamp", "statusCause", "policy", "strategy",
		"reportAfter", "children",
	)
	policyProperties = propertySet(
		"allowAdditionalCandidates", "additionalCandidatePriority", "selectionMethod",
		"minAvailableNodes", "fractionTrain", "minTrainNodes", "acceptFailures",
		"minCompletionRate",
	)
	strategyProperties         = propertySet("method", "aggregation", "methodParameters")
	fedProxParameterProperties = propertySet("proximalMu")
	reportAfterProperties      = propertySet("count", "unit")
)

func validateCandidateRawSubscription(body []byte, patch bool) error {
	object, parseErr := rawObject(body, "")
	if parseErr != nil {
		return parseErr
	}
	if raw, present := object["flTopology"]; present {
		if !patch || !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			count := 0
			if validationErr := validateRawTopologyNode(
				raw, "flTopology", 1, &count, patch,
			); validationErr != nil {
				return validationErr
			}
		}
	}
	if raw, present := object["retainedResultReq"]; present {
		if !patch || !isJSONNull(raw) {
			if validationErr := validateRawBoolean(raw, "retainedResultReq"); validationErr != nil {
				return validationErr
			}
		}
	}
	if raw, present := object["immReport"]; present {
		immediateReport, reportErr := rawObject(raw, "immReport")
		if reportErr != nil {
			return reportErr
		}
		if validationErr := validateCandidateRawNotificationObject(
			immediateReport, "immReport",
		); validationErr != nil {
			return validationErr
		}
	}
	return nil
}

func validateCandidateRawNotification(body []byte) error {
	object, err := rawObject(body, "")
	if err != nil {
		return err
	}
	return validateCandidateRawNotificationObject(object, "")
}

func validateCandidateRawNotificationObject(
	object map[string]json.RawMessage,
	prefix string,
) error {
	if raw, present := object["flTopologyReport"]; present {
		count := 0
		if err := validateRawTopologyReport(
			raw, joinPath(prefix, "flTopologyReport"), 1, &count,
		); err != nil {
			return err
		}
	}
	if raw, present := object["retainedResultStatus"]; present {
		if err := validateRawNonEmptyString(
			raw, joinPath(prefix, "retainedResultStatus"),
		); err != nil {
			return err
		}
	}
	return nil
}

func validateRawTopologyNode(
	raw json.RawMessage,
	path string,
	depth int,
	count *int,
	partial bool,
) error {
	if err := advanceTopologyBounds(path, depth, count); err != nil {
		return err
	}
	object, objectErr := strictRawObject(raw, path, topologyNodeProperties)
	if objectErr != nil {
		return objectErr
	}
	if validationErr := validateRawStringProperty(
		object, "nfInstanceId", path, !partial, partial,
	); validationErr != nil {
		return validationErr
	}
	if validationErr := validateRawBooleanProperty(
		object, "enabled", path, partial,
	); validationErr != nil {
		return validationErr
	}
	if validationErr := validateRawIntegerProperty(
		object, "priority", path, partial,
	); validationErr != nil {
		return validationErr
	}
	if childRaw, present := object["children"]; present {
		if partial && isJSONNull(childRaw) {
			return validateRawNodeContracts(object, path, partial)
		}
		children, childrenErr := rawArray(childRaw, path+".children")
		if childrenErr != nil {
			return childrenErr
		}
		if len(children) == 0 {
			return candidateInvalid(path+".children", "must contain at least one item when present")
		}
		for index, child := range children {
			if validationErr := validateRawTopologyNode(
				child, fmt.Sprintf("%s.children[%d]", path, index), depth+1, count, false,
			); validationErr != nil {
				return validationErr
			}
		}
	}
	return validateRawNodeContracts(object, path, partial)
}

func validateRawTopologyReport(raw json.RawMessage, path string, depth int, count *int) error {
	if err := advanceTopologyBounds(path, depth, count); err != nil {
		return err
	}
	object, objectErr := strictRawObject(raw, path, topologyReportProperties)
	if objectErr != nil {
		return objectErr
	}
	if validationErr := validateRawStringProperty(
		object, "nfInstanceId", path, true, false,
	); validationErr != nil {
		return validationErr
	}
	if validationErr := validateRawNodeContracts(object, path, false); validationErr != nil {
		return validationErr
	}
	if childRaw, present := object["children"]; present {
		children, childrenErr := rawArray(childRaw, path+".children")
		if childrenErr != nil {
			return childrenErr
		}
		if len(children) == 0 {
			return candidateInvalid(path+".children", "must contain at least one item when present")
		}
		for index, child := range children {
			if validationErr := validateRawTopologyReportNode(
				child, fmt.Sprintf("%s.children[%d]", path, index), depth+1, count,
			); validationErr != nil {
				return validationErr
			}
		}
	}
	return nil
}

func validateRawTopologyReportNode(
	raw json.RawMessage,
	path string,
	depth int,
	count *int,
) error {
	if err := advanceTopologyBounds(path, depth, count); err != nil {
		return err
	}
	object, objectErr := strictRawObject(raw, path, topologyReportNodeProperties)
	if objectErr != nil {
		return objectErr
	}
	for _, property := range []string{"nfInstanceId", "status", "statusTimestamp"} {
		if validationErr := validateRawStringProperty(
			object, property, path, true, false,
		); validationErr != nil {
			return validationErr
		}
	}
	if validationErr := validateRawStringProperty(
		object, "statusCause", path, false, false,
	); validationErr != nil {
		return validationErr
	}
	if validationErr := validateRawNodeContracts(object, path, false); validationErr != nil {
		return validationErr
	}
	if childRaw, present := object["children"]; present {
		children, childrenErr := rawArray(childRaw, path+".children")
		if childrenErr != nil {
			return childrenErr
		}
		if len(children) == 0 {
			return candidateInvalid(path+".children", "must contain at least one item when present")
		}
		for index, child := range children {
			if validationErr := validateRawTopologyReportNode(
				child, fmt.Sprintf("%s.children[%d]", path, index), depth+1, count,
			); validationErr != nil {
				return validationErr
			}
		}
	}
	return nil
}

func validateRawNodeContracts(
	object map[string]json.RawMessage,
	path string,
	partial bool,
) error {
	if raw, present := object["policy"]; present && (!partial || !isJSONNull(raw)) {
		policy, policyErr := strictRawObject(raw, path+".policy", policyProperties)
		if policyErr != nil {
			return policyErr
		}
		for _, property := range []string{"allowAdditionalCandidates", "acceptFailures"} {
			if validationErr := validateRawBooleanProperty(
				policy, property, path+".policy", partial,
			); validationErr != nil {
				return validationErr
			}
		}
		for _, property := range []string{
			"additionalCandidatePriority", "minAvailableNodes", "minTrainNodes",
		} {
			if validationErr := validateRawIntegerProperty(
				policy, property, path+".policy", partial,
			); validationErr != nil {
				return validationErr
			}
		}
		if validationErr := validateRawStringProperty(
			policy, "selectionMethod", path+".policy", false, partial,
		); validationErr != nil {
			return validationErr
		}
		for _, property := range []string{"fractionTrain", "minCompletionRate"} {
			if validationErr := validateRawNumberProperty(
				policy, property, path+".policy", partial,
			); validationErr != nil {
				return validationErr
			}
		}
	}
	if raw, present := object["strategy"]; present && (!partial || !isJSONNull(raw)) {
		strategy, strategyErr := strictRawObject(raw, path+".strategy", strategyProperties)
		if strategyErr != nil {
			return strategyErr
		}
		for _, property := range []string{"method", "aggregation"} {
			if validationErr := validateRawStringProperty(
				strategy, property, path+".strategy", !partial, partial,
			); validationErr != nil {
				return validationErr
			}
		}
		if parameters, parametersPresent := strategy["methodParameters"]; parametersPresent {
			if !partial || !isJSONNull(parameters) {
				parameterObject, parameterErr := strictRawObject(
					parameters, path+".strategy.methodParameters", fedProxParameterProperties,
				)
				if parameterErr != nil {
					return parameterErr
				}
				if validationErr := validateRawNumberProperty(
					parameterObject, "proximalMu", path+".strategy.methodParameters", partial,
				); validationErr != nil {
					return validationErr
				}
				if !partial {
					if validationErr := requireRawProperty(
						parameterObject, "proximalMu", path+".strategy.methodParameters",
					); validationErr != nil {
						return validationErr
					}
				}
			}
		} else if !partial {
			return candidateInvalid(path+".strategy.methodParameters", "is required")
		}
	}
	if raw, present := object["reportAfter"]; present && (!partial || !isJSONNull(raw)) {
		reportAfter, reportErr := strictRawObject(raw, path+".reportAfter", reportAfterProperties)
		if reportErr != nil {
			return reportErr
		}
		if validationErr := validateRawIntegerProperty(
			reportAfter, "count", path+".reportAfter", partial,
		); validationErr != nil {
			return validationErr
		}
		if validationErr := validateRawStringProperty(
			reportAfter, "unit", path+".reportAfter", !partial, partial,
		); validationErr != nil {
			return validationErr
		}
		if !partial {
			if validationErr := requireRawProperty(
				reportAfter, "count", path+".reportAfter",
			); validationErr != nil {
				return validationErr
			}
		}
	}
	if raw, present := object["retainedResultReq"]; present {
		if partial && isJSONNull(raw) {
			return nil
		}
		return validateRawBoolean(raw, path+".retainedResultReq")
	}
	return nil
}

func validateTopology(node *FlTopologyNode, path string) error {
	if node == nil {
		return nil
	}
	seen := make(map[string]struct{})
	return validateTopologyNode(node, path, seen)
}

func validateTopologyNode(node *FlTopologyNode, path string, seen map[string]struct{}) error {
	identity, identityErr := canonicalNFInstanceID(node.NFInstanceID, path+".nfInstanceId")
	if identityErr != nil {
		return identityErr
	}
	if _, duplicate := seen[identity]; duplicate {
		return candidateInvalid(path+".nfInstanceId", "must be unique within the subtree")
	}
	seen[identity] = struct{}{}
	if node.Priority != nil && *node.Priority < 0 {
		return candidateInvalid(path+".priority", "must be non-negative")
	}
	if boolValue(node.RetainedResultRequest) && node.Enabled != nil && !*node.Enabled {
		return candidateInvalid(
			path+".retainedResultReq", "cannot be true when the node is disabled",
		)
	}
	if validationErr := validatePolicy(
		node.Policy, node.Children, path+".policy", path+".children",
	); validationErr != nil {
		return validationErr
	}
	if validationErr := validateStrategy(node.Strategy, path+".strategy"); validationErr != nil {
		return validationErr
	}
	if validationErr := validateReportAfter(
		node.ReportAfter, path+".reportAfter",
	); validationErr != nil {
		return validationErr
	}
	for index := range node.Children {
		if validationErr := validateTopologyNode(
			&node.Children[index], fmt.Sprintf("%s.children[%d]", path, index), seen,
		); validationErr != nil {
			return validationErr
		}
	}
	return nil
}

func validateTopologyReport(report *FlTopologyReport, path string) error {
	if report == nil {
		return nil
	}
	identity, identityErr := canonicalNFInstanceID(report.NFInstanceID, path+".nfInstanceId")
	if identityErr != nil {
		return identityErr
	}
	if validationErr := validatePolicy(
		report.Policy, nil, path+".policy", path+".children",
	); validationErr != nil {
		return validationErr
	}
	if validationErr := validateStrategy(report.Strategy, path+".strategy"); validationErr != nil {
		return validationErr
	}
	if validationErr := validateReportAfter(
		report.ReportAfter, path+".reportAfter",
	); validationErr != nil {
		return validationErr
	}
	seen := map[string]struct{}{identity: {}}
	for index := range report.Children {
		if validationErr := validateTopologyReportNode(
			&report.Children[index], fmt.Sprintf("%s.children[%d]", path, index), seen,
		); validationErr != nil {
			return validationErr
		}
	}
	return nil
}

func validateTopologyReportNode(
	node *FlTopologyReportNode,
	path string,
	seen map[string]struct{},
) error {
	identity, identityErr := canonicalNFInstanceID(node.NFInstanceID, path+".nfInstanceId")
	if identityErr != nil {
		return identityErr
	}
	if _, duplicate := seen[identity]; duplicate {
		return candidateInvalid(path+".nfInstanceId", "must be unique within the subtree")
	}
	seen[identity] = struct{}{}
	if strings.TrimSpace(node.Status) == "" {
		return candidateInvalid(path+".status", "is required")
	}
	if _, timestampErr := time.Parse(time.RFC3339, node.StatusTimestamp); timestampErr != nil {
		return candidateInvalid(path+".statusTimestamp", "must be an RFC3339 date-time")
	}
	knownFailure := node.Status == "FAILED" || node.Status == "INACTIVE"
	knownNonFailure := node.Status == "UNCONFIRMED" || node.Status == "DEPLOYING" || node.Status == "ACTIVE"
	if knownFailure && strings.TrimSpace(node.StatusCause) == "" {
		return candidateInvalid(path+".statusCause", "is required for FAILED or INACTIVE")
	}
	if knownNonFailure && node.StatusCause != "" {
		return candidateInvalid(path+".statusCause", "is not allowed for this status")
	}
	if validationErr := validatePolicy(
		node.Policy, nil, path+".policy", path+".children",
	); validationErr != nil {
		return validationErr
	}
	if validationErr := validateStrategy(node.Strategy, path+".strategy"); validationErr != nil {
		return validationErr
	}
	if validationErr := validateReportAfter(
		node.ReportAfter, path+".reportAfter",
	); validationErr != nil {
		return validationErr
	}
	for index := range node.Children {
		if validationErr := validateTopologyReportNode(
			&node.Children[index], fmt.Sprintf("%s.children[%d]", path, index), seen,
		); validationErr != nil {
			return validationErr
		}
	}
	return nil
}

func validatePolicy(
	policy *FlPolicy,
	children []FlTopologyNode,
	path string,
	childrenPath string,
) error {
	if policy == nil {
		return nil
	}
	if policy.AdditionalCandidatePriority != nil && *policy.AdditionalCandidatePriority < 0 {
		return candidateInvalid(path+".additionalCandidatePriority", "must be non-negative")
	}
	if policy.MinimumAvailableNodes != nil && *policy.MinimumAvailableNodes < 1 {
		return candidateInvalid(path+".minAvailableNodes", "must be at least 1")
	}
	if policy.MinimumTrainNodes != nil && *policy.MinimumTrainNodes < 1 {
		return candidateInvalid(path+".minTrainNodes", "must be at least 1")
	}
	if policy.MinimumAvailableNodes != nil && policy.MinimumTrainNodes != nil &&
		*policy.MinimumAvailableNodes < *policy.MinimumTrainNodes {
		return candidateInvalid(path+".minAvailableNodes", "must be at least minTrainNodes")
	}
	if policy.FractionTrain != nil && (*policy.FractionTrain <= 0 || *policy.FractionTrain > 1) {
		return candidateInvalid(path+".fractionTrain", "must be greater than 0 and at most 1")
	}
	if policy.MinimumCompletionRate != nil &&
		(*policy.MinimumCompletionRate <= 0 || *policy.MinimumCompletionRate > 1) {
		return candidateInvalid(
			path+".minCompletionRate", "must be greater than 0 and at most 1",
		)
	}
	if policy.SelectionMethod == "priority" {
		for index := range children {
			child := &children[index]
			if (child.Enabled == nil || *child.Enabled) && child.Priority == nil {
				return candidateInvalid(
					fmt.Sprintf("%s[%d].priority", childrenPath, index),
					"is required for an enabled child when selectionMethod is priority",
				)
			}
		}
	}
	return nil
}

func validateStrategy(strategy *FlStrategy, path string) error {
	if strategy == nil {
		return nil
	}
	if strategy.Method != "fedProx" {
		return candidateInvalid(path+".method", "must be fedProx")
	}
	if strings.TrimSpace(strategy.Aggregation) == "" {
		return candidateInvalid(path+".aggregation", "is required")
	}
	if strategy.MethodParameters == nil || strategy.MethodParameters.ProximalMu == nil {
		return candidateInvalid(path+".methodParameters.proximalMu", "is required")
	}
	if *strategy.MethodParameters.ProximalMu < 0 {
		return candidateInvalid(path+".methodParameters.proximalMu", "must be non-negative")
	}
	return nil
}

func validateReportAfter(value *FlReportAfter, path string) error {
	if value == nil {
		return nil
	}
	if value.Count < 1 {
		return candidateInvalid(path+".count", "must be at least 1")
	}
	if strings.TrimSpace(value.Unit) == "" {
		return candidateInvalid(path+".unit", "is required")
	}
	return nil
}

func validateRetainedResultAt(value *NwdafMLModelTrainNotif, prefix string) error {
	if value == nil || value.RetainedResultStatus == "" {
		return nil
	}
	switch value.RetainedResultStatus {
	case "FOUND":
		if value.RoundIndicator == nil {
			return candidateInvalid(
				joinPath(prefix, "roundInd"),
				"is required when retainedResultStatus is FOUND",
			)
		}
		if len(value.MLModelInfos) == 0 {
			return candidateInvalid(
				joinPath(prefix, "mLModelInfos"),
				"is required when retainedResultStatus is FOUND",
			)
		}
	case "NOT_FOUND", "FAILED":
		if value.RoundIndicator != nil {
			return candidateInvalid(
				joinPath(prefix, "roundInd"),
				"is not allowed when retainedResultStatus is not FOUND",
			)
		}
		if len(value.MLModelInfos) > 0 {
			return candidateInvalid(
				joinPath(prefix, "mLModelInfos"),
				"is not allowed when retainedResultStatus is not FOUND",
			)
		}
	}
	return nil
}

func ValidateCandidateSubscriptionReceiver(
	value *NwdafMLModelTrainSubsc,
	expectedNFInstanceID string,
) error {
	if value == nil || value.FLTopology == nil || strings.TrimSpace(expectedNFInstanceID) == "" {
		return nil
	}
	if !sameNFInstanceID(value.FLTopology.NFInstanceID, expectedNFInstanceID) {
		return candidateInvalid(
			"flTopology.nfInstanceId", "must identify the request receiver",
		)
	}
	return nil
}

func ValidateCandidateNotificationParticipant(
	value *NwdafMLModelTrainNotif,
	expectedNFInstanceID string,
) error {
	if value == nil || value.FLTopologyReport == nil || strings.TrimSpace(expectedNFInstanceID) == "" {
		return nil
	}
	if !sameNFInstanceID(value.FLTopologyReport.NFInstanceID, expectedNFInstanceID) {
		return candidateInvalid(
			"flTopologyReport.nfInstanceId", "must identify the bound direct participant",
		)
	}
	return nil
}

func candidateInvalid(path string, reason string) error {
	return &InvalidMessageError{Violations: []InvalidParameter{{Parameter: path, Reason: reason}}}
}

func advanceTopologyBounds(path string, depth int, count *int) error {
	if depth > CandidateTopologyMaxDepth {
		return candidateInvalid(path, "exceeds the maximum topology depth")
	}
	*count++
	if *count > CandidateTopologyMaxNodes {
		return candidateInvalid(path, "exceeds the maximum topology node count")
	}
	return nil
}

func canonicalNFInstanceID(value string, path string) (string, error) {
	parsed, err := uuid.Parse(value)
	if err != nil {
		return "", candidateInvalid(path, "must be a UUID")
	}
	return parsed.String(), nil
}

func sameNFInstanceID(first string, second string) bool {
	firstID, firstErr := uuid.Parse(first)
	secondID, secondErr := uuid.Parse(second)
	return firstErr == nil && secondErr == nil && firstID == secondID
}

func strictRawObject(
	raw json.RawMessage,
	path string,
	allowed map[string]struct{},
) (map[string]json.RawMessage, error) {
	object, err := rawObject(raw, path)
	if err != nil {
		return nil, err
	}
	for property := range object {
		if _, ok := allowed[property]; !ok {
			return nil, candidateInvalid(joinPath(path, property), "is not allowed")
		}
	}
	return object, nil
}

func rawObject(raw []byte, path string) (map[string]json.RawMessage, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) || trimmed[0] != '{' {
		return nil, candidateInvalid(path, "must be a JSON object")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &object); err != nil {
		return nil, candidateInvalid(path, err.Error())
	}
	return object, nil
}

func rawArray(raw json.RawMessage, path string) ([]json.RawMessage, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) || trimmed[0] != '[' {
		return nil, candidateInvalid(path, "must be an array")
	}
	var values []json.RawMessage
	if err := json.Unmarshal(trimmed, &values); err != nil {
		return nil, candidateInvalid(path, err.Error())
	}
	return values, nil
}

func validateRawBoolean(raw json.RawMessage, path string) error {
	if isJSONNull(raw) {
		return candidateInvalid(path, "must be a boolean")
	}
	var value bool
	if err := json.Unmarshal(raw, &value); err != nil {
		return candidateInvalid(path, "must be a boolean")
	}
	return nil
}

func validateRawBooleanProperty(
	object map[string]json.RawMessage,
	property string,
	path string,
	allowNull bool,
) error {
	raw, present := object[property]
	if !present {
		return nil
	}
	if allowNull && isJSONNull(raw) {
		return nil
	}
	return validateRawBoolean(raw, joinPath(path, property))
}

func validateRawStringProperty(
	object map[string]json.RawMessage,
	property string,
	path string,
	required bool,
	allowNull bool,
) error {
	raw, present := object[property]
	if !present {
		if required {
			return candidateInvalid(joinPath(path, property), "is required")
		}
		return nil
	}
	if allowNull && isJSONNull(raw) {
		return nil
	}
	if isJSONNull(raw) {
		return candidateInvalid(joinPath(path, property), "must be a string")
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return candidateInvalid(joinPath(path, property), "must be a string")
	}
	return nil
}

func validateRawIntegerProperty(
	object map[string]json.RawMessage,
	property string,
	path string,
	allowNull bool,
) error {
	raw, present := object[property]
	if !present {
		return nil
	}
	if allowNull && isJSONNull(raw) {
		return nil
	}
	if isJSONNull(raw) {
		return candidateInvalid(joinPath(path, property), "must be an integer")
	}
	var value int64
	if err := json.Unmarshal(raw, &value); err != nil {
		return candidateInvalid(joinPath(path, property), "must be an integer")
	}
	return nil
}

func validateRawNumberProperty(
	object map[string]json.RawMessage,
	property string,
	path string,
	allowNull bool,
) error {
	raw, present := object[property]
	if !present {
		return nil
	}
	if allowNull && isJSONNull(raw) {
		return nil
	}
	if isJSONNull(raw) {
		return candidateInvalid(joinPath(path, property), "must be a number")
	}
	var value float64
	if err := json.Unmarshal(raw, &value); err != nil {
		return candidateInvalid(joinPath(path, property), "must be a number")
	}
	return nil
}

func requireRawProperty(
	object map[string]json.RawMessage,
	property string,
	path string,
) error {
	if _, present := object[property]; !present {
		return candidateInvalid(joinPath(path, property), "is required")
	}
	return nil
}

func validateRawNonEmptyString(raw json.RawMessage, path string) error {
	if isJSONNull(raw) {
		return candidateInvalid(path, "must be a non-empty string")
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil || strings.TrimSpace(value) == "" {
		return candidateInvalid(path, "must be a non-empty string")
	}
	return nil
}

func isJSONNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

func propertySet(values ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

func joinPath(prefix string, property string) string {
	if prefix == "" {
		return property
	}
	return prefix + "." + property
}
