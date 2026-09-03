package mlmodeltraining

import (
	"math/big"
)

const (
	HierarchicalFLOrchestrationFeature = 3
	HierarchicalFLOrchestrationMask    = "4"
	CandidateTopologyMaxDepth          = 16
	CandidateTopologyMaxNodes          = 1024
)

type FlTopologyNode struct {
	NFInstanceID          string           `json:"nfInstanceId"`
	Enabled               *bool            `json:"enabled,omitempty"`
	Priority              *int64           `json:"priority,omitempty"`
	Policy                *FlPolicy        `json:"policy,omitempty"`
	Strategy              *FlStrategy      `json:"strategy,omitempty"`
	ReportAfter           *FlReportAfter   `json:"reportAfter,omitempty"`
	RetainedResultRequest *bool            `json:"retainedResultReq,omitempty"`
	Children              []FlTopologyNode `json:"children,omitempty"`
}

type FlPolicy struct {
	AllowAdditionalCandidates   *bool    `json:"allowAdditionalCandidates,omitempty"`
	AdditionalCandidatePriority *int64   `json:"additionalCandidatePriority,omitempty"`
	SelectionMethod             string   `json:"selectionMethod,omitempty"`
	MinimumAvailableNodes       *int64   `json:"minAvailableNodes,omitempty"`
	FractionTrain               *float64 `json:"fractionTrain,omitempty"`
	MinimumTrainNodes           *int64   `json:"minTrainNodes,omitempty"`
	AcceptFailures              *bool    `json:"acceptFailures,omitempty"`
	MinimumCompletionRate       *float64 `json:"minCompletionRate,omitempty"`
}

type FlStrategy struct {
	Method           string             `json:"method"`
	Aggregation      string             `json:"aggregation"`
	MethodParameters *FedProxParameters `json:"methodParameters"`
}

type FedProxParameters struct {
	ProximalMu *float64 `json:"proximalMu"`
}

type FlReportAfter struct {
	Count int64  `json:"count"`
	Unit  string `json:"unit"`
}

type FlTopologyReport struct {
	NFInstanceID string                 `json:"nfInstanceId"`
	Policy       *FlPolicy              `json:"policy,omitempty"`
	Strategy     *FlStrategy            `json:"strategy,omitempty"`
	ReportAfter  *FlReportAfter         `json:"reportAfter,omitempty"`
	Children     []FlTopologyReportNode `json:"children,omitempty"`
}

type FlTopologyReportNode struct {
	NFInstanceID    string                 `json:"nfInstanceId"`
	Status          string                 `json:"status"`
	StatusTimestamp string                 `json:"statusTimestamp"`
	StatusCause     string                 `json:"statusCause,omitempty"`
	Policy          *FlPolicy              `json:"policy,omitempty"`
	Strategy        *FlStrategy            `json:"strategy,omitempty"`
	ReportAfter     *FlReportAfter         `json:"reportAfter,omitempty"`
	Children        []FlTopologyReportNode `json:"children,omitempty"`
}

type CandidateOperationDescriptor struct {
	TopLevelRetainedResultRequest bool
	NodeRequests                  []string
}

func HasCandidateSubscriptionFields(value *NwdafMLModelTrainSubsc) bool {
	return value != nil && (value.FLTopology != nil || value.RetainedResultRequest != nil)
}

func HasCandidatePatchFields(value *NwdafMLModelTrainSubscPatch) bool {
	return value != nil && (value.flTopologyPresent || value.retainedResultRequestPresent)
}

func RequiresCandidateSubscriptionCorrelation(value *NwdafMLModelTrainSubsc) bool {
	return value != nil && (value.FLTopology != nil || boolValue(value.RetainedResultRequest))
}

func RequiresCandidatePatchCorrelation(value *NwdafMLModelTrainSubscPatch) bool {
	return value != nil && (value.flTopologyPresent || boolValue(value.RetainedResultRequest))
}

func ContainsCandidateOperations(value *NwdafMLModelTrainSubsc) bool {
	if value == nil {
		return false
	}
	if value.RetainedResultRequest != nil {
		return true
	}
	return topologyContainsOperation(value.FLTopology)
}

func topologyContainsOperation(node *FlTopologyNode) bool {
	if node == nil {
		return false
	}
	if node.RetainedResultRequest != nil {
		return true
	}
	for index := range node.Children {
		if topologyContainsOperation(&node.Children[index]) {
			return true
		}
	}
	return false
}

func HasCandidateNotificationFields(value *NwdafMLModelTrainNotif) bool {
	return value != nil && (value.FLTopologyReport != nil || value.RetainedResultStatus != "")
}

func StripCandidateOperations(value *NwdafMLModelTrainSubsc) CandidateOperationDescriptor {
	if value == nil {
		return CandidateOperationDescriptor{}
	}
	descriptor := CandidateOperationDescriptor{
		TopLevelRetainedResultRequest: boolValue(value.RetainedResultRequest),
	}
	value.RetainedResultRequest = nil
	stripNodeOperations(value.FLTopology, &descriptor)
	return descriptor
}

func StripCandidatePatchOperations(value *NwdafMLModelTrainSubscPatch) CandidateOperationDescriptor {
	if value == nil {
		return CandidateOperationDescriptor{}
	}
	descriptor := CandidateOperationDescriptor{
		TopLevelRetainedResultRequest: boolValue(value.RetainedResultRequest),
	}
	value.RetainedResultRequest = nil
	stripNodeOperations(value.FLTopology, &descriptor)
	return descriptor
}

func stripNodeOperations(node *FlTopologyNode, descriptor *CandidateOperationDescriptor) {
	if node == nil || descriptor == nil {
		return
	}
	if boolValue(node.RetainedResultRequest) {
		descriptor.NodeRequests = append(descriptor.NodeRequests, node.NFInstanceID)
	}
	node.RetainedResultRequest = nil
	for index := range node.Children {
		stripNodeOperations(&node.Children[index], descriptor)
	}
}

func boolValue(value *bool) bool {
	return value != nil && *value
}

func SupportedFeaturesInclude(mask string, feature int) bool {
	if feature <= 0 {
		return false
	}
	value, ok := parseSupportedFeatures(mask)
	return ok && value.Bit(feature-1) == 1
}

func SupportedFeaturesAreSubset(offered string, negotiated string) bool {
	offeredValue, offeredOK := parseSupportedFeatures(offered)
	negotiatedValue, negotiatedOK := parseSupportedFeatures(negotiated)
	if !offeredOK || !negotiatedOK {
		return false
	}
	extra := new(big.Int).AndNot(negotiatedValue, offeredValue)
	return extra.Sign() == 0
}

func SupportedFeaturesEqual(first string, second string) bool {
	firstValue, firstOK := parseSupportedFeatures(first)
	secondValue, secondOK := parseSupportedFeatures(second)
	return firstOK && secondOK && firstValue.Cmp(secondValue) == 0
}

func parseSupportedFeatures(mask string) (*big.Int, bool) {
	if mask == "" {
		return new(big.Int), true
	}
	for _, value := range mask {
		if (value < '0' || value > '9') &&
			(value < 'a' || value > 'f') &&
			(value < 'A' || value > 'F') {
			return nil, false
		}
	}
	value := new(big.Int)
	_, ok := value.SetString(mask, 16)
	return value, ok
}
