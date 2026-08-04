package backend

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"

	compatnrf "github.com/free5gc/nwdaf/internal/compat/nrf"
	"github.com/free5gc/openapi/models"
)

const (
	serviceMLModelMonitor    = "nnwdaf-mlmodelmonitor"
	serviceMLModelTraining   = "nnwdaf-mlmodeltraining"
	serviceADRFData          = "nadrf-datamanagement"
	serviceADRFModel         = "nadrf-mlmodelmanagement"
	queryMLAnalyticsInfoList = "ml-analytics-info-list"
)

var (
	mccPattern = regexp.MustCompile(`^[0-9]{3}$`)
	mncPattern = regexp.MustCompile(`^[0-9]{2,3}$`)
	tacPattern = regexp.MustCompile(`^(?:[0-9A-Fa-f]{4}|[0-9A-Fa-f]{6})$`)
	sdPattern  = regexp.MustCompile(`^[0-9A-Fa-f]{6}$`)
	knownQuery = map[string]struct{}{
		"target-nf-type":          {},
		"requester-nf-type":       {},
		"target-nf-instance-id":   {},
		"service-names":           {},
		"nwdaf-event-list":        {},
		queryMLAnalyticsInfoList:  {},
		"internal-group-identity": {},
		"ml-model-storage-ind":    {},
		"data-storage-ind":        {},
	}
)

// NFDiscoveryQuery is the standard-shaped discovery request accepted from a
// local analytics backend before the SBI consumer sends it to the NRF.
type NFDiscoveryQuery struct {
	TargetNFType          models.NrfNfManagementNfType
	RequesterNFType       models.NrfNfManagementNfType
	TargetNFInstanceID    string
	ServiceNames          []models.ServiceName
	NwdafEventList        []models.NwdafEvent
	MLAnalyticsInfoList   []compatnrf.MLAnalyticsInfo
	InternalGroupIdentity string
	MLModelStorageInd     *bool
	DataStorageInd        *bool
}

// ParseNFDiscoveryQuery validates the NFDiscovery query subset exposed to the
// local AnLF and MTLF backends.
func ParseNFDiscoveryQuery(values url.Values) (NFDiscoveryQuery, *models.ProblemDetails) {
	for name := range values {
		if _, supported := knownQuery[name]; !supported {
			return NFDiscoveryQuery{}, invalid(
				fmt.Sprintf("query parameter %q is not supported on this internal edge", name),
			)
		}
		if len(values[name]) != 1 {
			return NFDiscoveryQuery{}, invalid(
				fmt.Sprintf("query parameter %q must appear exactly once", name),
			)
		}
	}

	target := models.NrfNfManagementNfType(strings.TrimSpace(values.Get("target-nf-type")))
	if !supportedTarget(target) {
		return NFDiscoveryQuery{}, invalid("target-nf-type is missing or unsupported")
	}
	requester := strings.TrimSpace(values.Get("requester-nf-type"))
	if requester != "" && requester != string(models.NrfNfManagementNfType_NWDAF) {
		return NFDiscoveryQuery{}, invalid(
			"requester-nf-type, when present, must be NWDAF",
		)
	}
	query := NFDiscoveryQuery{
		TargetNFType:    target,
		RequesterNFType: models.NrfNfManagementNfType_NWDAF,
	}

	if value := strings.TrimSpace(values.Get("target-nf-instance-id")); value != "" {
		if _, err := uuid.Parse(value); err != nil {
			return NFDiscoveryQuery{}, invalid(
				"target-nf-instance-id must be a UUID",
			)
		}
		query.TargetNFInstanceID = value
	}

	var err error
	if raw, present := presentValue(values, "service-names"); present {
		query.ServiceNames, err = parseServiceNames(raw)
		if err != nil {
			return NFDiscoveryQuery{}, invalid(err.Error())
		}
	}
	if raw, present := presentValue(values, "nwdaf-event-list"); present {
		query.NwdafEventList, err = parseNwdafEvents(raw)
		if err != nil {
			return NFDiscoveryQuery{}, invalid(err.Error())
		}
	}
	if raw, present := presentValue(values, queryMLAnalyticsInfoList); present {
		if err = json.Unmarshal([]byte(raw), &query.MLAnalyticsInfoList); err != nil {
			return NFDiscoveryQuery{}, invalid(
				"ml-analytics-info-list must be a JSON array",
			)
		}
		if len(query.MLAnalyticsInfoList) == 0 {
			return NFDiscoveryQuery{}, invalid(
				"ml-analytics-info-list must contain at least one entry",
			)
		}
		for index, entry := range query.MLAnalyticsInfoList {
			if entry.FLTimeInterval != nil {
				return NFDiscoveryQuery{}, invalid(
					"ml-analytics-info-list flTimeInterval matching is not supported",
				)
			}
			if err = validateMLAnalyticsInfo(entry); err != nil {
				return NFDiscoveryQuery{}, invalid(
					fmt.Sprintf("ml-analytics-info-list[%d]: %v", index, err),
				)
			}
		}
	}
	query.InternalGroupIdentity = strings.TrimSpace(values.Get("internal-group-identity"))
	if query.MLModelStorageInd, err = parseOptionalBool(values, "ml-model-storage-ind"); err != nil {
		return NFDiscoveryQuery{}, invalid(err.Error())
	}
	if query.DataStorageInd, err = parseOptionalBool(values, "data-storage-ind"); err != nil {
		return NFDiscoveryQuery{}, invalid(err.Error())
	}
	if err = validateCombination(query); err != nil {
		return NFDiscoveryQuery{}, invalid(err.Error())
	}
	return query, nil
}

func presentValue(values url.Values, name string) (string, bool) {
	raw, present := values[name]
	if !present {
		return "", false
	}
	return strings.TrimSpace(raw[0]), true
}

func parseServiceNames(raw string) ([]models.ServiceName, error) {
	if raw == "" {
		return nil, fmt.Errorf("service-names must not be empty when present")
	}
	seen := map[models.ServiceName]struct{}{}
	result := []models.ServiceName{}
	for _, item := range strings.Split(raw, ",") {
		name := models.ServiceName(strings.TrimSpace(item))
		if name == "" {
			return nil, fmt.Errorf("service-names contains an empty value")
		}
		if _, exists := seen[name]; exists {
			continue
		}
		seen[name] = struct{}{}
		result = append(result, name)
	}
	sort.Slice(result, func(left, right int) bool {
		return result[left] < result[right]
	})
	return result, nil
}

func parseNwdafEvents(raw string) ([]models.NwdafEvent, error) {
	if raw == "" {
		return nil, fmt.Errorf("nwdaf-event-list must not be empty when present")
	}
	seen := map[models.NwdafEvent]struct{}{}
	result := []models.NwdafEvent{}
	for _, item := range strings.Split(raw, ",") {
		event := models.NwdafEvent(strings.TrimSpace(item))
		if event != models.NwdafEvent_UE_COMMUNICATION {
			return nil, fmt.Errorf("nwdaf-event-list contains unsupported value %q", event)
		}
		if _, exists := seen[event]; exists {
			continue
		}
		seen[event] = struct{}{}
		result = append(result, event)
	}
	return result, nil
}

func parseOptionalBool(values url.Values, name string) (*bool, error) {
	raw, present := presentValue(values, name)
	if !present {
		return nil, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return nil, fmt.Errorf("%s must be true or false", name)
	}
	return &value, nil
}

func validateMLAnalyticsInfo(info compatnrf.MLAnalyticsInfo) error {
	if err := compatnrf.ValidateMLAnalyticsInfo(info); err != nil {
		return err
	}
	if len(info.MLAnalyticsIDs) == 0 {
		return fmt.Errorf("mlAnalyticsIds must contain at least one entry")
	}
	for _, event := range info.MLAnalyticsIDs {
		if event != models.NwdafEvent_UE_COMMUNICATION {
			return fmt.Errorf("mlAnalyticsIds contains unsupported value %q", event)
		}
	}
	if info.FLCapabilityType != "" && !compatnrf.IsKnownFLCapability(info.FLCapabilityType) {
		return fmt.Errorf("flCapabilityType %q is invalid", info.FLCapabilityType)
	}
	for index, snssai := range info.SNSSAIList {
		if snssai.Sst < 0 || snssai.Sst > 255 {
			return fmt.Errorf("snssaiList[%d].sst is invalid", index)
		}
		if snssai.Sd != "" && !sdPattern.MatchString(snssai.Sd) {
			return fmt.Errorf("snssaiList[%d].sd is invalid", index)
		}
	}
	for index, tai := range info.TrackingAreaList {
		if tai.PlmnId == nil ||
			!mccPattern.MatchString(tai.PlmnId.Mcc) ||
			!mncPattern.MatchString(tai.PlmnId.Mnc) ||
			!tacPattern.MatchString(tai.Tac) {
			return fmt.Errorf("trackingAreaList[%d] is invalid", index)
		}
	}
	return nil
}

func validateCombination(query NFDiscoveryQuery) error {
	serviceAllowed := func(allowed ...string) bool {
		for _, service := range query.ServiceNames {
			matched := false
			for _, candidate := range allowed {
				matched = matched || string(service) == candidate
			}
			if !matched {
				return false
			}
		}
		return true
	}

	switch query.TargetNFType {
	case models.NrfNfManagementNfType_NWDAF:
		if !serviceAllowed(
			string(models.ServiceName_NNWDAF_EVENTSSUBSCRIPTION),
			string(models.ServiceName_NNWDAF_MLMODELPROVISION),
			serviceMLModelMonitor,
			serviceMLModelTraining,
		) {
			return fmt.Errorf("service-names contains a service unsupported for target NWDAF")
		}
		if query.InternalGroupIdentity != "" ||
			query.MLModelStorageInd != nil || query.DataStorageInd != nil {
			return fmt.Errorf("the selected filters are not valid for target NWDAF")
		}
	case models.NrfNfManagementNfType_SMF:
		if len(query.ServiceNames) == 0 ||
			!serviceAllowed(string(models.ServiceName_NSMF_EVENT_EXPOSURE)) {
			return fmt.Errorf("target SMF requires service-names=nsmf-event-exposure")
		}
		if len(query.NwdafEventList) > 0 || query.MLAnalyticsInfoList != nil ||
			query.InternalGroupIdentity != "" ||
			query.MLModelStorageInd != nil || query.DataStorageInd != nil {
			return fmt.Errorf("the selected filters are not valid for target SMF")
		}
	case models.NrfNfManagementNfType_UDM:
		if len(query.ServiceNames) != 1 ||
			!serviceAllowed(
				string(models.ServiceName_NUDM_SDM),
				string(models.ServiceName_NUDM_UECM),
			) {
			return fmt.Errorf("target UDM requires exactly one of nudm-sdm or nudm-uecm")
		}
		sdmDiscovery := string(query.ServiceNames[0]) == string(models.ServiceName_NUDM_SDM)
		if sdmDiscovery && query.InternalGroupIdentity == "" {
			return fmt.Errorf("nudm-sdm discovery requires internal-group-identity")
		}
		if !sdmDiscovery && query.TargetNFInstanceID == "" {
			return fmt.Errorf("nudm-uecm discovery requires target-nf-instance-id")
		}
		if !sdmDiscovery && query.InternalGroupIdentity != "" {
			return fmt.Errorf("internal-group-identity is not valid for nudm-uecm discovery")
		}
		if len(query.NwdafEventList) > 0 || query.MLAnalyticsInfoList != nil ||
			query.MLModelStorageInd != nil || query.DataStorageInd != nil {
			return fmt.Errorf("the selected filters are not valid for target UDM")
		}
	case models.NrfNfManagementNfType_ADRF:
		if len(query.ServiceNames) == 0 ||
			!serviceAllowed(serviceADRFData, serviceADRFModel) {
			return fmt.Errorf("target ADRF requires a supported ADRF service name")
		}
		if len(query.NwdafEventList) > 0 || query.MLAnalyticsInfoList != nil ||
			query.InternalGroupIdentity != "" {
			return fmt.Errorf("the selected filters are not valid for target ADRF")
		}
	default:
		return fmt.Errorf("target-nf-type is unsupported")
	}
	return nil
}

func supportedTarget(value models.NrfNfManagementNfType) bool {
	switch value {
	case models.NrfNfManagementNfType_NWDAF,
		models.NrfNfManagementNfType_UDM,
		models.NrfNfManagementNfType_SMF,
		models.NrfNfManagementNfType_ADRF:
		return true
	default:
		return false
	}
}

func invalid(detail string) *models.ProblemDetails {
	return &models.ProblemDetails{
		Status: http.StatusBadRequest,
		Title:  http.StatusText(http.StatusBadRequest),
		Cause:  "MANDATORY_QUERY_PARAM_INCORRECT",
		Detail: detail,
	}
}
