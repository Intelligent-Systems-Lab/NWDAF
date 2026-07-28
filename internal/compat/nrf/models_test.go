package nrf

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/free5gc/openapi/models"
)

func TestNFProfilePreservesRelease18MLAnalyticsInfo(t *testing.T) {
	t.Parallel()

	body := []byte(`{
		"nfInstanceId":"11111111-1111-4111-8111-111111111111",
		"nfType":"NWDAF",
		"nfStatus":"REGISTERED",
		"nwdafInfo":{
			"nwdafEvents":["UE_COMMUNICATION"],
			"mlAnalyticsList":[{
				"mlAnalyticsIds":["UE_COMMUNICATION"],
				"trackingAreaList":[{
					"plmnId":{"mcc":"466","mnc":"92"},
					"tac":"000001"
				}],
				"mlModelInterInfo":{"vendorList":["001122"]},
				"flCapabilityType":"FL_CLIENT",
				"flTimeInterval":{
					"startTime":"2026-07-28T00:00:00Z",
					"stopTime":"2026-07-28T01:00:00Z"
				},
				"nfTypeList":["UPF"],
				"nfSetIdList":["set1.nwdafset.5gc.mnc092.mcc466"]
			}]
		}
	}`)
	var profile NFProfile
	if err := json.Unmarshal(body, &profile); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if profile.NwdafInfo == nil || len(profile.NwdafInfo.MLAnalyticsList) != 1 {
		t.Fatalf("nwdafInfo = %#v", profile.NwdafInfo)
	}
	info := profile.NwdafInfo.MLAnalyticsList[0]
	if err := ValidateMLAnalyticsInfo(info); err != nil {
		t.Fatalf("ValidateMLAnalyticsInfo() error = %v", err)
	}
	if info.FLCapabilityType != FLCapabilityTypeClient {
		t.Fatalf("flCapabilityType = %q", info.FLCapabilityType)
	}
	encoded, err := json.Marshal(profile)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	for _, field := range []string{
		`"nwdafEvents"`,
		`"mlAnalyticsList"`,
		`"flCapabilityType"`,
		`"mlModelInterInfo"`,
		`"nfTypeList"`,
	} {
		if !strings.Contains(string(encoded), field) {
			t.Fatalf("json.Marshal() dropped %s: %s", field, encoded)
		}
	}
	if strings.Count(string(encoded), `"nwdafInfo"`) != 1 {
		t.Fatalf("json.Marshal() emitted duplicate nwdafInfo: %s", encoded)
	}
}

func TestNFProfileMarshalKeepsGeneratedBaseFields(t *testing.T) {
	t.Parallel()

	profile := NFProfile{
		NrfNfManagementNfProfile: models.NrfNfManagementNfProfile{
			NfInstanceId: "11111111-1111-4111-8111-111111111111",
			NfType:       models.NrfNfManagementNfType_NWDAF,
			NfStatus:     models.NrfNfManagementNfStatus_REGISTERED,
		},
		NwdafInfo: &NwdafInfo{
			NwdafInfo: models.NwdafInfo{
				NwdafEvents: []models.NwdafEvent{models.NwdafEvent_UE_COMMUNICATION},
			},
			MLAnalyticsList: []MLAnalyticsInfo{{
				MLAnalyticsIDs:   []models.NwdafEvent{models.NwdafEvent_UE_COMMUNICATION},
				FLCapabilityType: FLCapabilityTypeServer,
			}},
		},
	}
	encoded, err := json.Marshal(profile)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"nfInstanceId"`) ||
		!strings.Contains(string(encoded), `"nwdafEvents"`) {
		t.Fatalf("generated base fields were dropped: %s", encoded)
	}
}

func TestNFProfileRoundTripsModelOnlyAndCombinedFLCapabilities(t *testing.T) {
	t.Parallel()

	body := []byte(`{
		"nfInstanceId":"11111111-1111-4111-8111-111111111111",
		"nfType":"NWDAF",
		"nfStatus":"REGISTERED",
		"nwdafInfo":{
			"mlAnalyticsList":[
				{
					"mlAnalyticsIds":["UE_COMMUNICATION"],
					"flCapabilityType":"FL_SERVER"
				},
				{
					"mlAnalyticsIds":["UE_COMMUNICATION"],
					"trackingAreaList":[{
						"plmnId":{"mcc":"466","mnc":"92"},
						"tac":"000001"
					}],
					"flCapabilityType":"FL_CLIENT"
				}
			]
		}
	}`)
	var profile NFProfile
	if err := json.Unmarshal(body, &profile); err != nil {
		t.Fatal(err)
	}
	if profile.NwdafInfo == nil || len(profile.NwdafInfo.MLAnalyticsList) != 2 {
		t.Fatalf("nwdafInfo = %#v", profile.NwdafInfo)
	}
	for _, info := range profile.NwdafInfo.MLAnalyticsList {
		if err := ValidateMLAnalyticsInfo(info); err != nil {
			t.Fatalf("ValidateMLAnalyticsInfo() error = %v", err)
		}
	}
	encoded, err := json.Marshal(profile)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), `"nwdafEvents"`) {
		t.Fatalf("model-only profile gained analytics provider fields: %s", encoded)
	}
	if strings.Count(string(encoded), `"flCapabilityType"`) != 2 {
		t.Fatalf("combined FL capabilities were not preserved: %s", encoded)
	}
}

func TestMLAnalyticsInfoValidationAndForwardCompatibleCapability(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC)
	stop := start.Add(time.Hour)
	valid := MLAnalyticsInfo{
		MLAnalyticsIDs: []models.NwdafEvent{models.NwdafEvent_UE_COMMUNICATION},
		MLModelInteroperabilityInfo: &MLModelInteroperabilityInfo{
			VendorList: []string{"001122"},
		},
		FLCapabilityType: "FUTURE_FL_ROLE",
		FLTimeInterval:   &models.TimeWindow{StartTime: &start, StopTime: &stop},
	}
	if err := ValidateMLAnalyticsInfo(valid); err != nil {
		t.Fatalf("ValidateMLAnalyticsInfo() error = %v", err)
	}
	if IsKnownFLCapability(valid.FLCapabilityType) {
		t.Fatal("future capability was incorrectly treated as a supported runtime role")
	}

	for _, value := range []MLAnalyticsInfo{
		{MLAnalyticsIDs: []models.NwdafEvent{}},
		{MLModelInteroperabilityInfo: &MLModelInteroperabilityInfo{VendorList: []string{"vendor"}}},
		{FLTimeInterval: &models.TimeWindow{StartTime: &start}},
	} {
		if err := ValidateMLAnalyticsInfo(value); err == nil {
			t.Fatalf("invalid MLAnalyticsInfo %#v unexpectedly passed", value)
		}
	}
}
