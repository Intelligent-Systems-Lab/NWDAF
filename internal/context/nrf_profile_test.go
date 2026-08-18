package context

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"

	compatnrf "github.com/free5gc/nwdaf/internal/compat/nrf"
	"github.com/free5gc/openapi/models"
)

func TestConfigureNFManagementBuildsTruthfulPhaseZeroProfile(t *testing.T) {
	t.Parallel()

	ctx := &NWDAFContext{
		NfId:                "11111111-1111-4111-8111-111111111111",
		nfServiceInstanceId: "22222222-2222-4222-8222-222222222222",
	}
	err := ctx.ConfigureNFManagement(NFManagementConfig{
		NrfURI:       "http://127.0.0.10:8000",
		NrfCertPEM:   " cert/nrf.pem ",
		NwdafName:    "NWDAF",
		SBIURI:       "https://192.0.2.10:8080",
		SBIScheme:    "https",
		RegisterIPv4: "192.0.2.10",
		SBIPort:      8080,
		ServiceNames: []models.ServiceName{models.ServiceName_NNWDAF_EVENTSSUBSCRIPTION},
		NwdafInfo: &compatnrf.NwdafInfo{NwdafInfo: models.NwdafInfo{
			NwdafEvents: []models.NwdafEvent{models.NwdafEvent_UE_COMMUNICATION},
		}},
	})
	if err != nil {
		t.Fatalf("ConfigureNFManagement() error = %v", err)
	}
	if got := ctx.NrfCertPem(); got != "cert/nrf.pem" {
		t.Fatalf("NrfCertPem() = %q, want cert/nrf.pem", got)
	}

	profile := mustProfileSnapshot(t, ctx)
	if profile.NfInstanceId != ctx.NfId {
		t.Fatalf("NfInstanceId = %q, want %q", profile.NfInstanceId, ctx.NfId)
	}
	if profile.NfInstanceName != "NWDAF" {
		t.Fatalf("NfInstanceName = %q, want NWDAF", profile.NfInstanceName)
	}
	if profile.NfType != models.NrfNfManagementNfType_NWDAF {
		t.Fatalf("NfType = %q, want NWDAF", profile.NfType)
	}
	if profile.NfStatus != models.NrfNfManagementNfStatus_REGISTERED {
		t.Fatalf("NfStatus = %q, want REGISTERED", profile.NfStatus)
	}
	if len(profile.Ipv4Addresses) != 1 || profile.Ipv4Addresses[0] != "192.0.2.10" {
		t.Fatalf("Ipv4Addresses = %v, want [192.0.2.10]", profile.Ipv4Addresses)
	}
	if len(profile.PlmnList) != 0 {
		t.Fatalf("PlmnList = %v, want omitted", profile.PlmnList)
	}
	if len(profile.NfServiceList) != 0 {
		t.Fatalf("NfServiceList = %v, want omitted", profile.NfServiceList)
	}
	if profile.NwdafInfo == nil || len(profile.NwdafInfo.NwdafEvents) != 1 ||
		profile.NwdafInfo.NwdafEvents[0] != models.NwdafEvent_UE_COMMUNICATION {
		t.Fatalf("NwdafInfo = %+v, want only UE_COMMUNICATION", profile.NwdafInfo)
	}
	if len(profile.NfServices) != 1 {
		t.Fatalf("NfServices length = %d, want 1", len(profile.NfServices))
	}

	service := profile.NfServices[0]
	if service.ServiceInstanceId != "22222222-2222-4222-8222-222222222222" {
		t.Fatalf("ServiceInstanceId = %q", service.ServiceInstanceId)
	}
	if service.ServiceName != models.ServiceName_NNWDAF_EVENTSSUBSCRIPTION {
		t.Fatalf("ServiceName = %q, want nnwdaf-eventssubscription", service.ServiceName)
	}
	if len(service.Versions) != 1 || service.Versions[0].ApiVersionInUri != "v1" ||
		service.Versions[0].ApiFullVersion != "1.0.0" {
		t.Fatalf("Versions = %+v, want v1/1.0.0", service.Versions)
	}
	if service.Scheme != models.UriScheme_HTTPS {
		t.Fatalf("Scheme = %q, want https", service.Scheme)
	}
	if service.NfServiceStatus != models.NfServiceStatus_REGISTERED {
		t.Fatalf("NfServiceStatus = %q, want REGISTERED", service.NfServiceStatus)
	}
	if service.ApiPrefix != "https://192.0.2.10:8080" {
		t.Fatalf("ApiPrefix = %q", service.ApiPrefix)
	}
	if len(service.IpEndPoints) != 1 {
		t.Fatalf("IpEndPoints length = %d, want 1", len(service.IpEndPoints))
	}
	endpoint := service.IpEndPoints[0]
	if endpoint.Ipv4Address != "192.0.2.10" || endpoint.Port != 8080 ||
		endpoint.Transport != models.NrfNfManagementTransportProtocol_TCP {
		t.Fatalf("IpEndPoint = %+v", endpoint)
	}

	encoded, err := json.Marshal(profile)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	body := string(encoded)
	for _, absent := range []string{`"nfServiceList"`, `"plmnList"`} {
		if strings.Contains(body, absent) {
			t.Fatalf("profile JSON contains %s: %s", absent, body)
		}
	}
}

func TestConfigureNFManagementOmitsEventsCapabilityWhenAnlfBackendDisabled(t *testing.T) {
	t.Parallel()

	ctx := &NWDAFContext{NfId: "11111111-1111-4111-8111-111111111111"}
	err := ctx.ConfigureNFManagement(NFManagementConfig{
		NrfURI:       "http://127.0.0.10:8000",
		NwdafName:    "NWDAF",
		SBIURI:       "http://192.0.2.10:8080",
		SBIScheme:    "http",
		RegisterIPv4: "192.0.2.10",
		SBIPort:      8080,
	})
	if err != nil {
		t.Fatalf("ConfigureNFManagement() error = %v", err)
	}
	profile := mustProfileSnapshot(t, ctx)
	if profile.NwdafInfo != nil || len(profile.NfServices) != 0 {
		t.Fatalf("disabled AnLF profile advertises dependent capability: %+v", profile)
	}
}

func TestNFRegistrationStateTransitions(t *testing.T) {
	t.Parallel()

	ctx := &NWDAFContext{}
	ctx.RecordHeartBeatTimer(10)
	ctx.MarkRegistered("http://nrf/nnrf-nfm/v1/nf-instances/id")
	state := ctx.RegistrationState()
	if !state.Registered || state.OAuth2Required || state.ResourceURI == "" || state.HeartBeatTimer != 10 {
		t.Fatalf("registered state = %+v", state)
	}

	ctx.RecordOAuth2Required("http://nrf/nnrf-nfm/v1/nf-instances/id")
	state = ctx.RegistrationState()
	if !state.Registered || !state.OAuth2Required || state.ResourceURI == "" {
		t.Fatalf("OAuth-required state = %+v", state)
	}

	ctx.MarkDeregistered()
	state = ctx.RegistrationState()
	if state.Registered || state.ResourceURI != "" {
		t.Fatalf("deregistered state = %+v", state)
	}
}

func TestNFRegistrationStateConcurrentSnapshots(t *testing.T) {
	t.Parallel()

	ctx := &NWDAFContext{}
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for iteration := 0; iteration < 100; iteration++ {
				if worker%2 == 0 {
					ctx.MarkRegistered("http://nrf/nnrf-nfm/v1/nf-instances/id")
				} else {
					ctx.RecordOAuth2Required("http://nrf/nnrf-nfm/v1/nf-instances/id")
				}
				_ = ctx.RegistrationState()
			}
		}(worker)
	}
	wg.Wait()

	state := ctx.RegistrationState()
	if !state.Registered || state.ResourceURI == "" {
		t.Fatalf("registration state = %+v", state)
	}
}

func TestConfigureNFManagementRejectsNonAdvertisableIPv4(t *testing.T) {
	t.Parallel()

	for _, registerIPv4 := range []string{"", "0.0.0.0", "nwdaf.example.com", "2001:db8::10"} {
		registerIPv4 := registerIPv4
		t.Run(registerIPv4, func(t *testing.T) {
			t.Parallel()

			ctx := &NWDAFContext{NfId: "11111111-1111-4111-8111-111111111111"}
			err := ctx.ConfigureNFManagement(NFManagementConfig{
				NrfURI:       "http://127.0.0.10:8000",
				NwdafName:    "NWDAF",
				SBIURI:       "http://192.0.2.10:8080",
				SBIScheme:    "http",
				RegisterIPv4: registerIPv4,
				SBIPort:      8080,
				ServiceNames: []models.ServiceName{models.ServiceName_NNWDAF_EVENTSSUBSCRIPTION},
			})
			if err == nil || !strings.Contains(err.Error(), "valid non-wildcard IPv4 address") {
				t.Fatalf("ConfigureNFManagement() error = %v, want invalid advertised IPv4", err)
			}
		})
	}
}

func TestConfigureNFManagementAdvertisesDistinctMLModelServices(t *testing.T) {
	t.Parallel()

	ctx := &NWDAFContext{
		NfId:                              "11111111-1111-4111-8111-111111111111",
		nfServiceInstanceId:               "22222222-2222-4222-8222-222222222222",
		mlModelProvisionServiceInstanceID: "33333333-3333-4333-8333-333333333333",
		mlModelMonitorServiceInstanceID:   "44444444-4444-4444-8444-444444444444",
	}
	if err := ctx.ConfigureNFManagement(NFManagementConfig{
		NrfURI:       "http://127.0.0.10:8000",
		NwdafName:    "NWDAF",
		SBIURI:       "http://192.0.2.10:8080",
		SBIScheme:    "http",
		RegisterIPv4: "192.0.2.10",
		SBIPort:      8080,
		ServiceNames: []models.ServiceName{
			models.ServiceName_NNWDAF_EVENTSSUBSCRIPTION,
			models.ServiceName_NNWDAF_MLMODELPROVISION,
			nwdafMLModelMonitorServiceName,
		},
		NwdafInfo: &compatnrf.NwdafInfo{
			NwdafInfo: models.NwdafInfo{
				NwdafEvents: []models.NwdafEvent{models.NwdafEvent_UE_COMMUNICATION},
			},
			MLAnalyticsList: []compatnrf.MLAnalyticsInfo{{
				MLAnalyticsIDs: []models.NwdafEvent{models.NwdafEvent_UE_COMMUNICATION},
			}},
		},
	}); err != nil {
		t.Fatalf("ConfigureNFManagement() error = %v", err)
	}
	profile := mustProfileSnapshot(t, ctx)
	if len(profile.NfServices) != 3 {
		t.Fatalf("NfServices length = %d, want 3", len(profile.NfServices))
	}
	want := map[models.ServiceName]string{
		models.ServiceName_NNWDAF_EVENTSSUBSCRIPTION: "22222222-2222-4222-8222-222222222222",
		models.ServiceName_NNWDAF_MLMODELPROVISION:   "33333333-3333-4333-8333-333333333333",
		nwdafMLModelMonitorServiceName:               "44444444-4444-4444-8444-444444444444",
	}
	for _, service := range profile.NfServices {
		if service.ServiceInstanceId != want[service.ServiceName] {
			t.Fatalf("service %s instance ID = %s", service.ServiceName, service.ServiceInstanceId)
		}
		delete(want, service.ServiceName)
	}
	if len(want) != 0 {
		t.Fatalf("missing services: %v", want)
	}
	if profile.NwdafInfo == nil || len(profile.NwdafInfo.MLAnalyticsList) != 1 ||
		len(profile.NwdafInfo.MLAnalyticsList[0].MLAnalyticsIDs) != 1 ||
		profile.NwdafInfo.MLAnalyticsList[0].MLAnalyticsIDs[0] !=
			models.NwdafEvent_UE_COMMUNICATION {
		t.Fatalf("ML analytics profile = %+v", profile.NwdafInfo)
	}
}

func TestConfigureNFManagementAdvertisesMLAnalyticsWithoutEventsCapability(t *testing.T) {
	t.Parallel()

	ctx := &NWDAFContext{NfId: "11111111-1111-4111-8111-111111111111"}
	if err := ctx.ConfigureNFManagement(NFManagementConfig{
		NrfURI:       "http://127.0.0.10:8000",
		NwdafName:    "NWDAF",
		SBIURI:       "http://192.0.2.10:8080",
		SBIScheme:    "http",
		RegisterIPv4: "192.0.2.10",
		SBIPort:      8080,
		ServiceNames: []models.ServiceName{models.ServiceName_NNWDAF_MLMODELPROVISION},
		NwdafInfo: &compatnrf.NwdafInfo{
			MLAnalyticsList: []compatnrf.MLAnalyticsInfo{{
				MLAnalyticsIDs: []models.NwdafEvent{models.NwdafEvent_UE_COMMUNICATION},
			}},
		},
	}); err != nil {
		t.Fatalf("ConfigureNFManagement() error = %v", err)
	}
	profile := mustProfileSnapshot(t, ctx)
	if profile.NwdafInfo == nil || len(profile.NwdafInfo.NwdafEvents) != 0 ||
		len(profile.NwdafInfo.MLAnalyticsList) != 1 {
		t.Fatalf("ML-only NwdafInfo = %+v", profile.NwdafInfo)
	}
}

func TestFLCapabilityProjectionCanonicalizesAdvertisedCapabilities(t *testing.T) {
	t.Parallel()

	ctx := &NWDAFContext{NfId: "11111111-1111-4111-8111-111111111111"}
	if err := ctx.ConfigureNFManagement(NFManagementConfig{
		NrfURI:       "http://127.0.0.10:8000",
		NwdafName:    "NWDAF",
		SBIURI:       "http://192.0.2.10:8080",
		SBIScheme:    "http",
		RegisterIPv4: "192.0.2.10",
		SBIPort:      8080,
		NwdafInfo: &compatnrf.NwdafInfo{
			MLAnalyticsList: []compatnrf.MLAnalyticsInfo{
				{
					MLAnalyticsIDs: []models.NwdafEvent{
						models.NwdafEvent_UE_MOBILITY,
						models.NwdafEvent_UE_COMMUNICATION,
						models.NwdafEvent_UE_COMMUNICATION,
					},
					FLCapabilityType: compatnrf.FLCapabilityTypeServerAndClient,
				},
				{
					MLAnalyticsIDs:   []models.NwdafEvent{models.NwdafEvent_UE_COMMUNICATION},
					FLCapabilityType: compatnrf.FLCapabilityTypeClient,
				},
				{
					MLAnalyticsIDs:   []models.NwdafEvent{models.NwdafEvent_UE_COMMUNICATION},
					FLCapabilityType: compatnrf.FLCapabilityTypeClient,
					NFSetIDList:      []string{"set-that-is-not-projected"},
				},
				{
					MLAnalyticsIDs: []models.NwdafEvent{models.NwdafEvent_UE_COMMUNICATION},
				},
			},
		},
	}); err != nil {
		t.Fatalf("ConfigureNFManagement() error = %v", err)
	}

	projection, err := ctx.FLCapabilityProjection()
	if err != nil {
		t.Fatalf("FLCapabilityProjection() error = %v", err)
	}
	if len(projection) != 2 {
		t.Fatalf("projection = %+v, want 2 canonical entries", projection)
	}
	if projection[0].FLCapabilityType != compatnrf.FLCapabilityTypeClient ||
		len(projection[0].MLAnalyticsIDs) != 1 ||
		projection[0].MLAnalyticsIDs[0] != models.NwdafEvent_UE_COMMUNICATION {
		t.Fatalf("projection[0] = %+v", projection[0])
	}
	if projection[1].FLCapabilityType != compatnrf.FLCapabilityTypeServerAndClient ||
		len(projection[1].MLAnalyticsIDs) != 2 ||
		projection[1].MLAnalyticsIDs[0] != models.NwdafEvent_UE_COMMUNICATION ||
		projection[1].MLAnalyticsIDs[1] != models.NwdafEvent_UE_MOBILITY {
		t.Fatalf("projection[1] = %+v", projection[1])
	}
}

func TestFLCapabilityProjectionRejectsInvalidProfile(t *testing.T) {
	t.Parallel()

	tests := map[string]*NWDAFContext{
		"unconfigured profile": {
			NfId: "11111111-1111-4111-8111-111111111111",
		},
		"unsupported capability": configuredContextWithMLAnalytics(t, []compatnrf.MLAnalyticsInfo{{
			MLAnalyticsIDs:   []models.NwdafEvent{models.NwdafEvent_UE_COMMUNICATION},
			FLCapabilityType: compatnrf.FLCapabilityType("UNKNOWN"),
		}}),
		"missing analytics IDs": configuredContextWithMLAnalytics(t, []compatnrf.MLAnalyticsInfo{{
			FLCapabilityType: compatnrf.FLCapabilityTypeServer,
		}}),
	}

	for name, ctx := range tests {
		ctx := ctx
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if projection, err := ctx.FLCapabilityProjection(); err == nil {
				t.Fatalf("FLCapabilityProjection() = %+v, want error", projection)
			}
		})
	}
}

func configuredContextWithMLAnalytics(
	t *testing.T,
	entries []compatnrf.MLAnalyticsInfo,
) *NWDAFContext {
	t.Helper()
	ctx := &NWDAFContext{NfId: "11111111-1111-4111-8111-111111111111"}
	if err := ctx.ConfigureNFManagement(NFManagementConfig{
		NrfURI:       "http://127.0.0.10:8000",
		NwdafName:    "NWDAF",
		SBIURI:       "http://192.0.2.10:8080",
		SBIScheme:    "http",
		RegisterIPv4: "192.0.2.10",
		SBIPort:      8080,
		NwdafInfo: &compatnrf.NwdafInfo{
			MLAnalyticsList: entries,
		},
	}); err != nil {
		t.Fatalf("ConfigureNFManagement() error = %v", err)
	}
	return ctx
}

func mustProfileSnapshot(t *testing.T, ctx *NWDAFContext) compatnrf.NFProfile {
	t.Helper()
	profile, err := ctx.NFProfileSnapshot()
	if err != nil {
		t.Fatalf("NFProfileSnapshot() error = %v", err)
	}
	return profile
}
