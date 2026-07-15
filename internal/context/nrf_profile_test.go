package context

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/free5gc/openapi/models"
)

func TestConfigureNFManagementBuildsTruthfulPhaseZeroProfile(t *testing.T) {
	t.Parallel()

	ctx := &NWDAFContext{
		NfId:                "11111111-1111-4111-8111-111111111111",
		nfServiceInstanceId: "22222222-2222-4222-8222-222222222222",
	}
	err := ctx.ConfigureNFManagement(
		"http://127.0.0.10:8000",
		"NWDAF",
		"https://192.0.2.10:8080",
		"https",
		"192.0.2.10",
		8080,
	)
	if err != nil {
		t.Fatalf("ConfigureNFManagement() error = %v", err)
	}

	profile := ctx.NFProfile()
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
	if state.Registered || !state.OAuth2Required || state.ResourceURI == "" {
		t.Fatalf("OAuth-required state = %+v", state)
	}

	ctx.MarkDeregistered()
	state = ctx.RegistrationState()
	if state.Registered || state.ResourceURI != "" {
		t.Fatalf("deregistered state = %+v", state)
	}
}

func TestConfigureNFManagementRejectsNonAdvertisableIPv4(t *testing.T) {
	t.Parallel()

	for _, registerIPv4 := range []string{"", "0.0.0.0", "nwdaf.example.com", "2001:db8::10"} {
		registerIPv4 := registerIPv4
		t.Run(registerIPv4, func(t *testing.T) {
			t.Parallel()

			ctx := &NWDAFContext{NfId: "11111111-1111-4111-8111-111111111111"}
			err := ctx.ConfigureNFManagement(
				"http://127.0.0.10:8000",
				"NWDAF",
				"http://192.0.2.10:8080",
				"http",
				registerIPv4,
				8080,
			)
			if err == nil || !strings.Contains(err.Error(), "valid non-wildcard IPv4 address") {
				t.Fatalf("ConfigureNFManagement() error = %v, want invalid advertised IPv4", err)
			}
		})
	}
}
