package mtlf

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/backend"
	compatnrf "github.com/free5gc/nwdaf/internal/compat/nrf"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/openapi/models"
)

func TestGetContainingNwdafContext(t *testing.T) {
	gin.SetMode(gin.TestMode)
	nwdaf_context.InitWithNFInstanceID("11111111-1111-4111-8111-111111111111")
	configureContainingNwdafProfile(t, nil)
	server := &Server{
		publicCallbackBaseURI: "http://nwdaf.example:8000",
		internalAPIBaseURI:    "http://nwdaf.example:8091",
	}
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	context.Request = httptest.NewRequestWithContext(
		t.Context(), http.MethodGet, "/internal/v1/nwdaf-context", nil,
	)

	server.GetContainingNwdafContext(context)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	var response backend.NwdafContextResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.NFInstanceID != "11111111-1111-4111-8111-111111111111" ||
		response.APIRoot != "http://nwdaf.example:8000" ||
		response.InternalAPIRoot != "http://nwdaf.example:8091" ||
		len(response.MLAnalyticsCapabilities) != 0 {
		t.Fatalf("response = %+v", response)
	}
}

func TestGetContainingNwdafContextProjectsFLCapabilities(t *testing.T) {
	gin.SetMode(gin.TestMode)
	nwdaf_context.InitWithNFInstanceID("11111111-1111-4111-8111-111111111111")
	configureContainingNwdafProfile(t, []compatnrf.MLAnalyticsInfo{
		{
			MLAnalyticsIDs:   []models.NwdafEvent{models.NwdafEvent_UE_COMMUNICATION},
			FLCapabilityType: compatnrf.FLCapabilityTypeServer,
		},
		{
			MLAnalyticsIDs:   []models.NwdafEvent{models.NwdafEvent_UE_COMMUNICATION},
			FLCapabilityType: compatnrf.FLCapabilityTypeClient,
		},
		{
			MLAnalyticsIDs:   []models.NwdafEvent{models.NwdafEvent_UE_COMMUNICATION},
			FLCapabilityType: compatnrf.FLCapabilityTypeServerAndClient,
		},
	})
	server := &Server{
		publicCallbackBaseURI: "http://nwdaf.example:8000",
		internalAPIBaseURI:    "http://nwdaf.example:8091",
	}
	recorder := httptest.NewRecorder()
	requestContext, _ := gin.CreateTestContext(recorder)
	requestContext.Request = httptest.NewRequestWithContext(
		t.Context(), http.MethodGet, "/internal/v1/nwdaf-context", nil,
	)

	server.GetContainingNwdafContext(requestContext)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	var response backend.NwdafContextResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(response.MLAnalyticsCapabilities) != 3 {
		t.Fatalf("mlAnalyticsCapabilities = %+v", response.MLAnalyticsCapabilities)
	}
	wantCapabilities := []compatnrf.FLCapabilityType{
		compatnrf.FLCapabilityTypeClient,
		compatnrf.FLCapabilityTypeServer,
		compatnrf.FLCapabilityTypeServerAndClient,
	}
	for index, want := range wantCapabilities {
		got := response.MLAnalyticsCapabilities[index]
		if got.FLCapabilityType != want || len(got.MLAnalyticsIDs) != 1 ||
			got.MLAnalyticsIDs[0] != models.NwdafEvent_UE_COMMUNICATION {
			t.Fatalf("mlAnalyticsCapabilities[%d] = %+v, want capability %q", index, got, want)
		}
	}
}

func TestGetContainingNwdafContextRejectsUnconfiguredProfile(t *testing.T) {
	gin.SetMode(gin.TestMode)
	nwdaf_context.InitWithNFInstanceID("11111111-1111-4111-8111-111111111111")
	server := &Server{
		publicCallbackBaseURI: "http://nwdaf.example:8000",
		internalAPIBaseURI:    "http://nwdaf.example:8091",
	}
	recorder := httptest.NewRecorder()
	requestContext, _ := gin.CreateTestContext(recorder)
	requestContext.Request = httptest.NewRequestWithContext(
		t.Context(), http.MethodGet, "/internal/v1/nwdaf-context", nil,
	)

	server.GetContainingNwdafContext(requestContext)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
}

func configureContainingNwdafProfile(t *testing.T, entries []compatnrf.MLAnalyticsInfo) {
	t.Helper()
	ctx := nwdaf_context.GetSelf()
	var info *compatnrf.NwdafInfo
	if entries != nil {
		info = &compatnrf.NwdafInfo{MLAnalyticsList: entries}
	}
	if err := ctx.ConfigureNFManagement(nwdaf_context.NFManagementConfig{
		NrfURI:       "http://127.0.0.10:8000",
		NwdafName:    "NWDAF",
		SBIURI:       "http://192.0.2.10:8080",
		SBIScheme:    "http",
		RegisterIPv4: "192.0.2.10",
		SBIPort:      8080,
		NwdafInfo:    info,
	}); err != nil {
		t.Fatalf("ConfigureNFManagement() error = %v", err)
	}
}
