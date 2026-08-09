package mtlf

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/backend"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
)

func TestGetContainingNwdafContext(t *testing.T) {
	gin.SetMode(gin.TestMode)
	nwdaf_context.InitWithNFInstanceID("11111111-1111-4111-8111-111111111111")
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
		response.InternalAPIRoot != "http://nwdaf.example:8091" {
		t.Fatalf("response = %+v", response)
	}
}
