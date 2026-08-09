package anlf

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestAnLFRouteOwnershipExcludesMTLFOriginatedOperations(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	server := &Server{router: router}
	routes := server.eventsSubscriptionNotificationRoutes()
	routes = append(routes, server.nfDiscoveryRoutes()...)
	routes = append(routes, server.smfEventExposureRoutes()...)
	routes = append(routes, server.adrfStorageRoutes()...)
	routes = append(routes, server.trainingDataDescriptorRoutes()...)
	routes = append(routes, server.anlfMLModelRoutes()...)
	applyRoutes(router.Group(""), routes)

	request := httptest.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		"/internal/v1/ml-model-monitor/subscriptions",
		strings.NewReader(`{}`),
	)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("AnLF edge accepted MTLF-originated operation: status=%d", recorder.Code)
	}
}
