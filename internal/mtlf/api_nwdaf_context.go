package mtlf

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/backend"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
)

func (s *Server) nwdafContextRoutes() []Route {
	return []Route{{
		Name:    "GetContainingNwdafContext",
		Method:  http.MethodGet,
		Pattern: "/internal/v1/nwdaf-context",
		APIFunc: s.GetContainingNwdafContext,
	}}
}

func (s *Server) GetContainingNwdafContext(c *gin.Context) {
	if s.processInstanceID == "" {
		c.AbortWithStatus(http.StatusServiceUnavailable)
		return
	}
	context := nwdaf_context.GetSelf()
	if context == nil {
		c.AbortWithStatus(http.StatusServiceUnavailable)
		return
	}
	projection, err := context.FLCapabilityProjection()
	if err != nil {
		mtlfLog.Warnf("Cannot project containing NWDAF FL capabilities: %v", err)
		c.AbortWithStatus(http.StatusServiceUnavailable)
		return
	}
	capabilities := make([]backend.MLAnalyticsCapability, len(projection))
	for index, entry := range projection {
		capabilities[index] = backend.MLAnalyticsCapability{
			MLAnalyticsIDs:   entry.MLAnalyticsIDs,
			FLCapabilityType: entry.FLCapabilityType,
		}
	}
	c.JSON(http.StatusOK, backend.NwdafContextResponse{
		NFInstanceID:            context.NfId,
		ProcessInstanceID:       s.processInstanceID,
		APIRoot:                 s.publicCallbackBaseURI,
		InternalAPIRoot:         s.internalAPIBaseURI,
		MLAnalyticsCapabilities: capabilities,
	})
}
