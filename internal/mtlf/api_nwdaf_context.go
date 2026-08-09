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
	context := nwdaf_context.GetSelf()
	if context == nil {
		c.Status(http.StatusServiceUnavailable)
		return
	}
	c.JSON(http.StatusOK, backend.NwdafContextResponse{
		NFInstanceID:    context.NfId,
		APIRoot:         s.publicCallbackBaseURI,
		InternalAPIRoot: s.internalAPIBaseURI,
	})
}
