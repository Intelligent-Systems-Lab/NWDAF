package anlf

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi"
	"github.com/free5gc/openapi/models"
)

func (s *Server) HandleModelAccuracyReport(c *gin.Context) {
	var report contract.ModelAccuracyReport
	if err := c.ShouldBindJSON(&report); err != nil {
		util.GinProblemJson(c, openapi.ProblemDetailsMalformedReqSyntax(err.Error()))
		return
	}
	if report.ReportID == "" || !report.ModelIdentity.Valid() || report.Generation <= 0 ||
		report.MonitoringContext.ScopeID == "" || report.AccuracyInformation.SampleCount <= 0 {
		util.GinProblemJson(c, &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "MANDATORY_IE_MISSING",
			Detail: "accuracy report identity, generation, scope, and samples are required",
		})
		return
	}
	if err := s.processor.HandleModelAccuracyReport(&report); err != nil {
		if errors.Is(err, contract.ErrStaleModelGeneration) {
			c.Status(http.StatusConflict)
			return
		}
		c.Status(http.StatusServiceUnavailable)
		return
	}
	c.Status(http.StatusNoContent)
}
