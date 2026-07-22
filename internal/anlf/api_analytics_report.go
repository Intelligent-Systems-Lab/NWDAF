package anlf

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	"github.com/free5gc/nwdaf/internal/anlf/processor"
)

func (s *Server) HandleAnalyticsReport(c *gin.Context) {
	subscriptionID := c.Param("subscriptionId")
	var report contract.AnalyticsReport
	if err := c.ShouldBindJSON(&report); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Malformed analytics report"})
		return
	}
	if report.ReportID == "" || report.RuntimeRevision <= 0 || report.ReportSequence <= 0 ||
		report.GeneratedAt.IsZero() || len(report.EventNotifications) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid analytics report identity"})
		return
	}
	err := s.processor.HandleAnalyticsReport(subscriptionID, &report)
	switch {
	case err == nil:
		c.Status(http.StatusNoContent)
	case errors.Is(err, processor.ErrSubscriptionNotFound):
		c.JSON(http.StatusNotFound, gin.H{"detail": err.Error()})
	case errors.Is(err, processor.ErrStaleAnalyticsReport):
		c.JSON(http.StatusConflict, gin.H{"detail": err.Error()})
	case errors.Is(err, processor.ErrInvalidAnalyticsReport):
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
	case errors.Is(err, processor.ErrAnalyticsReportInFlight):
		c.JSON(http.StatusServiceUnavailable, gin.H{"detail": err.Error()})
	default:
		c.JSON(http.StatusBadGateway, gin.H{"detail": "External analytics delivery failed"})
	}
}
