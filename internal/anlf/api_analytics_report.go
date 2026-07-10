package anlf

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
)

func (s *Server) HandleAnalyticsReport(c *gin.Context) {
	subscriptionID := c.Param("subscriptionId")
	var report AnalyticsReport
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
	case errors.Is(err, ErrSubscriptionNotFound):
		c.JSON(http.StatusNotFound, gin.H{"detail": err.Error()})
	case errors.Is(err, ErrStaleAnalyticsReport):
		c.JSON(http.StatusConflict, gin.H{"detail": err.Error()})
	case errors.Is(err, ErrInvalidAnalyticsReport):
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
	case errors.Is(err, ErrAnalyticsReportInFlight):
		c.JSON(http.StatusServiceUnavailable, gin.H{"detail": err.Error()})
	default:
		c.JSON(http.StatusBadGateway, gin.H{"detail": "External analytics delivery failed"})
	}
}
