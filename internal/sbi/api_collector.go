package sbi

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/collector"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/openapi/models"
)

// getCollectorRoutes returns routes for data collection callbacks
func (s *Server) getCollectorRoutes() []Route {
	return []Route{
		{
			Name:    "SmfEventExposureNotify",
			Method:  "POST",
			Pattern: "/notify",
			APIFunc: s.HandleCollectorNotify,
		},
	}
}

// HandleCollectorNotify handles SMF event exposure notifications
// POST /collector/notify
func (s *Server) HandleCollectorNotify(c *gin.Context) {
	var notification models.NsmfEventExposureNotification

	if err := c.BindJSON(&notification); err != nil {
		logger.SBILog.Errorf("Failed to parse notification: %v", err)
		c.JSON(http.StatusBadRequest, models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_JSON",
			Detail: err.Error(),
		})
		return
	}

	// Process the notification
	ctx := collector.GetSelf()
	if err := ctx.HandleNotification(&notification); err != nil {
		logger.SBILog.Errorf("Failed to handle notification: %v", err)
		c.JSON(http.StatusInternalServerError, models.ProblemDetails{
			Status: http.StatusInternalServerError,
			Cause:  "INTERNAL_ERROR",
			Detail: err.Error(),
		})
		return
	}

	// Return 204 No Content on success (per 3GPP spec)
	c.Status(http.StatusNoContent)
}
