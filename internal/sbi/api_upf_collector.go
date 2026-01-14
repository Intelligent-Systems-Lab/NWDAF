package sbi

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/collector"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/openapi/models"
)

// HandleUpfNotify handles UPF event exposure notifications
// POST /collector/upf-notify
// Per TS 29.564: UPF sends Nupf_EventExposure_Notify directly to NWDAF
func (s *Server) HandleUpfNotify(c *gin.Context) {
	var notification collector.UpfNotificationData

	if err := c.BindJSON(&notification); err != nil {
		logger.SBILog.Errorf("Failed to parse UPF notification: %v", err)
		c.JSON(http.StatusBadRequest, models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_JSON",
			Detail: err.Error(),
		})
		return
	}

	logger.SBILog.Infof("Received UPF notification, items: %d", len(notification.NotificationItems))

	// Process the notification
	ctx := collector.GetSelf()
	if err := ctx.HandleUpfNotification(&notification); err != nil {
		logger.SBILog.Errorf("Failed to handle UPF notification: %v", err)
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
