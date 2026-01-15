package sbi

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/sbi/processor"
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
		{
			Name:    "UpfEventExposureNotify",
			Method:  "POST",
			Pattern: "/upf-notify",
			APIFunc: s.HandleUpfNotify,
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

	// Process the notification using processor
	proc := s.Processor()
	if err := proc.HandleSmfNotification(&notification); err != nil {
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

// HandleUpfNotify handles UPF event exposure notifications
// POST /collector/upf-notify
func (s *Server) HandleUpfNotify(c *gin.Context) {
	var notification processor.UpfNotificationData

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

	// Process the notification using processor
	proc := s.Processor()
	if err := proc.HandleUpfNotification(&notification); err != nil {
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
