package sbi

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/sbi/processor"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi"
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
		{
			Name:    "AdrfRetrievalNotify",
			Method:  "POST",
			Pattern: "/retrieval-notify",
			APIFunc: s.HandleAdrfRetrievalNotify,
		},
	}
}

// HandleCollectorNotify handles SMF event exposure notifications
// POST /collector/notify
func (s *Server) HandleCollectorNotify(c *gin.Context) {
	var notification models.NsmfEventExposureNotification

	requestBody, err := c.GetRawData()
	if err != nil {
		logger.SBILog.Errorf("Get Request Body error: %+v", err)
		util.GinProblemJson(c, openapi.ProblemDetailsSystemFailure(err.Error()))
		return
	}

	if deserializeErr := openapi.Deserialize(&notification, requestBody, "application/json"); deserializeErr != nil {
		logger.SBILog.Errorf("Deserialize notification error: %v", deserializeErr)
		util.GinProblemJson(c, openapi.ProblemDetailsMalformedReqSyntax(deserializeErr.Error()))
		return
	}

	logger.SBILog.Infof("Handle SmfNotification: notifId=%s events=%d",
		notification.NotifId, len(notification.EventNotifs))

	// Process the notification using processor
	proc := s.Processor()
	if handleErr := proc.HandleSmfNotification(&notification); handleErr != nil {
		logger.SBILog.Errorf("Handle SmfNotification failed: %v", handleErr)
		util.GinProblemJson(c, openapi.ProblemDetailsSystemFailure(handleErr.Error()))
		return
	}

	// Return 204 No Content on success (per 3GPP spec)
	c.Status(http.StatusNoContent)
}

// HandleUpfNotify handles UPF event exposure notifications
// POST /collector/upf-notify
func (s *Server) HandleUpfNotify(c *gin.Context) {
	var notification processor.UpfNotificationData

	if err := c.ShouldBindJSON(&notification); err != nil {
		logger.SBILog.Errorf("Failed to parse UPF notification: %v", err)
		util.GinProblemJson(c, openapi.ProblemDetailsMalformedReqSyntax(err.Error()))
		return
	}

	logger.SBILog.Infof("Handle UpfNotification: corr=%s items=%d",
		notification.CorrelationId, len(notification.NotificationItems))

	// Process the notification using processor
	proc := s.Processor()
	if err := proc.HandleUpfNotification(&notification); err != nil {
		logger.SBILog.Errorf("Handle UpfNotification failed: %v", err)
		util.GinProblemJson(c, openapi.ProblemDetailsSystemFailure(err.Error()))
		return
	}

	// Return 204 No Content on success (per 3GPP spec)
	c.Status(http.StatusNoContent)
}
