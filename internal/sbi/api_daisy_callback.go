package sbi

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi"
	"github.com/free5gc/openapi/models"
)

// DaisyTrainingCompleteNotif is the callback payload sent by Daisy when async
// training finishes.
type DaisyTrainingCompleteNotif struct {
	TaskId   string `json:"task_id"`
	ModelUrl string `json:"model_url,omitempty"`
	// Status is "success" or "failure".
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// HandleDaisyTrainingComplete handles POST /mtlf/training-complete.
// Daisy calls this endpoint after completing an async training task.
func (s *Server) HandleDaisyTrainingComplete(c *gin.Context) {
	var notif DaisyTrainingCompleteNotif
	if err := c.ShouldBindJSON(&notif); err != nil {
		logger.SBILog.Errorf("Failed to parse Daisy training callback: %v", err)
		util.GinProblemJson(c, openapi.ProblemDetailsMalformedReqSyntax(err.Error()))
		return
	}
	if notif.TaskId == "" {
		util.GinProblemJson(c, &models.ProblemDetails{
			Title:  "Mandatory IEs are missing",
			Status: http.StatusBadRequest,
			Cause:  "MANDATORY_IE_MISSING",
			Detail: "task_id is required",
		})
		return
	}

	logger.SBILog.Infof("Handle DaisyTrainingComplete: task=%s status=%s",
		notif.TaskId, notif.Status)

	s.Processor().HandleDaisyCallback(notif.TaskId, notif.ModelUrl, notif.Status, notif.Error)
	c.Status(http.StatusNoContent)
}

// getDaisyCallbackRoutes returns routes for the Daisy async callback.
func (s *Server) getDaisyCallbackRoutes() []Route {
	return []Route{
		{
			Name:    "DaisyTrainingComplete",
			Method:  "POST",
			Pattern: "/training-complete",
			APIFunc: s.HandleDaisyTrainingComplete,
		},
	}
}
