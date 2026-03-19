package sbi

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/logger"
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
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload"})
		return
	}
	if notif.TaskId == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing task_id"})
		return
	}

	logger.SBILog.Infof("Received Daisy training callback: taskId=%s status=%s",
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
