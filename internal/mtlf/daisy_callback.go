package mtlf

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi"
	"github.com/free5gc/openapi/models"
)

// DaisyTrainingCompleteNotification is the callback payload sent by Daisy when async
// training finishes.
type DaisyTrainingCompleteNotification struct {
	TaskID   string `json:"task_id"`
	ModelURL string `json:"model_url,omitempty"`
	Status   string `json:"status"`
	Error    string `json:"error,omitempty"`
}

// RegisterCallbackRoutes registers Daisy callback routes outside the SBI package.
func RegisterCallbackRoutes(router gin.IRouter, service *MtlfService) {
	if router == nil || service == nil {
		return
	}
	router.POST("/training-complete", service.HandleDaisyTrainingComplete)
}

// HandleDaisyTrainingComplete handles POST /mtlf/training-complete.
func (m *MtlfService) HandleDaisyTrainingComplete(c *gin.Context) {
	var notif DaisyTrainingCompleteNotification
	if err := c.ShouldBindJSON(&notif); err != nil {
		mtlfLog.Errorf("Failed to parse Daisy training callback: %v", err)
		util.GinProblemJson(c, openapi.ProblemDetailsMalformedReqSyntax(err.Error()))
		return
	}
	if notif.TaskID == "" {
		util.GinProblemJson(c, &models.ProblemDetails{
			Title:  "Mandatory IEs are missing",
			Status: http.StatusBadRequest,
			Cause:  "MANDATORY_IE_MISSING",
			Detail: "task_id is required",
		})
		return
	}

	mtlfLog.Infof("Handle DaisyTrainingComplete: task=%s status=%s",
		notif.TaskID, notif.Status)

	m.HandleTrainingComplete(notif.TaskID, notif.ModelURL, notif.Status, notif.Error)
	c.Status(http.StatusNoContent)
}
