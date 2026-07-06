package anlf

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi"
	"github.com/free5gc/openapi/models"
)

// HandleMlModelProvisionNotify handles POST /mlmodel-notify.
// Per TS 29.520 §5.4.5.2: callback body is []NwdafMlModelProvNotif.
func (s *Server) HandleMlModelProvisionNotify(c *gin.Context) {
	var notifications []models.NwdafMlModelProvNotif
	requestBody, err := c.GetRawData()
	if err != nil {
		anlfLog.Errorf("Get Request Body error: %+v", err)
		util.GinProblemJson(c, openapi.ProblemDetailsSystemFailure(err.Error()))
		return
	}

	if deserializeErr := openapi.Deserialize(&notifications, requestBody, "application/json"); deserializeErr != nil {
		anlfLog.Errorf("Failed to deserialize ML model provision notification: %v", deserializeErr)
		util.GinProblemJson(c, openapi.ProblemDetailsMalformedReqSyntax(deserializeErr.Error()))
		return
	}

	if len(notifications) == 0 {
		anlfLog.Warn("Empty ML model provision notification received")
		c.Status(http.StatusNoContent)
		return
	}

	s.processor.HandleMlModelProvisionNotify(notifications)
	c.Status(http.StatusNoContent)
}
