package sbi

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi"
	"github.com/free5gc/openapi/models"
)

// NadrfDataRetrievalNotification is the callback payload from ADRF (TS 29.575).
type NadrfDataRetrievalNotification struct {
	NotifCorrId    string                   `json:"notifCorrId"`
	TimeStamp      string                   `json:"timeStamp"`
	FetchInstruct  *models.FetchInstruction `json:"fetchInstruct,omitempty"`
	TerminationReq bool                     `json:"terminationReq,omitempty"`
}

// HandleAdrfRetrievalNotify handles POST /collector/retrieval-notify.
// ADRF calls this endpoint to deliver fetch instructions for data retrieval.
func (s *Server) HandleAdrfRetrievalNotify(c *gin.Context) {
	var notif NadrfDataRetrievalNotification
	requestBody, err := c.GetRawData()
	if err != nil {
		logger.SBILog.Errorf("Get Request Body error: %+v", err)
		util.GinProblemJson(c, openapi.ProblemDetailsSystemFailure(err.Error()))
		return
	}

	if deserializeErr := openapi.Deserialize(&notif, requestBody, "application/json"); deserializeErr != nil {
		logger.SBILog.Errorf("Failed to deserialize ADRF retrieval notification: %v", deserializeErr)
		util.GinProblemJson(c, openapi.ProblemDetailsMalformedReqSyntax(deserializeErr.Error()))
		return
	}

	var fetchCorrIds []string
	if notif.FetchInstruct != nil {
		fetchCorrIds = notif.FetchInstruct.FetchCorrIds
	}

	s.Processor().HandleAdrfRetrievalNotify(notif.NotifCorrId, fetchCorrIds, notif.TerminationReq)

	c.Status(http.StatusNoContent)
}
