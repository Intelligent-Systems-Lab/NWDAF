package sbi

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi"
)

// NadrfDataRetrievalNotification is the callback payload from ADRF (TS 29.575).
type NadrfDataRetrievalNotification struct {
	NotifCorrId    string            `json:"notifCorrId"`
	TimeStamp      string            `json:"timeStamp"`
	FetchInstruct  *FetchInstruction `json:"fetchInstruct,omitempty"`
	TerminationReq bool              `json:"terminationReq,omitempty"`
}

// FetchInstruction carries fetch-correlation-ids from ADRF (TS 29.576).
type FetchInstruction struct {
	FetchUri     string   `json:"fetchUri"`
	FetchCorrIds []string `json:"fetchCorrIds"`
}

// HandleAdrfRetrievalNotify handles POST /collector/retrieval-notify.
// ADRF calls this endpoint to deliver fetch instructions for data retrieval.
func (s *Server) HandleAdrfRetrievalNotify(c *gin.Context) {
	var notif NadrfDataRetrievalNotification
	if err := c.ShouldBindJSON(&notif); err != nil {
		logger.SBILog.Errorf("Failed to parse ADRF retrieval notification: %v", err)
		util.GinProblemJson(c, openapi.ProblemDetailsMalformedReqSyntax(err.Error()))
		return
	}

	var fetchCorrIds []string
	if notif.FetchInstruct != nil {
		fetchCorrIds = notif.FetchInstruct.FetchCorrIds
	}

	s.Processor().HandleAdrfRetrievalNotify(notif.NotifCorrId, fetchCorrIds, notif.TerminationReq)

	c.Status(http.StatusNoContent)
}
