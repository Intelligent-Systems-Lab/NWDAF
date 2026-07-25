package sbi

import (
	"encoding/json"
	"errors"
	"mime"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/backend"
	adrfcompat "github.com/free5gc/nwdaf/internal/compat/adrf"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi/models"
)

const applicationJSONMediaType = "application/json"

// HandleAdrfRetrievalNotify handles POST /collector/retrieval-notify.
// ADRF calls this endpoint to deliver fetch instructions for data retrieval.
func (s *Server) HandleAdrfRetrievalNotify(c *gin.Context) {
	var notif adrfcompat.DataRetrievalNotification
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4*1024*1024)
	requestBody, err := c.GetRawData()
	if err != nil {
		util.GinProblemJson(c, &models.ProblemDetails{
			Status: http.StatusRequestEntityTooLarge,
			Title:  http.StatusText(http.StatusRequestEntityTooLarge),
			Detail: "ADRF retrieval notification exceeds the transport limit",
		})
		return
	}

	mediaType, _, mediaTypeErr := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if mediaTypeErr != nil || mediaType != applicationJSONMediaType {
		util.GinProblemJson(c, &models.ProblemDetails{
			Status: http.StatusUnsupportedMediaType,
			Title:  http.StatusText(http.StatusUnsupportedMediaType),
		})
		return
	}
	if decodeErr := json.Unmarshal(requestBody, &notif); decodeErr != nil ||
		notif.NotifCorrId == "" || notif.TimeStamp == "" ||
		boolCount(notif.FetchInstruct != nil, len(notif.DataNotif) > 0, len(notif.AnaNotifications) > 0) != 1 ||
		notif.FetchInstruct != nil &&
			(notif.FetchInstruct.FetchUri == "" ||
				len(notif.FetchInstruct.FetchCorrIds) == 0 && !notif.TerminationReq) {
		util.GinProblemJson(c, &models.ProblemDetails{
			Status: http.StatusBadRequest, Title: http.StatusText(http.StatusBadRequest),
			Cause: "INVALID_MSG_FORMAT", Detail: "invalid NadrfDataRetrievalNotification",
		})
		return
	}
	response, deliveryErr := s.Processor().HandleAdrfRetrievalNotify(c.Request.Context(), requestBody)
	if deliveryErr == nil && response != nil {
		c.Status(http.StatusNoContent)
		return
	}
	var standardError *backend.StandardError
	if errors.As(deliveryErr, &standardError) {
		util.GinProblemJson(c, standardError.StandardProblemDetails())
		return
	}
	util.GinProblemJson(c, &models.ProblemDetails{
		Status: http.StatusServiceUnavailable, Title: http.StatusText(http.StatusServiceUnavailable),
		Detail: "MTLF backend did not accept the retrieval notification",
	})
}

func boolCount(values ...bool) int {
	count := 0
	for _, value := range values {
		if value {
			count++
		}
	}
	return count
}
