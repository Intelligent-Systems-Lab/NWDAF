package anlf

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"

	anlfprocessor "github.com/free5gc/nwdaf/internal/anlf/processor"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi/models"
)

const (
	maxAdrfStorageBodyBytes = 4 * 1024 * 1024
	httpsURLScheme          = "https"
)

type adrfStorageProcessor interface {
	StoreAdrfDataRecord(context.Context, string, []byte) (*consumer.StandardAdrfResponse, error)
}

func (s *Server) adrfStorageRoutes() []Route {
	return []Route{{
		Name:    "StoreAdrfDataRecord",
		Method:  http.MethodPost,
		Pattern: "/internal/v1/adrf-data-management/data-store-records",
		APIFunc: s.StoreAdrfDataRecord,
	}}
}

func (s *Server) StoreAdrfDataRecord(c *gin.Context) {
	body, problem := readStandardJSONBody(
		c,
		maxAdrfStorageBodyBytes,
		"ADRF storage body exceeds the configured transport limit",
	)
	if problem != nil {
		util.GinProblemJson(c, problem)
		return
	}
	target := strings.TrimSpace(c.GetHeader("Target-Api-Root"))
	parsedTarget, targetErr := url.Parse(target)
	if targetErr != nil || parsedTarget.Scheme != "http" && parsedTarget.Scheme != httpsURLScheme ||
		parsedTarget.Host == "" || parsedTarget.User != nil || parsedTarget.RawQuery != "" ||
		parsedTarget.Fragment != "" || parsedTarget.Path != "" && parsedTarget.Path != "/" {
		util.GinProblemJson(c, malformedRequestProblem("Target-Api-Root must be an HTTP(S) origin"))
		return
	}
	var record consumer.NadrfDataStoreRecord
	if err := json.Unmarshal(body, &record); err != nil {
		util.GinProblemJson(c, malformedRequestProblem(err.Error()))
		return
	}
	if len(record.DataSub) == 0 || record.DataNotif == nil ||
		(len(record.DataNotif.UpfEventNotifs) == 0 && len(record.DataNotif.SmfEventNotifs) == 0) {
		util.GinProblemJson(c, &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Title:  http.StatusText(http.StatusBadRequest),
			Cause:  "MANDATORY_IE_MISSING",
			Detail: "dataSub and a supported dataNotif array are required",
		})
		return
	}
	processor, ok := s.processor.(adrfStorageProcessor)
	if !ok {
		util.GinProblemJson(c, adrfStorageUnavailableProblem())
		return
	}
	response, err := processor.StoreAdrfDataRecord(c.Request.Context(), strings.TrimRight(target, "/"), body)
	if err == nil && response != nil {
		c.Header("Location", response.Location)
		contentType := response.ContentType
		if contentType == "" {
			contentType = standardJSONMediaType
		}
		c.Data(response.StatusCode, contentType, response.Body)
		return
	}
	if errors.Is(err, anlfprocessor.ErrAdrfStorageUnavailable) || response == nil && err == nil {
		util.GinProblemJson(c, adrfStorageUnavailableProblem())
		return
	}
	var standardError interface {
		StandardProblemDetails() *models.ProblemDetails
	}
	if errors.As(err, &standardError) {
		util.GinProblemJson(c, standardError.StandardProblemDetails())
		return
	}
	util.GinProblemJson(c, &models.ProblemDetails{
		Status: http.StatusBadGateway,
		Title:  http.StatusText(http.StatusBadGateway),
		Detail: "ADRF storage request failed",
	})
}

func adrfStorageUnavailableProblem() *models.ProblemDetails {
	return &models.ProblemDetails{
		Status: http.StatusServiceUnavailable,
		Title:  http.StatusText(http.StatusServiceUnavailable),
		Detail: "ADRF storage is temporarily unavailable",
	}
}
