package anlf

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi/models"
)

type adrfMLModelProcessor interface {
	RetrieveAdrfMLModelRecord(
		context.Context,
		string,
		string,
		[]int64,
	) (*consumer.StandardAdrfResponse, error)
}

func (s *Server) adrfMLModelRoutes() []Route {
	return []Route{{
		Name:    "RetrieveAdrfMLModelRecord",
		Method:  http.MethodGet,
		Pattern: "/internal/v1/adrf-mlmodelmanagement/mlmodel-store-records",
		APIFunc: s.RetrieveAdrfMLModelRecord,
	}}
}

func (s *Server) RetrieveAdrfMLModelRecord(c *gin.Context) {
	storeTransID := strings.TrimSpace(c.Query("store-trans-id"))
	rawModelIDs := c.QueryArray("model-unique-ids")
	if (storeTransID == "") == (len(rawModelIDs) == 0) || len(rawModelIDs) > 1 {
		util.GinProblemJson(
			c,
			malformedRequestProblem(
				"exactly one store-trans-id or one model-unique-ids value is required",
			),
		)
		return
	}
	var modelIDs []int64
	if len(rawModelIDs) == 1 {
		modelID, err := strconv.ParseInt(rawModelIDs[0], 10, 64)
		if err != nil || modelID < 0 {
			util.GinProblemJson(c, malformedRequestProblem("model-unique-ids is invalid"))
			return
		}
		modelIDs = []int64{modelID}
	}
	target := strings.TrimRight(strings.TrimSpace(c.GetHeader("Target-Api-Root")), "/")
	parsed, err := url.Parse(target)
	if err != nil || parsed.Scheme != "http" && parsed.Scheme != "https" ||
		parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" ||
		parsed.Fragment != "" || parsed.Path != "" {
		util.GinProblemJson(c, malformedRequestProblem("Target-Api-Root must be an HTTP(S) origin"))
		return
	}
	processor, ok := s.processor.(adrfMLModelProcessor)
	if !ok {
		util.GinProblemJson(c, adrfStorageUnavailableProblem())
		return
	}
	response, requestErr := processor.RetrieveAdrfMLModelRecord(
		c.Request.Context(),
		target,
		storeTransID,
		modelIDs,
	)
	if requestErr == nil && response != nil {
		if response.Location != "" {
			c.Header("Location", response.Location)
		}
		if len(response.Body) == 0 {
			c.Status(response.StatusCode)
		} else {
			c.Data(response.StatusCode, response.ContentType, response.Body)
		}
		return
	}
	var standardError interface {
		StandardProblemDetails() *models.ProblemDetails
	}
	if errors.As(requestErr, &standardError) {
		util.GinProblemJson(c, standardError.StandardProblemDetails())
		return
	}
	util.GinProblemJson(c, &models.ProblemDetails{
		Status: http.StatusBadGateway,
		Title:  http.StatusText(http.StatusBadGateway),
		Detail: "ADRF ML model retrieval request failed",
	})
}
