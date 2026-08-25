package mtlf

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	adrfcompat "github.com/free5gc/nwdaf/internal/compat/adrf"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi/models"
)

type adrfMLModelProcessor interface {
	StoreAdrfMLModelRecord(context.Context, string, []byte) (*consumer.StandardAdrfResponse, error)
	RetrieveAdrfMLModelRecord(
		context.Context,
		string,
		string,
		[]int64,
	) (*consumer.StandardAdrfResponse, error)
}

func (s *Server) adrfMLModelRoutes() []Route {
	return []Route{
		{
			Name:    "StoreAdrfMLModelRecord",
			Method:  http.MethodPost,
			Pattern: "/internal/v1/adrf-mlmodelmanagement/mlmodel-store-records",
			APIFunc: s.StoreAdrfMLModelRecord,
		},
		{
			Name:    "RetrieveAdrfMLModelRecord",
			Method:  http.MethodGet,
			Pattern: "/internal/v1/adrf-mlmodelmanagement/mlmodel-store-records",
			APIFunc: s.RetrieveAdrfMLModelRecord,
		},
	}
}

func (s *Server) StoreAdrfMLModelRecord(c *gin.Context) {
	body, problem := readStandardJSONBody(
		c,
		maxAdrfControlBodyBytes,
		"ADRF ML model store body exceeds limit",
	)
	if problem != nil {
		util.GinProblemJson(c, problem)
		return
	}
	var record adrfcompat.MLModelStoreRecord
	if err := json.Unmarshal(body, &record); err != nil || !validMLModelStoreRequest(record) {
		util.GinProblemJson(c, malformedRequestProblem("invalid NadrfMLModelStoreRecord"))
		return
	}
	target, ok := adrfMLModelTarget(c)
	if !ok {
		return
	}
	processor, ok := s.processor.(adrfMLModelProcessor)
	if !ok {
		util.GinProblemJson(c, adrfRetrievalUnavailableProblem())
		return
	}
	response, err := processor.StoreAdrfMLModelRecord(c.Request.Context(), target, body)
	writeAdrfMLModelResponse(c, response, err)
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
	target, ok := adrfMLModelTarget(c)
	if !ok {
		return
	}
	processor, ok := s.processor.(adrfMLModelProcessor)
	if !ok {
		util.GinProblemJson(c, adrfRetrievalUnavailableProblem())
		return
	}
	response, err := processor.RetrieveAdrfMLModelRecord(
		c.Request.Context(),
		target,
		storeTransID,
		modelIDs,
	)
	writeAdrfMLModelResponse(c, response, err)
}

func adrfMLModelTarget(c *gin.Context) (string, bool) {
	target := strings.TrimRight(strings.TrimSpace(c.GetHeader("Target-Api-Root")), "/")
	parsed, err := url.Parse(target)
	if err != nil || parsed.Scheme != "http" && parsed.Scheme != targetHTTPSScheme ||
		parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" ||
		parsed.Fragment != "" || parsed.Path != "" {
		util.GinProblemJson(c, malformedRequestProblem("Target-Api-Root must be an HTTP(S) origin"))
		return "", false
	}
	return target, true
}

func validMLModelStoreRequest(record adrfcompat.MLModelStoreRecord) bool {
	if (record.NFInstanceID == "") == (record.NFSetID == "") ||
		len(record.MLModelInfo) != 1 || record.ModelStoreResult != nil {
		return false
	}
	info := record.MLModelInfo[0]
	return info.ModelUniqueID != nil && *info.ModelUniqueID >= 0 &&
		info.MLStorageSize != nil && *info.MLStorageSize >= 0 &&
		(info.MLFileAddr.MLModelURL == "") != (info.MLFileAddr.MLFileFQDN == "")
}

func writeAdrfMLModelResponse(
	c *gin.Context,
	response *consumer.StandardAdrfResponse,
	err error,
) {
	if err == nil && response != nil {
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
	if errors.As(err, &standardError) {
		util.GinProblemJson(c, standardError.StandardProblemDetails())
		return
	}
	util.GinProblemJson(c, &models.ProblemDetails{
		Status: http.StatusBadGateway,
		Title:  http.StatusText(http.StatusBadGateway),
		Detail: "ADRF ML model request failed",
	})
}
